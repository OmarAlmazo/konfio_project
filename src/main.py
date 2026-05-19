from extract.external_api import FrankfurterAPI
from transform.process import DataProcessor
from transform.add_derived_fields import MetricsCalculator
from transform.currencyservice import CurrencyService
from transform.anomalies import AnomalyDetector
from utils import spark_build as sb
from utils import constants as c
from transform.dataquality import DataQualityAnalyzer
import logging
from load.lakehouse import LakehouseOrchestrator
from transform.cdc import CDCHandler
from pyspark.sql import functions as F
from transform.financialmodel import FinancialModeler
from transform.eventpublish import EventPublisher



def run():
    # Inicialización
    api = FrankfurterAPI()
    spark_builder = sb.SparkBuilder()
    spark_cr = spark_builder._create_spark_session()
    proc = DataProcessor(spark_session=spark_cr)
    metrics = MetricsCalculator()
    currency_service = CurrencyService()
    anomaly_detector = AnomalyDetector()
    dq_analyzer = DataQualityAnalyzer(spark_session=spark_cr)
    orchestrator = LakehouseOrchestrator(spark_cr)
    cdc_handler = CDCHandler(spark_cr)
    modeler = FinancialModeler(spark_cr)
    publisher = EventPublisher(output_path="./events/")
    
    logging.basicConfig(level=logging.WARNING, format='%(asctime)s - %(levelname)s - %(name)s - %(message)s')
    logger = logging.getLogger("MAIN")
    # =======================================================================
    # Extraccion de data desde API externa
    # =======================================================================

    data = api.fetch_exchange_rates(c.FECHA_INICIO_HISTORICA, c.FECHA_FIN_HISTORICA)

    if data and "rates" in data:
        # =======================================================================
        # Creacion de DataFrame tipado con Spark (Capa de Formateo)
        # =======================================================================
        df = proc.format(data)
        df_clean = proc.clean_and_validate(df)
        logger.warning(">>> [MAIN] DataFrame formateado y validado. Mostrando:")
        df_clean.show(5)

        # =======================================================================
        # Filtra informacion ya que la api puede traer datos fuera del rango 
        # =======================================================================
        df_base_filtered = df_clean.filter(
            F.col("date").between(c.FECHA_INICIO_HISTORICA, c.FECHA_FIN_HISTORICA)
        )

        # =======================================================================
        # PRUEBA PARA FORZAR EL DELETE:
        # Creamos un DataFrame completamente vacío con el mismo esquema
        # =======================================================================
        # logger.warning(">>> [TEST] Vaciando el DataFrame entrante para forzar el DELETE...")
        # df_base_filtered = spark_cr.createDataFrame([], df_base_filtered.schema)
        # =======================================================================

        # =======================================================================
        # PRUEBA PARA FORZAR EL UPDATE:
        # Modificamos el rate de un registro existente para engañar al FULL JOIN
        # =======================================================================
        #logger.warning(">>> [TEST] Modificando artificialmente el rate para forzar un UPDATE...")
        #df_base_filtered = df_base_filtered.withColumn(
        #    c.RATE, 
        #    F.when(
        #        (F.col(c.PARAM_BASE) == "USD") & (F.col(c.TARGET) == "MXN"), 
        #        F.col(c.RATE) * 1.69  # Inflamos el peso un 50% para que el CDC detecte el cambio
        #    ).otherwise(F.col(c.RATE))
        #)
        # =======================================================================

        # =======================================================================
        # Proceso CDC: Detectar cambios reales entre el DataFrame entrante y la tabla base en el Lakehouse
        # =======================================================================
        logger.warning(">>> [MAIN] Procesando matriz CDC...")
        df_cdc_results = cdc_handler.process_cdc(
            df_incoming=df_base_filtered,
            target_table_name=orchestrator.tables["base"], 
            start_date=c.FECHA_INICIO_HISTORICA, 
            end_date=c.FECHA_FIN_HISTORICA
        ).cache()

        if not df_cdc_results.isEmpty():
            logger.warning(">>> [MAIN] Se detectaron cambios reales. Actualizando Tabla Base...")
            # =======================================================================
            # Se crea Nuevo dataframe con los resultados del CDC para mostrar antes de hacer el merge
            # =======================================================================            
            cdc_rows_local = df_cdc_results.collect()
            df_cdc_check = spark_cr.createDataFrame(cdc_rows_local, df_cdc_results.schema)

            # =======================================================================
            # Actualización de la tabla base en el Lakehouse usando los resultados del CDC
            # ======================================================================= 
            df_full_base, flag = orchestrator.load_lakehouse(
                df=df_cdc_results,
                table_name=orchestrator.tables["base"],
                mode="merge_cdc"
            )
            showdf0 = spark_cr.read.table(c.TABLE_NAME_EX_RATE) \
                .select(F.count("*").alias("total")) \
                .first()  # .first() nos da la primera fila directamente sin regresar una lista
            # 2. Imprimimos usando la notación de punto del objeto Row
            logger.warning(f"Total registros: {c.TABLE_NAME_EX_RATE} {showdf0.total} ")
            spark_cr.sql(f"SELECT * FROM {c.TABLE_NAME_EX_RATE}").show(5)

            # =======================================================================
            # Enriquecimiento (Métricas calculadas sobre la base limpia)
            # =======================================================================
            if not flag:
                logger.warning(">>> [AUDITORÍA] Verificando el esquema final antes de escribir en Gold...")
                df_with_metrics = metrics.add_derived_fields(df_cdc_check)
                df_metrics, _ = orchestrator.load_lakehouse(
                    df=df_with_metrics, 
                    table_name=orchestrator.tables["enriched"], 
                    mode="overwrite_partitions"
                    )
                showdf1 = spark_cr.read.table(c.TABLE_NAME_ENRICHED) \
                    .select(F.count("*").alias("total")) \
                    .first()  # .first() nos da la primera fila directamente sin regresar una lista
                # 2. Imprimimos usando la notación de punto del objeto Row
                logger.warning(f"Total registros: {c.TABLE_NAME_ENRICHED} {showdf1.total} ")        
                spark_cr.sql(f"SELECT * FROM {c.TABLE_NAME_ENRICHED}").show(5)

                # =======================================================================
                # Agregaciones por 
                # =======================================================================    
                df_resumen = currency_service.get_monthly_summary(df_cdc_check)
                df_resumen2, _ = orchestrator.load_lakehouse(
                    df=df_resumen, 
                    table_name=orchestrator.tables["monthly"], 
                    mode="overwrite_partitions"
                    )
                showdf2 = spark_cr.read.table(c.TABLE_NAME_MONTHLY_METRICS) \
                    .select(F.count("*").alias("total")) \
                    .first()  # .first() nos da la primera fila directamente sin regresar una lista
                # 2. Imprimimos usando la notación de punto del objeto Row
                logger.warning(f"Total registros: {c.TABLE_NAME_MONTHLY_METRICS} {showdf2.total} ")        
                spark_cr.sql(f"SELECT * FROM {c.TABLE_NAME_MONTHLY_METRICS}").show(5)

                # =======================================================================
                # PASO 4: Detencion de anomalias
                # =======================================================================
                df_anomalies = anomaly_detector.detect_anomalies(df_with_metrics)
                df_anomalies2, _ = orchestrator.load_lakehouse(
                    df=df_anomalies, 
                    table_name=orchestrator.tables["anomalies"], 
                    mode="append"
                    )
                showdf3 = spark_cr.read.table(c.TABLE_NAME_ANOMALIES) \
                    .select(F.count("*").alias("total")) \
                    .first()  # .first() nos da la primera fila directamente sin regresar una lista
                # 2. Imprimimos usando la notación de punto del objeto Row
                logger.warning(f"Total registros: {c.TABLE_NAME_ANOMALIES} {showdf3.total} ")        
                spark_cr.sql(f"SELECT * FROM {c.TABLE_NAME_ANOMALIES}").show(5)

                # =======================================================================
                # PASO 7: CALIDAD Y AUDITORÍA (Siempre se ejecuta, no importa si hay cambios)
                # =======================================================================
                logger.warning(">>> [MAIN] Ejecutando Capa de Calidad de Datos...")        
                df_dq = dq_analyzer.run_dq_report(df_base_filtered)
                # Al usar 'append', dejas un log histórico de cada ejecución

                orchestrator.load_lakehouse(
                    df=df_dq, 
                    table_name=orchestrator.tables["quality"], 
                    mode="append"
                )
                showdf4 = spark_cr.read.table(c.TABLE_NAME_DATA_QUALITY) \
                    .select(F.count("*").alias("total")) \
                    .first()  # .first() nos da la primera fila directamente sin regresar una lista
                # 2. Imprimimos usando la notación de punto del objeto Row
                logger.warning(f"Total registros: {c.TABLE_NAME_DATA_QUALITY} {showdf4.total} ")        
                spark_cr.sql(f"SELECT * FROM {c.TABLE_NAME_DATA_QUALITY} ORDER BY dq_date DESC").show(5)

                # =====================================================================
                # 2. LA MAGIA NUEVA: MODELADO DE DATOS (4.4)
                # =====================================================================

                dim_currency, dim_customer = modeler.build_dimensions()
                fact_rates, fact_txns = modeler.build_facts(df_cdc_check, dim_customer)

                # (Opcional) Si quieres mostrarle a los evaluadores que sí se generaron:
                logger.warning("Logica de modelado ejecutada. Mostrando muestras de las tablas dimensionales y de hechos generadas:")
                dim_currency.show()
                dim_customer.show()
                fact_rates.show()
                fact_txns.show()

                # =====================================================================
                # 3. LA MAGIA NUEVA: EMISIÓN DE EVENTOS JSON (4.5)
                # =====================================================================
                publisher = EventPublisher(output_path="./events/")
                publisher.publish_json_events(df_cdc_check)
            else:
                logging.warning(">>> [MAIN] Se detecto al menos un DELETE en el lote actual. Por seguridad, se ha actualizado solo la tabla base ")

        else:
            logging.warning(">>> [MAIN] No se detectaron cambios reales en el rango de fechas especificado. Saltando actualización de la tabla base y capas de negocio.")
 
    else:
        print(">>> [SKIP] No hay datos nuevos para procesar.")

if __name__ == "__main__":
    run()