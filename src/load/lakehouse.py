import logging
from pyspark.sql import DataFrame
from pyspark.sql import functions as F


class LakehouseOrchestrator:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger("LakehouseOrchestrator")
        # Asegúrate de usar el prefijo de tu catálogo Iceberg si es necesario (ej: local.db.tipos_cambio)
        self.tables = {
            "base": "local.db.tipos_cambio",
            "enriched": "local.db.tipos_cambio_enriquecidos",
            "monthly": "local.db.metricas_mensuales",
            "anomalies": "local.db.anomalias",
            "quality": "local.db.reporte_calidad"
        }

    def load_lakehouse(self, df: DataFrame, table_name: str, mode="append") -> tuple[DataFrame, bool]:
        """
        Valida la existencia de la tabla en el catálogo, aplica la carga incremental
        de forma segura y retorna una lectura fresca directa de Apache Iceberg junto con
        una bandera booleana que indica si se detectó una operación DELETE.
        """
        flag_delete = False

        if table_name == self.tables["base"] and "operation_type" in df.columns:
            flag_delete = len(df.filter(F.col("operation_type") == "DELETE").take(1)) > 0
            
            if flag_delete:
                self.logger.warning(f">>>> [CDC ALERT] Se detectaron registros de tipo DELETE en la tabla maestra: {table_name}")

        table_exists = self.spark.catalog.tableExists(table_name)
        
        if not table_exists:
            # Inicialización de la tabla con formato nativo Iceberg
            df.writeTo(table_name).using("iceberg").create()
            self.logger.warning(f">>>> [INITIAL LOAD] Tabla creada desde cero en Iceberg: {table_name}")
        else:
            if mode == "merge_cdc":
                # Creamos la vista temporal para que el motor SQL de Iceberg la pueda leer
                view_name = f"stg_cdc_{table_name.split('.')[-1]}"
                df.createOrReplaceTempView(view_name)
                
                # Ejecución del MERGE nativo mapeando el 100% de las columnas 🚀
                self.spark.sql(f"""
                    MERGE INTO {table_name} target
                    USING {view_name} source
                    ON target.date = source.date 
                       AND target.base = source.base 
                       AND target.target = source.target
                    
                    WHEN MATCHED AND source.operation_type = 'UPDATE' THEN
                      UPDATE SET 
                        target.base_amount = source.base_amount,
                        target.rate = source.rate,
                        target.row_hash = source.row_hash,
                        target.operation_type = source.operation_type,
                        target.updated_at = source.updated_at
                        
                    WHEN MATCHED AND source.operation_type = 'DELETE' THEN
                      UPDATE SET 
                        target.operation_type = 'DELETE',
                        target.updated_at = source.updated_at
                        
                    WHEN NOT MATCHED THEN
                      INSERT (
                        date, base_amount, base, target, rate, 
                        ingested_at, row_hash, operation_type, 
                        ingestion_timestamp, updated_at
                      )
                      VALUES (
                        source.date, source.base_amount, source.base, source.target, source.rate, 
                        source.ingested_at, source.row_hash, source.operation_type, 
                        source.ingestion_timestamp, source.updated_at
                      )
                """)
                self.logger.warning(f">>>> [INCREMENTAL] MERGE SQL ejecutado con éxito en: {table_name}")
                
            elif mode == "overwrite_partitions":
                # Operación atómica nativa de Iceberg para tablas particionadas
                df.writeTo(table_name).overwritePartitions()
                self.logger.warning(f">>>> [PARTITION OVERWRITE] Partición actualizada en: {table_name}")
                
            else:
                # Carga incremental estándar append-only
                df.writeTo(table_name).append()
                self.logger.warning(f">>>> [APPEND] Registros añadidos a: {table_name}")
                
        # Forzar la actualización de los metadatos en el catálogo de Iceberg
        self.spark.catalog.refreshTable(table_name)
        
        # Rompemos el linaje perezoso (Lazy Evaluation) regresando la lectura física asentada
        df_output = self.spark.read.table(table_name)
        
        # Retornamos el DataFrame original leído de Iceberg y el estado de la bandera 🚀
        return df_output, flag_delete