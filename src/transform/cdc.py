from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
import logging

class CDCHandler:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger("CDCHandler")
        self.business_keys = ["date", "base", "target"]

    def process_cdc(self, df_incoming: DataFrame, target_table_name: str, start_date: str, end_date: str) -> DataFrame:
        """
        Detecta INSERTS, UPDATES y DELETES lógicos acotados a un rango de fechas.
        Usa Hash de Fila (SHA-256) para la comparación.
        """
        # 1. Generar el Hash de fila para los datos frescos de la API
        df_incoming_hashed = df_incoming.withColumn(
            "row_hash", 
            F.sha2(F.concat_ws("||", F.coalesce(F.col("rate"), F.lit(""))), 256)
        )

        table_exists = self.spark.catalog.tableExists(target_table_name)
        
        # SI NO EXISTE LA TABLA: Todo el lote inicial se clasifica como INSERT
        if not table_exists:
            self.logger.warning(f">>> [CDC] La tabla {target_table_name} no existe. Inicializando histórico.")
            return df_incoming_hashed \
                .withColumn("operation_type", F.lit("INSERT")) \
                .withColumn("ingestion_timestamp", F.current_timestamp()) \
                .withColumn("updated_at", F.current_timestamp())

        # SI SÍ EXISTE LA TABLA: Leemos solo el segmento del rango de fechas solicitado
        df_target_historical = self.spark.read.table(target_table_name)
        
        df_target_range = df_target_historical.filter(
            F.col("date").between(start_date, end_date)
        ).withColumn(
            "target_row_hash", 
            F.sha2(F.concat_ws("||", F.coalesce(F.col("rate"), F.lit(""))), 256)
        )

        # Renombrar columnas históricas para el JOIN
        df_target_prep = df_target_range.select(
            *[F.col(c).alias(f"target_{c}") for c in df_target_range.columns]
        )

        # Condiciones del JOIN basadas en las Llaves de Negocio
        join_conditions = [
            df_incoming_hashed[key] == df_target_prep[f"target_{key}"] 
            for key in self.business_keys
        ]

        # FULL OUTER JOIN acotado al rango de tiempo
        cdc_joined = df_incoming_hashed.join(df_target_prep, join_conditions, "full")

        # 2. Matriz de Detección de Cambios (Requisitos del entregable)
        df_cdc_evaluated = cdc_joined.withColumn(
            "operation_type",
            F.when(
                df_target_prep[f"target_{self.business_keys[0]}"].isNull(), 
                "INSERT" # Está en la API pero no en Iceberg
            ).when(
                df_incoming_hashed[self.business_keys[0]].isNull(), 
                "DELETE" # Estaba en Iceberg pero la API ya no lo mandó para esa fecha (Borrado Lógico)
            ).when(
                F.col("row_hash") != F.col("target_row_hash"), 
                "UPDATE" # Las llaves coinciden pero el rate cambió
            ).otherwise("NO_CHANGE")
        )

        # 3. Construcción del Dataset Final con Campos de Auditoría Mandatorios
        df_cdc_final = df_cdc_evaluated.select(
            F.coalesce(df_incoming_hashed["date"], df_target_prep["target_date"]).alias("date"),
            
            # 🚀 AQUÍ ESTÁ LA CORRECCIÓN: Agregamos las 3 columnas omitidas
            F.coalesce(df_incoming_hashed["base_amount"], df_target_prep["target_base_amount"]).alias("base_amount"),
            
            F.coalesce(df_incoming_hashed["base"], df_target_prep["target_base"]).alias("base"),
            F.coalesce(df_incoming_hashed["target"], df_target_prep["target_target"]).alias("target"),
            F.coalesce(df_incoming_hashed["rate"], df_target_prep["target_rate"]).alias("rate"),
            
            # Mapeamos la fecha de ingesta original e histórica
            F.coalesce(df_incoming_hashed["ingested_at"], df_target_prep["target_ingested_at"]).alias("ingested_at"),
            
            # Mapeamos el row_hash original e histórico
            F.coalesce(df_incoming_hashed["row_hash"], df_target_prep["target_row_hash"]).alias("row_hash"),
            
            F.col("operation_type"),
            F.current_timestamp().alias("ingestion_timestamp"),
            F.current_timestamp().alias("updated_at")
        ).filter(F.col("operation_type") != "NO_CHANGE")

        return df_cdc_final