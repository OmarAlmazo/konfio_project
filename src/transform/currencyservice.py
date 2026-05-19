import pyspark.sql.functions as F
import utils.constants as c
import logging 

class CurrencyService:
    def __init__(self):
        self.logger = logging.getLogger("CurrencyService")

    def get_monthly_summary(self, df):
        """
        Genera un resumen estadístico agrupado por moneda y mes.
        Ideal para reportes de BI o tablas de agregación.
        """
        if not df or df.isEmpty():
            return df

        # Creamos columnas temporales de año y mes para agrupar
        summary_df = (df
            # Envolvemos "date" en F.col() para que las funciones de Spark extraigan bien el dato
            .withColumn("year", F.year(F.col("date"))) 
            .withColumn("month", F.month(F.col("date"))) 
            
            # Agrupamos usando los nombres de columna fijos
            .groupBy("base", "target", "year", "month")
            
            # Calculamos las métricas directamente sobre la columna "rate"
            .agg(
                F.round(F.avg(F.col("rate")), 4).alias("avg_rate"),
                F.min(F.col("rate")).alias("min_rate"),
                F.max(F.col("rate")).alias("max_rate"),
                F.round(
                    F.coalesce(F.stddev(F.col("rate")), F.lit(0.0)), 
                    4
                ).alias("monthly_volatility"),
                F.count(F.col("rate")).alias("observations_count")
            ) 
            # Ordenamos cronológicamente por año, mes y moneda destino
            .orderBy("year", "month", "target")
        )
        final_df = summary_df.withColumn("ingestion_timestamp", F.current_timestamp())

        # OPTIMIZACIÓN 2: Log de éxito correcto
        self.logger.info(">>> [SUMMARY] Tabla resumen generada exitosamente.")
        return final_df