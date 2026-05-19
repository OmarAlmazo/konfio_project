from typing import Optional
from pyspark.sql import DataFrame, SparkSession
import pyspark.sql.functions as F
import logging

class DataQualityAnalyzer:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session
        self.logger = logging.getLogger("DataQualityAnalyzer")

    def run_dq_report(self, df: Optional[DataFrame]) -> Optional[DataFrame]:
        """
        Genera un reporte estructurado de calidad de datos:
        - Identifica fechas sin cobertura (Gaps).
        - Clasifica días faltantes (Fin de semana vs. Dato perdido).
        - Calcula estadísticas generales.
        """
        if not df or df.isEmpty():
            self.logger.error(">>> [DQ] El DataFrame de entrada está vacío. No se puede generar el reporte.")
            return None

        self.logger.warning(">>> [DQ] Iniciando análisis de calidad de datos...")

        # 1. Obtener el rango de fechas real del dataset (Operación de control)
        date_bounds = df.select(F.min("date").alias("min_date"), F.max("date").alias("max_date")).collect()[0]
        min_date, max_date = date_bounds["min_date"], date_bounds["max_date"]

        if not min_date or not max_date:
            self.logger.warning(">>> [DQ] No se encontraron fechas válidas para el análisis.")
            return None

        # 2. Generar Serie de Tiempo Teórica (Todos los días consecutivos que DEBERÍAN existir)
        # Usamos funciones nativas de Spark para crear la secuencia de días
        theoretical_calendar = self.spark.range(1).select(
            F.explode(
                F.sequence(F.to_date(F.lit(min_date)), F.to_date(F.lit(max_date)), F.expr("INTERVAL 1 DAY"))
            ).alias("expected_date")
        )

        # 3. Obtener los días únicos que SÍ tenemos en nuestro dataset
        actual_dates = df.select("date").distinct()

        # 4. Identificar los Gaps (Días faltantes) mediante un Left Anti Join u Outter Join
        # Clasificamos el día de la semana: Spark considera 1 = Domingo, 7 = Sábado
        dq_report = (theoretical_calendar
            .join(actual_dates, theoretical_calendar.expected_date == actual_dates.date, "left")
            .withColumn("is_missing", F.when(F.col("date").isNull(), True).otherwise(False))
            .withColumn("day_idx", F.dayofweek(F.col("expected_date")))
            .withColumn("day_type", 
                F.when(F.col("day_idx").isin(1, 7), "Weekend")
                .otherwise("Weekday")
            )
            # Solo nos interesan los registros que faltan o estructurar la métrica
            .withColumn("status_detail",
                F.when((F.col("is_missing") == True) & (F.col("day_type") == "Weekend"), "Missing - Normal Weekend No-Publish")
                .when((F.col("is_missing") == True) & (F.col("day_type") == "Weekday"), "CRITICAL - Lost Data / Gap")
                .otherwise("OK - Data Present")
            )
            .select(
                F.col("expected_date").alias("dq_date"),
                F.col("day_type"),
                F.col("status_detail"),
                F.col("is_missing")
            )
            .orderBy("dq_date")
        )

        # 5. Calcular Estadísticas para el Log
        total_days = theoretical_calendar.count()
        missing_df = dq_report.filter(F.col("is_missing") == True).cache()
        
        total_missing = missing_df.count()
        lost_weekdays = missing_df.filter(F.col("day_type") == "Weekday").count()
        normal_weekends = missing_df.filter(F.col("day_type") == "Weekend").count()

        # Liberamos memoria de la caché del conteo de control
        missing_df.unpersist()

        # Print/Log Estructurado del reporte general
        self.logger.warning("=========================================================")
        self.logger.warning("  REPORTÉ DE CALIDAD DE DATOS (DATA QUALITY SUMMARY)")
        self.logger.warning(f" -> Período Evaluado: {min_date} al {max_date}")
        self.logger.warning(f" -> Total de días teóricos: {total_days}")
        self.logger.warning(f" -> Total de días faltantes (Gaps): {total_missing}")
        self.logger.warning(f"     Datos perdidos (Días de semana): {lost_weekdays}")
        self.logger.warning(f"     Fines de semana sin publicación: {normal_weekends}")
        self.logger.warning(f" -> Cobertura del Dataset: {((total_days - total_missing) / total_days) * 100:.2f}%")
        self.logger.warning("=========================================================")
        df_quality_results = dq_report.withColumn("execution_timestamp", F.current_timestamp())
        
        return df_quality_results