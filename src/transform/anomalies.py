#from pyspark.sql import SparkSession, functions as F
from pyspark.sql import functions as F

class AnomalyDetector:

    def detect_anomalies(self, df):
            """
            Identifica registros donde el rate se desvía más de 2 sigmas 
            respecto al promedio móvil de 30 días.
            """
            if not df or df.count() == 0:
                return df
    
            # Un registro es anomalía si: |Precio Actual - Promedio| > (2 * Volatilidad)
            # También validamos que volatility_30d > 0 para evitar el error de los primeros registros
            df_anomalies = df.withColumn(
                "is_anomaly",
                F.when(
                    (F.abs(F.col("rate") - F.col("sma_30d")) > (F.lit(2) * F.col("volatility_30d"))) &
                    (F.col("volatility_30d") > 0),
                    True
                ).otherwise(False)
            )
    
            # Agregamos el Z-Score para saber cuántas desviaciones estándar se movió exactamente
            df_anomalies = df_anomalies.withColumn(
                "z_score",
                F.when(F.col("volatility_30d") > 0,
                       F.round((F.col("rate") - F.col("sma_30d")) / F.col("volatility_30d"), 2)
                ).otherwise(0.0)
            )
    
            print(">>> [QUALITY] Detección de anomalías finalizada.")
            return df_anomalies