from pyspark.sql import Window
from pyspark.sql import DataFrame
import pyspark.sql.functions as F
import utils.constants as c
import logging 
from typing import Optional


class MetricsCalculator:

    def __init__(self):
        self.logger = logging.getLogger("MetricsCalculator")

    def add_derived_fields(self, df: Optional[DataFrame]) -> Optional[DataFrame]:
        """
        Calcula métricas financieras mediante Window Functions:
        - Variación diaria (%)
        - Promedios móviles (7 y 30 días)
        - Volatilidad (Desviación estándar de 30 días)
        """
                
        if not df or df.isEmpty():
            return df
        cols_exclude = ["ingested_at", "row_hash", "operation_type", "ingestion_timestamp"]
        columns = [F.col(c) for c in df.columns if c not in cols_exclude]
        df_filter = df.select(
            *columns           
        )

        # 1. Definición de las Ventanas (Window Specs)
        # Particionamos por par de monedas para no mezclar cálculos (ej. USD-MXN con EUR-MXN)
        base_window = Window.partitionBy(c.PARAM_BASE, c.TARGET).orderBy(c.DATE)
        
        # Ventanas para cálculos móviles (rowsBetween: desde N días atrás hasta la fila actual)
        win_7d = base_window.rowsBetween(-6, 0)
        win_30d = base_window.rowsBetween(-29, 0)

        # 2. Aplicación de cálculos en cadena (Method Chaining)
        df_metrics = (df_filter
            # Obtenemos el tipo de cambio del día anterior
            .withColumn(c.PREV_RATE, F.lag(c.RATE, 1).over(base_window))
            
            # Calculamos la variación porcentual con protección de división por cero
            .withColumn(c.DAILY_VARIATION_PCT, 
                F.when((F.col(c.PREV_RATE).isNotNull()) & (F.col(c.PREV_RATE) > 0),
                       ((F.col(c.RATE) - F.col(c.PREV_RATE)) / F.col(c.PREV_RATE)) * 100
                ).otherwise(0.0)
            )
            
            # Calculamos promedios móviles
            .withColumn(c.SMA_7D, F.avg(c.RATE).over(win_7d))
            .withColumn(c.SMA_30D, F.avg(c.RATE).over(win_30d))
            
            # Calculamos volatilidad (stddev requiere al menos 2 valores para no dar null)
            .withColumn(c.VOLATILITY_30D, 
                F.coalesce(F.stddev(c.RATE).over(win_30d), F.lit(0.0))
            )
            .withColumn("ingestion_timestamp", F.current_timestamp())
        )

        self.logger.warning(">>> [METRICS] Campos derivados calculados exitosamente.")
        df_last = df_metrics.drop(c.PREV_RATE)
        return df_last