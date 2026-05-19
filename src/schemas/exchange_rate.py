from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType


class ExchangeRateSchema:
    def _get_schema(self):
        """Define el esquema tipado requerido para la tabla de tipos de cambio."""
        return StructType([
            StructField("date", StringType(), False),
            StructField("base_amount", DoubleType(), False),
            StructField("base", StringType(), False),
            StructField("target", StringType(), False),
            StructField("rate", DoubleType(), False)
        ])

