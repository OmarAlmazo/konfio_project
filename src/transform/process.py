from datetime import datetime, timedelta
from pyspark.sql import functions as F
from schemas.exchange_rate import ExchangeRateSchema
from utils import constants as c
import logging 


class DataProcessor:

    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger("DataProcessor")
        self.exchange_rate_schema = ExchangeRateSchema()

    def format(self, raw_data):
        """
        Convierte JSON crudo a DataFrame tipado (FORMAT).
        Trae la información directa de la API sin lógica de más para fechas faltantes.
        """
        base = raw_data.get(c.PARAM_BASE, c.BASE_CURRENCY)
        rates_dict = raw_data.get(c.PARAM_RATES, {})

        if not rates_dict:
            schema = self.exchange_rate_schema._get_schema()
            return self.spark.createDataFrame([], schema)

        # lista por comprensión.
        rows = [
            (date_str, c.DEFAULT_EXCHANGE_RATE, base, curr, float(value))
            for date_str, currencies in rates_dict.items()
            for curr, value in currencies.items()
        ]

        # 3. Construimos el DataFrame con Schema
        schema = self.exchange_rate_schema._get_schema()
        df = self.spark.createDataFrame(rows, schema)
        
        try:
            self.logger.info(">>> [FORMAT] Creando DataFrame de Spark con esquema estricto...")
            schema = self.exchange_rate_schema._get_schema()
            df = self.spark.createDataFrame(rows, schema)
            
        except TypeError as te:
            self.logger.error(f">>> [FATAL] Error de tipado al construir el DataFrame. Revisar estructura de la API: {str(te)}")
            raise te

        except Exception as e:
            self.logger.error(f">>> [FATAL] Error inesperado en la validación de esquema: {str(e)}")
            raise e        

        final_df = df.withColumn("ingested_at", F.current_timestamp())
        self.logger.warning(f">>> [FORMAT] Se crea DataFrame con schema sin modificaciones")
        return final_df

    def clean_and_validate(self, df):
           """
           1. Elimina filas con nulos en columnas clave.
           2. Valida rangos de tasas (positivas y límites razonables).
           3. Elimina duplicados por llave compuesta.
           """

           if df.isEmpty():
               self.logger.warning(">>> [QUALITY] DataFrame vacío. Saltando limpieza.")
               return df

           self.logger.warning(">>> [QUALITY] Iniciando validación de calidad de datos en el DataFrame...")
  
           # 1. Manejar Nulos: Eliminamos registros que no tengan las llaves completas o la tasa
           df_clean = df.na.drop(subset=["date", "base", "target", "rate"])

           # 2. Validar Rangos: Filtramos tasas fuera de parámetros lógicos de negocio
           df_clean = df_clean.filter(
               (F.col("rate") >= c.RATE_MIN_LIMIT) & 
               (F.col("rate") <= c.RATE_MAX_LIMIT)
           )

           # 3. Eliminar Duplicados: Garantizamos unicidad por la llave (fecha + origen + destino)
           df_clean = df_clean.dropDuplicates(["date", "base", "target"])
           self.logger.warning(">>> [QUALITY] Transformación y reglas de calidad aplicadas exitosamente.")
           return df_clean