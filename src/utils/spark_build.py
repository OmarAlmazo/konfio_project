from pyspark.sql import SparkSession
from utils import constants as c

class SparkBuilder:
    def _create_spark_session(self):
        # 1. Creamos o recuperamos la sesión de Spark con tus configs de Iceberg
        spark = SparkSession.builder \
            .appName("KonfioPipeline") \
            .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
            .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
            .config("spark.sql.catalog.local.type", "hadoop") \
            .config("spark.sql.catalog.local.warehouse", c.WAREHOUSE) \
            .getOrCreate()
        
        # 2. Le cambiamos el nivel de log al contexto antes de regresar la sesión
        spark.sparkContext.setLogLevel("WARN")
        
        return spark