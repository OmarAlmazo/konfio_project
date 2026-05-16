import os
import requests
from datetime import datetime, timedelta
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# --- 1. CONFIGURACIÓN DE SESIÓN SPARK ---
def create_spark_session():
    return SparkSession.builder \
        .appName("KonfioPipeline") \
        .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions") \
        .config("spark.sql.catalog.local", "org.apache.iceberg.spark.SparkCatalog") \
        .config("spark.sql.catalog.local.type", "hadoop") \
        .config("spark.sql.catalog.local.warehouse", "/app/warehouse") \
        .getOrCreate()

# --- 2. EXTRACCIÓN CON MANEJO DE RANGOS Y ERRORES ---
def fetch_exchange_rates(start_date, end_date):
    """
    Consume la API de Frankfurter manejando reintentos básicos y errores HTTP.
    """
    url = f"https://api.frankfurter.app/{start_date}..{end_date}"
    params = {
        "base": "USD",
        "symbols": "MXN,EUR,BRL,GBP" # Monedas requeridas + adicionales
    }
    
    try:
        print(f">>> Solicitando datos desde {start_date} hasta {end_date}...")
        response = requests.get(url, params=params, timeout=15)
        response.raise_for_status() # Lanza error si hay falla HTTP (4xx o 5xx)
        return response.json()
    except Exception as e:
        print(f"!!! Error fatal al conectar con la API: {e}")
        return None

# --- 3. PROCESAMIENTO DINÁMICO (Spark) ---
def process_to_dataframe(spark, raw_data):
    """
    Transforma el JSON anidado de la API en un DataFrame plano.
    Maneja la estructura de rangos de fechas.
    """
    rows = []
    base_currency = raw_data.get("base", "USD")
    rates_dict = raw_data.get("rates", {})

    # La API devuelve: {"rates": {"fecha": {"moneda": valor}}}
    for date_str, currencies in rates_dict.items():
        for curr, value in currencies.items():
            rows.append((date_str, 1.0, base_currency, curr, float(value)))

    schema = StructType([
        StructField("date", StringType(), True),
        StructField("base_amount", DoubleType(), True), # <--- Nueva columna
        StructField("base", StringType(), True),
        StructField("target", StringType(), True),
        StructField("rate", DoubleType(), True)
    ])

    df = spark.createDataFrame(rows, schema)
    # Metadatos de auditoría: Cuándo se procesó la información
    return df.withColumn("processed_at", F.current_timestamp())

# --- 4. PIPELINE PRINCIPAL ---
def run_pipeline():
    spark = create_spark_session()
    table_name = "local.db.exchange_rates"
    
    # Asegurar que la DB existe
    spark.sql("CREATE DATABASE IF NOT EXISTS local.db")

    # --- LÓGICA DE CARGA (HISTÓRICA VS INCREMENTAL) ---
    table_exists = spark.catalog.tableExists(table_name)
    
    if not table_exists:
        # Escenario: Primera ejecución (Backfill)
        print(">>> CARGA INICIAL DETECTADA. Procesando historial 2024...")
        start_date = "2024-01-01"
        end_date = datetime.now().strftime('%Y-%m-%d')
    else:
        # Escenario: Ejecución diaria (Incremental)
        # Pedimos los últimos 7 días para cubrir fines de semana y feriados
        print(">>> CARGA INCREMENTAL. Sincronizando última semana...")
        end_date = datetime.now().strftime('%Y-%m-%d')
        start_date = (datetime.now() - timedelta(days=7)).strftime('%Y-%m-%d')

    # Ejecución
    json_data = fetch_exchange_rates(start_date, end_date)
    
    if json_data and "rates" in json_data:
        df_final = process_to_dataframe(spark, json_data)
        
        # Guardar en Iceberg
        if not table_exists:
            # Crea la tabla con el esquema basado en el DataFrame
            df_final.writeTo(table_name).create()
            print(">>> Tabla histórica creada exitosamente.")
        else:
            """ Metodeo con anti join para evitar duplicados (si la API devuelve datos ya existentes) 
            # Leemos la tabla existente (solo la columna date para que sea rápido)
            df_existing = spark.table(table_name).select("date", "base", "target").distinct()
            # Hacemos un ANTI JOIN: 
            # "Quédate con lo de df_final que NO esté en df_existing"
            df_incremental = df_final.join(
                df_existing, 
                on=["date", "base", "target"], 
                how="left_anti"
            )

            if df_incremental.count() > 0:
                df_incremental.writeTo(table_name).append()
                print(f">>> {df_incremental.count()} registros nuevos insertados.")

            else:
                print(">>> La tabla ya está actualizada.")
            """
##################################
            """ Metodeo collect para evitar duplicados (si la API devuelve datos ya existentes)

            # Buscamos la última fecha (Eficiencia pura)
            last_date = spark.sql(f"SELECT max(date) FROM {table_name}").collect()[0][0]
            
            # Filtramos antes de tocar la tabla
            df_incremental = df_final.filter(df_final.date > last_date)
            
            if df_incremental.count() > 0:
                df_incremental.writeTo(table_name).append()
                print(f">>> {df_incremental.count()} registros nuevos insertados.")
            else:
                print(">>> La tabla ya está actualizada.")  
                """             
            # 1. Creamos una vista temporal de los datos nuevos
            df_final.createOrReplaceTempView("new_data")
            
            # 2. Insertamos usando SQL directo
            # Esto permite que el optimizador de Iceberg use los metadatos 
            # para buscar el MAX sin hacer un scan completo ni un collect.
            spark.sql(f"""
                INSERT INTO {table_name}
                SELECT * FROM new_data
                WHERE date > (SELECT MAX(date) FROM {table_name})
            """)
            
            print(">>> Carga incremental completada vía SQL (Pushdown optimization).")

        # --- VERIFICACIÓN FINAL ---
        print(">>> Verificando tabla Iceberg...")
        summary = spark.sql(f"SELECT * FROM {table_name}")
        summary.show(50) 
        summary = spark.sql(f"SELECT count(*) as total, max(date) as ultima_fecha FROM {table_name}").collect()
        print(f"Total registros: {summary[0]['total']}")

        print(f"Última actualización de datos: {summary[0]['ultima_fecha']}")
        
    else:
        print(">>> No se obtuvieron datos. Verifique logs de la API.")

if __name__ == "__main__":
    run_pipeline()