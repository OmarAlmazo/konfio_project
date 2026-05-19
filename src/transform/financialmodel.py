import pyspark.sql.functions as F
from pyspark.sql import DataFrame
import logging

class FinancialModeler:
    def __init__(self, spark_session):
        self.spark = spark_session
        self.logger = logging.getLogger("FinancialModeler")

    def build_dimensions(self):
        """
        Construye las tablas de dimensiones (Catálogos).
        """
        # 1. DIMENSIÓN DE MONEDAS
        # GRANO: Una fila = Una moneda única soportada por el sistema.
        currencies_data = [
            ("USD", "United States Dollar"),
            ("EUR", "Euro"),
            ("BRL", "Brazilian Real"),
            ("MXN", "Mexican Peso")
        ]
        dim_currency = self.spark.createDataFrame(currencies_data, ["currency_code", "currency_name"])

        # 2. DIMENSIÓN DE CLIENTES (Punto extra de la rúbrica)
        # GRANO: Una fila = Un cliente único del banco.
        customers_data = [
            ("CUST-001", "Omar Almazo", "Premium"),
            ("CUST-002", "Bruce Wayne", "Standard")
        ]
        dim_customer = self.spark.createDataFrame(customers_data, ["customer_id", "customer_name", "segment"])

        return dim_currency, dim_customer

    def build_facts(self, df_cdc_final: DataFrame, dim_customer: DataFrame):
        """
        Construye las tablas de hechos a partir de datos transaccionales, CDC y Dimensiones.
        """        
        # Si viene 'ingested_at' en lugar de 'ingestion_timestamp', la renombramos para evitar NameError.
        if "ingested_at" in df_cdc_final.columns and "ingestion_timestamp" not in df_cdc_final.columns:
            df_cdc_final = df_cdc_final.withColumnRenamed("ingested_at", "ingestion_timestamp")

        # 1. HECHOS DE TIPO DE CAMBIO
        fact_exchange_rates = df_cdc_final.filter(F.col("operation_type") != "DELETE").select(
            "date", "base", "target", "rate", "ingestion_timestamp"
        )

        # 2. HECHOS DE TRANSACCIONES (Punto extra de la rúbrica)
        transactions_data = [
            ("TXN-100", "2026-05-08", "CUST-001", "BRL", 250.00), 
            ("TXN-101", "2026-05-08", "CUST-002", "EUR", 15.50)   
        ]
        fact_transactions = self.spark.createDataFrame(
            transactions_data, 
            ["transaction_id", "date", "customer_id", "target_currency", "local_amount"]
        )

        # 🚀 UNIFICACIÓN DE MÁXIMO RENDIMIENTO:
        df_reporte_final = (fact_transactions.alias("tx")
            # 1. Unificamos con la Dimensión que ahora sí entra por parámetro
            .join(dim_customer.alias("cust"), 
                  F.col("tx.customer_id") == F.col("cust.customer_id"), 
                  "inner")

            # 2. Unificamos con Tipos de Cambio usando la LLAVE COMBINADA (Fecha y Moneda)
            .join(fact_exchange_rates.alias("fx"), 
                  (F.col("tx.date") == F.col("fx.date")) & 
                  (F.col("tx.target_currency") == F.col("fx.target")), 
                  "inner")

            # 3. Calculamos la unificación financiera en tiempo real
            .withColumn("amount_in_usd", F.col("tx.local_amount") / F.col("fx.rate"))

            # 4. Seleccionamos campos finales de negocio
            .select(
                F.col("tx.transaction_id"),
                F.col("tx.date"),
                F.col("cust.customer_name").alias("cliente"),
                F.col("cust.segment").alias("nivel"),
                F.col("tx.target_currency").alias("moneda_origen"),
                F.col("tx.local_amount").alias("monto_local"),
                F.col("fx.rate").alias("tipo_cambio_del_dia"),
                F.round(F.col("amount_in_usd"), 2).alias("total_en_usd")
            )
        )
        
        self.logger.warning(">>>> [DATAMART] Reporte Final de Transacciones Unificadas:")
        df_reporte_final.show()        

        return fact_exchange_rates, fact_transactions