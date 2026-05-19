# API Config
FECHA_INICIO_HISTORICA = "2026-05-06"
FECHA_FIN_HISTORICA = "2026-05-08"

# Configuración de monedas
BASE_CURRENCY = "USD"
MONEDAS = "MXN,EUR,BRL,GBP"  # MXN, EUR + 2 adicionales (BRL, GBP)

# URLs y Endpoints
FRANKFURTER_BASE_URL = "https://api.frankfurter.app"

# Nombres de los parámetros que exige la API
PARAM_BASE = "base"
TARGET = "target"
PARAM_SYMBOLS = "symbols"
PARAM_RATES = "rates"
DATE = "date"
RATE = "rate"
PREV_RATE = "prev_rate"
DAILY_VARIATION_PCT = "daily_variation_pct"
SMA_7D = "sma_7d"
SMA_30D = "sma_30d"
VOLATILITY_30D = "volatility_30d"
YEAR = "year"
MONTH = "month"

#Codigos de retorno HTTP específicos de la API de Frankfurter
HTTP_STATUS_SUCCESS = 200
HTTP_STATUS_BAD_REQUEST = 400
HTTP_STATUS_NOT_FOUND = 404
HTTP_STATUS_RATE_LIMIT = 429
HTTP_STATUS_SERVER_ERROR = 500

# Iceberg Config
CATALOG = "local"
DATABASE = "db"
TABLE_NAME_EX_RATE = f"{CATALOG}.{DATABASE}.tipos_cambio"
WAREHOUSE = "/app/warehouse"

RATE_MIN_LIMIT = 0.00001  # Una tasa no puede ser cero ni negativa
RATE_MAX_LIMIT = 500000.0 # Límite razonable para evitar anomalías o corrupción de la API
DEFAULT_EXCHANGE_RATE = 1.0 # Valor por defecto 

# API Retry Configuration
API_TIMEOUT = 15  # segundos
RATE_LIMIT_RETRY_DELAY = 5  # segundos
MAX_RETRY_ATTEMPTS = 3  # número máximo de reintentos

TABLE_NAME_ENRICHED = f"{CATALOG}.{DATABASE}.tipos_cambio_enriquecidos"
TABLE_NAME_MONTHLY_METRICS = f"{CATALOG}.{DATABASE}.metricas_mensuales"
TABLE_NAME_ANOMALIES = f"{CATALOG}.{DATABASE}.anomalias"
TABLE_NAME_DATA_QUALITY = f"{CATALOG}.{DATABASE}.reporte_calidad"