import pytest
from extract.external_api import FrankfurterAPI

# --- 1. CONFIGURACIÓN COMPARTIDA (FIXTURE REAL) ---

@pytest.fixture(scope="module")
def api_response():
    """
    Hace UNA SOLA llamada real a internet al inicio.
    Guarda el resultado en memoria para que los 4 tests lo analicen sin saturar la API.
    """
    client = FrankfurterAPI(max_retries=2)
    start_date = "2026-05-04"
    end_date = "2026-05-08"
    
    # Retornamos los datos reales y las fechas para usarlas en las aserciones
    data = client.fetch_exchange_rates(start_date, end_date)
    return {
        "data": data,
        "start_date": start_date,
        "end_date": end_date
    }


# --- 2. LOS 4 TESTS SEPARADOS ---

def test_api_returns_valid_data(api_response):
    """Validación 1: Garantiza conexión exitosa (HTTP 200) y que llegue información."""
    result = api_response["data"]
    assert result is not None, "Error: La API real no respondió o devolvió un código de error."


def test_api_base_currency_is_usd(api_response):
    """Validación 2: Verifica que la moneda base del contrato siga siendo USD."""
    result = api_response["data"]
    # Corregí el detalle de "USDt" que tenías en tu ejemplo para que pase limpio
    assert result["base"] == "USD", f"Error: El contrato cambió, la base ya no es USD, llegó: {result['base']}"


def test_api_contains_requested_dates(api_response):
    """Validación 3: Comprueba que el JSON mapee la fecha solicitada en el diccionario de observaciones."""
    result = api_response["data"]
    start_date = api_response["start_date"]
    assert start_date in result["rates"], f"Error: El JSON no contiene la fecha solicitada {start_date} en 'rates'."


def test_api_contains_mxn_rates(api_response):
    """Validación 4: Asegura que el Peso Mexicano (MXN) exista dentro de las tasas de cambio."""
    result = api_response["data"]
    start_date = api_response["start_date"]
    assert "MXN" in result["rates"][start_date], f"Error: La llave 'MXN' no se encontró dentro de las monedas devueltas."