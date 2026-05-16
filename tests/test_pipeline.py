import pytest
#from src.main import get_data_from_api
from main import get_data_from_api

def test_get_data_from_api_structure():
    """Prueba que la API devuelva la estructura esperada"""
    data = get_data_from_api()
    
    # Verificamos que sea un diccionario
    assert isinstance(data, dict)
    
    # Verificamos que contenga las llaves principales
    assert "rates" in data
    assert "base" in data
    
    # Verificamos que traiga datos (que no esté vacío)
    assert len(data["rates"]) > 0

def test_exchange_rate_is_numeric():
    """Prueba que los valores de las monedas sean números"""
    data = get_data_from_api()
    usd_rate = data["rates"].get("MXN") # Verificamos pesos mexicanos
    
    if usd_rate:
        assert isinstance(usd_rate, (float, int))