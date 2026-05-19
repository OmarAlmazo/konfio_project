import requests
import time
import logging
from typing import Optional, Dict, Any
from datetime import datetime
from utils import constants as c

# Configurar logging



class FrankfurterAPI:
    def __init__(self, max_retries: int = c.MAX_RETRY_ATTEMPTS):
        """Inicializa el cliente de API con configuración de reintentos."""
        self.max_retries = max_retries
        self.logger = logging.getLogger("FrankfurterClient")
    
    def _validate_dates(self, start_date: str, end_date: str) -> bool:
        """Valida que las fechas tengan formato válido (YYYY-MM-DD)."""
        try:
            datetime.strptime(start_date, "%Y-%m-%d")
            datetime.strptime(end_date, "%Y-%m-%d")
            return True
        except ValueError:
            self.logger.error(f">>> [VALIDATION] Formato de fecha inválido. Esperado YYYY-MM-DD, recibido: {start_date}..{end_date}")
            return False
    
    def fetch_exchange_rates(self, start_date: str, end_date: str, attempt: int = 1) -> Optional[Dict[str, Any]]:
        """
        Extrae datos manejando errores HTTP específicos, rate limiting y timeouts.
        
        Args:
            start_date: Fecha inicial en formato YYYY-MM-DD
            end_date: Fecha final en formato YYYY-MM-DD
            attempt: Número de intento actual (uso interno)
            
        Returns:
            Dict con datos de tipos de cambio o None si hay error
        """
        # Validar fechas solo en el primer intento
        if attempt == 1 and not self._validate_dates(start_date, end_date):
            return None
        
        url = f"{c.FRANKFURTER_BASE_URL}/{start_date}..{end_date}"
        params = {
            c.PARAM_BASE: c.BASE_CURRENCY,
            c.PARAM_SYMBOLS: c.MONEDAS
        }
        
        try:
            self.logger.debug(f"Intento {attempt}/{self.max_retries} - Llamada a API: {url}")
            response = requests.get(url, params=params, timeout=c.API_TIMEOUT)
            
            # Para probar el Errores o llamdas incorrectas
            # response = type('Response', (object,), {'status_code': 500, 'json': lambda: None})
            #raise requests.exceptions.ConnectionError("Simulación de Timeout: El servidor tardó más de 15 segundos")
            
            # --- CONTROL DE STATUS CODES ---
            
            # 200: Éxito 
            if response.status_code == c.HTTP_STATUS_SUCCESS:
                self.logger.warning(f">>> [API 200] Datos extraídos correctamente para {start_date}..{end_date}")                
                return response.json()
            
            # 400: Parámetros mal formados (fechas inválidas o monedas mal escritas)
            elif response.status_code == c.HTTP_STATUS_BAD_REQUEST:
                self.logger.warning(f">>> [API 400] Bad Request: Estructura de petición inválida para el rango {start_date}..{end_date}")
                return None
            
            # 404: No encontrado
            elif response.status_code == c.HTTP_STATUS_NOT_FOUND:
                self.logger.warning(f">>> [API 404] El rango solicitado no contiene registros disponibles.")
                return None
            
            # 429: Rate Limiting con reintentos limitados
            elif response.status_code == c.HTTP_STATUS_RATE_LIMIT:
                if attempt < self.max_retries:
                    self.logger.warning(f">>> [API 429] Rate limit detectado. Reintentando en {c.RATE_LIMIT_RETRY_DELAY}s ({attempt}/{self.max_retries})")
                    time.sleep(c.RATE_LIMIT_RETRY_DELAY)
                    return self.fetch_exchange_rates(start_date, end_date, attempt + 1)
                else:
                    self.logger.error(f">>> [API 429] Rate limit alcanzado después de {self.max_retries} reintentos")
                    return None
            
            # 5xx: Errores del servidor externo
            elif 500 <= response.status_code < 600:
                self.logger.error(f">>> [API {response.status_code}] Error interno del servidor externo")
                return None
            
            # Salvaguarda para cualquier otro código inesperado
            else:
                self.logger.warning(f">>> [API {response.status_code}] Código de estado inesperado")
                return None
            
        except requests.exceptions.Timeout:
            self.logger.error(f">>> [TIMEOUT] API tardó más de {c.API_TIMEOUT}s en responder")
        except requests.exceptions.ConnectionError as conn_err:
            self.logger.error(f">>> [CONNECTION ERROR] No se pudo conectar a la API: {conn_err}")
        except requests.exceptions.HTTPError as http_err:
            self.logger.error(f">>> [HTTP ERROR] Error inesperado: {http_err}")
        except requests.exceptions.RequestException as e:
            self.logger.error(f">>> [REQUEST ERROR] Fallo en la comunicación con la API: {e}")
        except Exception as e:
            self.logger.critical(f">>> [UNEXPECTED ERROR] Error no controlado: {type(e).__name__} - {e}")
            
        return None