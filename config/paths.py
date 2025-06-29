import os
from dotenv import load_dotenv

load_dotenv()
# Ultimamente estoy teniendo varios problemas con los paths relativos por ende utiliso OS para controlar las variables y utilizo Paths absolutos para que no ocurra ningun error al mover las cosas de lugar

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# Rutas generales
DATALAKE_DIR = os.path.join(BASE_DIR, 'datalake')
os.makedirs(DATALAKE_DIR, exist_ok=True)

# Capas ELT
BRONZE_DIR = os.path.join(DATALAKE_DIR, 'bronze')
SILVER_DIR = os.path.join(DATALAKE_DIR, 'silver')
GOLD_DIR = os.path.join(DATALAKE_DIR, 'gold')

# Rutas específicas para cada dataset por capa
BRONZE_COIN_MARKET = os.path.join(BRONZE_DIR, 'coin_market_data')
BRONZE_EXCHANGES = os.path.join(BRONZE_DIR, 'exchanges')

SILVER_COIN_MARKET = os.path.join(SILVER_DIR, 'coin_market_data')
SILVER_EXCHANGES = os.path.join(SILVER_DIR, 'exchanges')

GOLD_COIN_MARKET = os.path.join(GOLD_DIR, 'coin_market_data')
GOLD_EXCHANGES = os.path.join(GOLD_DIR, 'exchanges')

# Variables de entorno
COINGECKO_API_KEY = os.getenv('COINGECKO_API_KEY')
