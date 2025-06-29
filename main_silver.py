from datetime import datetime, timezone
import pandas as pd
from deltalake import DeltaTable
from config.paths import (
    BRONZE_COIN_MARKET,
    BRONZE_EXCHANGES,
    SILVER_COIN_MARKET,
    SILVER_EXCHANGES
)
from transform.clean_data import transform_coin_market_data, transform_exchanges_data
from load.delta_writer import save_to_delta

# Obtener la fecha actual como string para filtrar
today = datetime.now(timezone.utc).strftime('%Y-%m-%d')

# --- Leer Coin Market desde Bronze y transformar ---
try:
    dt_coin = DeltaTable(BRONZE_COIN_MARKET)
    df_coin_bronze = dt_coin.to_pandas()

    # Filtrar solo registros del día actual
    df_coin_bronze = df_coin_bronze[df_coin_bronze['load_day'] == today]

    df_coin_silver = transform_coin_market_data(df_coin_bronze)
    save_to_delta(df_coin_silver, SILVER_COIN_MARKET, mode='overwrite')
    print(f'[SILVER] Coin Market: {len(df_coin_silver)} registros transformados y cargados.')
except Exception as e:
    print(f'[SILVER] Coin Market: Error procesando tabla Bronze → Silver: {e}')

# --- Leer Exchanges desde Bronze y transformar ---
try:
    dt_ex = DeltaTable(BRONZE_EXCHANGES)
    df_ex_bronze = dt_ex.to_pandas()

    # Filtrar solo registros del día actual
    df_ex_bronze = df_ex_bronze[df_ex_bronze['load_day'] == today]

    df_ex_silver = transform_exchanges_data(df_ex_bronze)
    save_to_delta(df_ex_silver, SILVER_EXCHANGES, mode='overwrite')
    print(f'[SILVER] Exchanges: {len(df_ex_silver)} registros transformados y cargados.')
except Exception as e:
    print(f'[SILVER] Exchanges: Error procesando tabla Bronze → Silver: {e}')