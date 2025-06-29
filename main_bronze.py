from extract.api import get_data, build_table
from config.paths import (
    BRONZE_COIN_MARKET,
    BRONZE_EXCHANGES,
    COINGECKO_API_KEY
)
from config.metadata import get_last_timestamp, update_last_timestamp
from load.delta_writer import save_to_delta
from datetime import datetime, timezone

headers = {'x-cg-demo-api-key': COINGECKO_API_KEY}
base_url = 'https://api.coingecko.com/api/v3'

# --- Coin Market Data (Incremental) ---
params = {'vs_currency': 'usd', 'order': 'market_cap_desc'}
coin_data = get_data(base_url, 'coins/markets', headers, params)
df_coin_raw = build_table(coin_data)

if df_coin_raw is not None and not df_coin_raw.empty:
    now = datetime.now(timezone.utc)
    df_coin_raw['load_timestamp'] = now
    df_coin_raw['load_day'] = now.strftime('%Y-%m-%d')

    last_loaded = get_last_timestamp('coin_market_data')
    df_coin_new = df_coin_raw[df_coin_raw['load_timestamp'] > last_loaded]

    if not df_coin_new.empty:
        save_to_delta(df_coin_new, BRONZE_COIN_MARKET, mode='overwrite')
        update_last_timestamp('coin_market_data', df_coin_new['load_timestamp'].max())
        print(f'[BRONZE] Coin Market: {len(df_coin_new)} nuevos registros guardados.')
    else:
        print('[BRONZE] Coin Market: No hay nuevos datos.')
else:
    print('[BRONZE] Coin Market: Error o datos vacíos.')

# --- Exchanges Data (Full Load) ---
params = {'per_page': 10}
exchange_data = get_data(base_url, 'exchanges', headers, params)
df_ex_raw = build_table(exchange_data)

if df_ex_raw is not None and not df_ex_raw.empty:
    now = datetime.now(timezone.utc)
    df_ex_raw['load_timestamp'] = now
    df_ex_raw['load_day'] = now.strftime('%Y-%m-%d')  # NUEVA COLUMNA

    save_to_delta(df_ex_raw, BRONZE_EXCHANGES, mode='overwrite')
    print(f'[BRONZE] Exchanges: {len(df_ex_raw)} registros cargados (full load).')
else:
    print('[BRONZE] Exchanges: Error o datos vacíos.')