import pandas as pd
from datetime import datetime, timezone

def transform_coin_market_data(df: pd.DataFrame) -> pd.DataFrame:
    # Parseo de fechas en formato datetime
    df['ath_date'] = pd.to_datetime(df['ath_date'], errors='coerce')
    df['atl_date'] = pd.to_datetime(df['atl_date'], errors='coerce')

    # Eliminamos duplicados
    df = df.drop_duplicates(subset='id', keep='last')

    # Columnas a redondear
    cols_to_round = ['current_price', 'ath', 'atl']
    df[cols_to_round] = df[cols_to_round].round(2)

    # Convertir columnas a millones
    cols_to_millions = ['market_cap', 'circulating_supply', 'total_supply']
    for col in cols_to_millions:
        df[col] = (df[col] / 1_000_000).round(2)

    # Renombrar
    df = df.rename(columns={
        'market_cap': 'market_cap_millions',
        'circulating_supply': 'circulating_supply_millions',
        'total_supply': 'total_supply_millions'
    })

    # Timestamp de carga
    df['load_timestamp'] = datetime.now(timezone.utc)

    # Filtrar columnas
    df = df[[
        'id', 'symbol', 'name', 'current_price', 'market_cap_millions',
        'market_cap_rank', 'circulating_supply_millions',
        'total_supply_millions', 'ath', 'ath_date', 'atl', 'atl_date',
        'load_timestamp'
    ]]

    # Tipos correctos
    df = df.astype({
        'id': 'string',
        'symbol': 'string',
        'name': 'string',
        'current_price': 'float64',
        'market_cap_millions': 'float64',
        'market_cap_rank': 'Int64',
        'circulating_supply_millions': 'float64',
        'total_supply_millions': 'float64',
        'ath': 'float64',
        'atl': 'float64'
    })

    # Formateo de fechas a dd/mm/YYYY
    df['ath_date'] = df['ath_date'].dt.strftime('%d/%m/%Y')
    df['atl_date'] = df['atl_date'].dt.strftime('%d/%m/%Y')
    df['load_timestamp'] = df['load_timestamp'].dt.strftime('%d/%m/%Y')

    return df


def transform_exchanges_data(df: pd.DataFrame) -> pd.DataFrame:
    # Eliminar columnas no necesarias
    df = df.drop(columns=[
        'url', 'image', 'has_trading_incentive', 'trade_volume_24h_btc_normalized'
    ], errors='ignore')

    # Redondear volumen a 2 decimales
    df['trade_volume_24h_btc'] = df['trade_volume_24h_btc'].round(2)

    # Agregar timestamp de carga
    df['load_timestamp'] = datetime.now(timezone.utc)

    # Filtrar columnas deseadas
    df = df[[
        'id', 'name', 'year_established', 'country', 'description',
        'trust_score', 'trust_score_rank', 'trade_volume_24h_btc',
        'load_timestamp'
    ]]

    # Asignar tipos de datos
    df = df.astype({
        'id': 'string',
        'name': 'string',
        'year_established': 'Int64',
        'country': 'string',
        'description': 'string',
        'trust_score': 'Int64',
        'trust_score_rank': 'Int64',
        'trade_volume_24h_btc': 'float64'
    })

    # Formatear fecha
    df['load_timestamp'] = df['load_timestamp'].dt.strftime('%d/%m/%Y')

    return df
