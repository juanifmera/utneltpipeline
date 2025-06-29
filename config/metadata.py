import os
import pandas as pd
from datetime import datetime, timezone
from deltalake import DeltaTable
from deltalake.writer import write_deltalake
from config.paths import DATALAKE_DIR

META_PATH = os.path.join(DATALAKE_DIR, 'etl_metadata')

def get_last_timestamp(table_name: str) -> datetime:
    try:
        dt = DeltaTable(META_PATH)
        df = dt.to_pandas()
        row = df[df['table'] == table_name].sort_values('last_loaded', ascending=False).head(1)
        if not row.empty:
            return pd.to_datetime(row.iloc[0]['last_loaded'])
    except Exception as e:
        print(f'No se pudo leer metadata para "{table_name}": {e}')
    return datetime(1970, 1, 1, tzinfo=timezone.utc)

def update_last_timestamp(table_name: str, new_timestamp: datetime):
    df = pd.DataFrame([{
        'table': table_name,
        'last_loaded': new_timestamp.replace(microsecond=0)
    }])
    write_deltalake(
        table_or_uri=META_PATH,
        data=df,
        mode='append'
    )
    print(f'Metadata actualizada para "{table_name}": {new_timestamp}')
