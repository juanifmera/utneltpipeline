import os
import pandas as pd
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake

def save_to_delta(df: pd.DataFrame, path: str, mode: str = 'overwrite'):
    if not os.path.exists(path):
        os.makedirs(path)

    df = df.convert_dtypes()
    write_deltalake(table_or_uri=path, data=df, mode=mode)
    print(f'Delta table escrita en: {path}')

def merge_to_delta(df_new: pd.DataFrame, path: str, primary_key: str):
    '''
    Realiza un UPSERT en una tabla Delta Lake utilizando PyArrow.
    :param df_new: Nuevos datos (DataFrame)
    :param path: Ruta Delta
    :param primary_key: Columna clave primaria
    '''
    df_new['load_timestamp'] = pd.to_datetime(df_new['load_timestamp'], utc=True)
    df_new['id'] = df_new['id'].astype(str)

    table_new = pa.Table.from_pandas(df_new, preserve_index=False)

    try:
        dt = DeltaTable(path)
        (
            dt.merge(
                source=table_new,
                predicate=f'target.{primary_key} = source.{primary_key}',
                source_alias='source',
                target_alias='target'
            )
            .when_matched_update_all()
            .when_not_matched_insert_all()
            .execute()
        )
        print(f'MERGE aplicado en: {path}')
    except Exception as e:
        print(f'No se encontro tabla previa o error en el schema. Se creará nueva: {e}')
        write_deltalake(table_or_uri=path, data=df_new, mode='overwrite')
        print(f'Delta table creada en: {path}')
