import requests
import time
import pandas as pd

def get_data(base_url, end_point, headers=None, params=None, data_field=None, max_retries=3, timeout=10):
    full_url = f'{base_url.rstrip("/")}/{end_point.lstrip("/")}'
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.get(full_url, headers=headers, params=params, timeout=timeout)
            response.raise_for_status()
            data = response.json()
            return data.get(data_field, data) if data_field else data
        except requests.exceptions.Timeout:
            print(f'Timeout en intento {attempt}/{max_retries} para endpoint: {end_point}')
        except requests.exceptions.RequestException as e:
            print(f'Error de conexion en intento {attempt}/{max_retries}: {e}')
        time.sleep(2)
    print(f'No se pudo obtener respuesta del endpoint: {end_point} tras {max_retries} intentos.')
    return None

def build_table(information):
    try:
        return pd.json_normalize(information)
    except Exception as e:
        print(f'Error al convertir JSON a DataFrame: {e}')
        return None
