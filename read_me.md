# Proyecto ELT Criptomonedas con Delta Lake

Este proyecto implementa un pipeline ELT completo en Python que extrae información desde la API de CoinGecko, la almacena en una **capa Bronze sin transformación**, luego aplica limpiezas y transformaciones para almacenar los datos depurados en **Silver**. Se utiliza **Delta Lake** como formato de almacenamiento y partición de datos.

## Estructura del Proyecto

```
cripto_etl_v3/
├── config/
│ ├── paths.py # Rutas absolutas y acceso a variables de entorno
│ └── metadata.py # Gestión de extracción incremental
├── datalake/
│ ├── bronze/ # Datos crudos extraídos desde la API
│ ├── silver/ # Datos limpios y transformados
│ └── gold/ # Capa para agregados y KPIs (Aun NO utilizada)
├── extract/
│ └── api.py # Funciones de extracción desde la API de CoinGecko
├── load/
│ └── delta_writer.py # Funciones para guardar en formato Delta Lake
├── transform/
│ └── clean_data.py # Transformaciones específicas por cada dataset
├── main_bronze.py # Extracción desde la API y guardado en capa Bronze
├── main_silver.py # Transformaciones y guardado en capa Silver
├── .env # Variables de entorno como la API Key
├── read_me.md # Documentación del proyecto
```

## Justificación del diseño

### 1. Enfoque ELT

- Se adoptó una arquitectura moderna **ELT con capas Bronze, Silver y Gold** para mayor claridad, trazabilidad y mantenibilidad.

### 2. Extracción de datos

- `main_bronze.py` extrae datos desde CoinGecko:
  - **Incremental** para `coin_market_data` (con control de timestamp).
  - **Full load** para `exchanges`.

### 3. Almacenamiento en Delta Lake

- Los datos crudos se almacenan en la carpeta `datalake/bronze/` usando formato Delta.
- En Silver se sobrescriben los datos transformados (`overwrite`).
- **No se utiliza `merge_to_delta`** por estabilidad y simplicidad de esquema.

### 4. Transformaciones en Silver

- Se aplican:
  - Conversión de fechas, nombres y tipos.
  - Conversión de montos a millones.
  - Validación y eliminación de duplicados (`drop_duplicates` por `id`).

### 5. Buenas prácticas

- Uso de `.env` para guardar la API Key de forma segura.
- Separación por capas y responsabilidades.
- Modularidad clara entre extracción, transformación y carga.
- Logs informativos y validación de consistencia de datos.

---
