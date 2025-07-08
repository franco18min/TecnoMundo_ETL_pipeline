import os
import logging
import pandas as pd
import numpy as np
import re
import requests
import base64
import tempfile
import csv
from dotenv import load_dotenv
from databricks import sql
from pathlib import Path

# --- CONFIGURACIÓN INICIAL ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("databricks.sql").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)  # Silenciar logs de requests

try:
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent.parent.parent
    dotenv_path = project_root / 'conf' / 'env' / '.env'
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)
    else:
        load_dotenv()
except NameError:
    load_dotenv()


# --- FUNCIÓN DE UTILIDAD PARA LEER DATOS ---

def get_df_from_databricks(catalog: str, schema: str, table_name: str) -> pd.DataFrame | None:
    """Se conecta a Databricks y obtiene una tabla específica como un DataFrame de Pandas."""
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not all([server_hostname, http_path, access_token]):
        logging.error("Error: Faltan variables de conexión en el .env.")
        return None

    logging.info(f"Intentando obtener la tabla '{catalog}.{schema}.{table_name}' de Databricks...")
    try:
        with sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=access_token) as connection:
            logging.info("✅ ¡Conexión exitosa a Databricks!")
            query = f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}`"
            logging.info(f"Ejecutando consulta: {query}")
            df = pd.read_sql(query, connection)
            logging.info(f"✅ Tabla '{table_name}' cargada exitosamente con {len(df)} filas.")
            return df
    except Exception as e:
        logging.error(f"❌ Ocurrió un error al obtener los datos: {e}")
        return None


# --- FUNCIONES DE UTILIDAD PARA GUARDAR DATOS (MÉTODO PARQUET) ---

def sanitize_column_name(col_name: str) -> str:
    """Limpia un nombre de columna para que sea compatible con Databricks/SQL."""
    col_name = col_name.replace('º', '').replace('Nº', 'num').replace('ñ', 'n').replace('Ñ', 'N')
    col_name = re.sub(r'[\s\.\-/\\]+', '_', col_name)
    col_name = re.sub(r'[^a-zA-Z0-9_]', '', col_name)
    return col_name.lower()


def pandas_dtype_to_sql(dtype) -> str:
    """Convierte un tipo de dato de Pandas a un tipo de dato SQL compatible con Databricks."""
    if pd.api.types.is_integer_dtype(dtype): return "BIGINT"
    if pd.api.types.is_float_dtype(dtype): return "DOUBLE"
    if pd.api.types.is_datetime64_any_dtype(dtype): return "TIMESTAMP"
    if pd.api.types.is_bool_dtype(dtype): return "BOOLEAN"
    return "STRING"


def save_df_to_databricks_fast(df_to_save: pd.DataFrame, catalog: str, schema: str, table_name: str) -> bool:
    """
    Guarda un DataFrame en Databricks usando inserción directa de registros.
    Esta versión evita problemas con CSV al procesar los datos directamente desde el DataFrame.
    """
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not all([server_hostname, http_path, access_token]):
        logging.error("Error: Faltan variables de conexión en el .env.")
        return False

    df_clean = df_to_save.copy()
    # Limpiar nombres de columnas
    df_clean.columns = [sanitize_column_name(col) for col in df_clean.columns]
    logging.info(f"Nombres de columnas limpiados: {list(df_clean.columns)}")

    try:
        with sql.connect(server_hostname=server_hostname, http_path=http_path, access_token=access_token) as connection:
            with connection.cursor() as cursor:
                logging.info(f"Asegurando que el esquema '{schema}' exista...")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

                logging.info(f"Eliminando tabla anterior si existe: `{catalog}`.`{schema}`.`{table_name}`")
                cursor.execute(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table_name}`")

                # Crear la tabla
                cols_with_types = ", ".join([f"`{col}` {pandas_dtype_to_sql(dtype)}"
                                             for col, dtype in df_clean.dtypes.items()])
                create_table_sql = f"CREATE TABLE `{catalog}`.`{schema}`.`{table_name}` ({cols_with_types})"
                logging.info(f"Creando tabla con la siguiente estructura: {cols_with_types}")
                cursor.execute(create_table_sql)

                # Procesar e insertar datos en lotes para mejorar rendimiento
                batch_size = 500  # Reducido para evitar consultas demasiado grandes
                total_rows = len(df_clean)

                for i in range(0, total_rows, batch_size):
                    df_batch = df_clean.iloc[i:i + batch_size]
                    values_list = []

                    for _, row in df_batch.iterrows():
                        # Procesar cada valor según su tipo
                        processed_values = []
                        for val in row:
                            if pd.isna(val):
                                processed_values.append('NULL')
                            elif isinstance(val, (int, float)):
                                processed_values.append(str(val))
                            elif isinstance(val, pd.Timestamp):
                                processed_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                # Escapar comillas simples en strings
                                val_str = str(val).replace("'", "''")
                                processed_values.append(f"'{val_str}'")

                        values_list.append(f"({', '.join(processed_values)})")

                    # Construir y ejecutar la consulta de inserción
                    if values_list:
                        insert_sql = f"INSERT INTO `{catalog}`.`{schema}`.`{table_name}` VALUES {', '.join(values_list)}"
                        cursor.execute(insert_sql)
                        logging.info(
                            f"Insertados {len(df_batch)} registros (lote {i // batch_size + 1}/{(total_rows - 1) // batch_size + 1})")

                # Verificar que los datos se insertaron correctamente
                cursor.execute(f"SELECT COUNT(*) FROM `{catalog}`.`{schema}`.`{table_name}`")
                count = cursor.fetchone()[0]
                logging.info(f"✅ Éxito: {count} registros cargados en `{catalog}`.`{schema}`.`{table_name}`")

                return count > 0

    except Exception as e:
        logging.error(f"❌ Ocurrió un error durante la carga de datos: {e}", exc_info=True)
        return False


# --- BLOQUE DE PRUEBA ---
if __name__ == "__main__":
    logging.info("--- Ejecutando prueba del conector ---")
    test_catalog = os.getenv("DATABRICKS_CATALOG")
    test_schema = os.getenv("DATABRICKS_SCHEMA")
    test_table = os.getenv("DATABRICKS_TABLE_NAME")

    if not all([test_catalog, test_schema, test_table]):
        logging.error("Para la prueba, las variables DATABRICKS_CATALOG, SCHEMA, y TABLE_NAME deben estar en el .env.")
    else:
        my_dataframe = get_df_from_databricks(catalog=test_catalog, schema=test_schema, table_name=test_table)
        if my_dataframe is not None:
            logging.info("\n--- Probando save_df_to_databricks_fast ---")
            test_save_schema = "default"
            test_save_table = f"test_save_fast_{pd.Timestamp.now().strftime('%Y%m%d_%H%M%S')}"
            save_success = save_df_to_databricks_fast(
                df_to_save=my_dataframe,
                catalog=test_catalog, schema=test_save_schema, table_name=test_save_table
            )
            if save_success:
                logging.info(f"Prueba de guardado rápido exitosa. Revisa la tabla '{test_save_table}'.")
    logging.info("\n--- Prueba del conector finalizada ---")