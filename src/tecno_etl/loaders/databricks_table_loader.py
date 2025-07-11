import logging
import pandas as pd
import re
from pathlib import Path

# --- 1. IMPORTACIÓN DE LA UTILIDAD DE CONEXIÓN ---
# Se importa la función de conexión centralizada desde el módulo de utilidades.
# Esto asegura que toda la lógica de conexión resida en un único lugar.
try:
    from src.tecno_etl.utils.databricks_connector import get_databricks_connection
except ImportError as e:
    logging.basicConfig()
    logging.critical(f"Error crítico de importación: {e}. Verifique la ruta del conector.")
    # En un caso real, podrías querer que el programa termine si no puede importar un módulo esencial.
    # import sys
    # sys.exit(1)

# --- CONFIGURACIÓN INICIAL ---
# Ya no es necesario cargar el .env aquí, el conector se encarga de eso.
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("databricks.sql").setLevel(logging.WARNING)


# --- FUNCIONES DE UTILIDAD PARA GUARDAR DATOS ---

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


def save_df_as_table(df_to_save: pd.DataFrame, catalog: str, schema: str, table_name: str) -> bool:
    """
    Guarda un DataFrame en Databricks como una nueva tabla, usando una conexión centralizada.

    Args:
        df_to_save: El DataFrame de pandas que se va a guardar.
        catalog: El catálogo de destino en Databricks.
        schema: El schema (base de datos) de destino en Databricks.
        table_name: El nombre de la tabla que se creará/sobrescribirá.

    Returns:
        True si la carga fue exitosa, False en caso contrario.
    """
    # Obtenemos la conexión desde nuestro módulo conector centralizado.
    connection = get_databricks_connection()
    if not connection:
        logging.error("No se pudo obtener la conexión desde databricks_connector. Abortando carga.")
        return False

    df_clean = df_to_save.copy()
    df_clean.columns = [sanitize_column_name(col) for col in df_clean.columns]
    logging.info(f"Nombres de columnas limpiados: {list(df_clean.columns)}")

    try:
        with connection:
            with connection.cursor() as cursor:
                logging.info(f"Asegurando que el esquema '{schema}' exista...")
                cursor.execute(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")

                logging.info(f"Eliminando tabla anterior si existe: `{catalog}`.`{schema}`.`{table_name}`")
                cursor.execute(f"DROP TABLE IF EXISTS `{catalog}`.`{schema}`.`{table_name}`")

                cols_with_types = ", ".join(
                    [f"`{col}` {pandas_dtype_to_sql(dtype)}" for col, dtype in df_clean.dtypes.items()])
                create_table_sql = f"CREATE TABLE `{catalog}`.`{schema}`.`{table_name}` ({cols_with_types})"
                logging.info(f"Creando tabla con la siguiente estructura: {cols_with_types}")
                cursor.execute(create_table_sql)

                batch_size = 500
                total_rows = len(df_clean)

                for i in range(0, total_rows, batch_size):
                    df_batch = df_clean.iloc[i:i + batch_size]
                    values_list = []
                    for _, row in df_batch.iterrows():
                        processed_values = []
                        for val in row:
                            if pd.isna(val):
                                processed_values.append('NULL')
                            elif isinstance(val, (int, float)):
                                processed_values.append(str(val))
                            elif isinstance(val, pd.Timestamp):
                                processed_values.append(f"'{val.strftime('%Y-%m-%d %H:%M:%S')}'")
                            else:
                                val_str = str(val).replace("'", "''")
                                processed_values.append(f"'{val_str}'")
                        values_list.append(f"({', '.join(processed_values)})")

                    if values_list:
                        insert_sql = f"INSERT INTO `{catalog}`.`{schema}`.`{table_name}` VALUES {', '.join(values_list)}"
                        cursor.execute(insert_sql)
                        logging.info(
                            f"Insertados {len(df_batch)} registros (lote {i // batch_size + 1}/{(total_rows - 1) // batch_size + 1})")

                cursor.execute(f"SELECT COUNT(*) FROM `{catalog}`.`{schema}`.`{table_name}`")
                count = cursor.fetchone()[0]
                logging.info(f"✅ Éxito: {count} registros cargados en `{catalog}`.`{schema}`.`{table_name}`")
                return count > 0

    except Exception as e:
        logging.error(f"❌ Ocurrió un error durante la carga de datos: {e}", exc_info=True)
        return False
