import os
import logging
import pandas as pd
from dotenv import load_dotenv
from databricks import sql
from pathlib import Path

# --- CONFIGURACIÓN INICIAL ---

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logging.getLogger("databricks.sql").setLevel(logging.WARNING)
logging.getLogger("urllib3").setLevel(logging.WARNING)

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


# --- FUNCIONES DE CONEXIÓN Y LECTURA ---

def get_databricks_connection():
    """
    Establece y devuelve una conexión a Databricks SQL Warehouse.
    Esta es la función central para gestionar la conexión.
    """
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    access_token = os.getenv("DATABRICKS_TOKEN")

    if not all([server_hostname, http_path, access_token]):
        logging.error("Error: Faltan variables de conexión en el .env.")
        return None

    try:
        connection = sql.connect(
            server_hostname=server_hostname,
            http_path=http_path,
            access_token=access_token
        )
        return connection
    except Exception as e:
        logging.error(f"❌ Ocurrió un error al establecer la conexión: {e}")
        return None

def get_df_from_databricks(catalog: str, schema: str, table_name: str) -> pd.DataFrame | None:
    """
    Obtiene una tabla específica de Databricks como un DataFrame de Pandas,
    utilizando la función de conexión centralizada.
    """
    logging.info(f"Intentando obtener la tabla '{catalog}.{schema}.{table_name}' de Databricks...")
    connection = get_databricks_connection()
    if not connection:
        return None

    try:
        with connection:
            logging.info("✅ ¡Conexión exitosa a Databricks!")
            query = f"SELECT * FROM `{catalog}`.`{schema}`.`{table_name}`"
            logging.info(f"Ejecutando consulta: {query}")
            df = pd.read_sql(query, connection)
            logging.info(f"✅ Tabla '{table_name}' cargada exitosamente con {len(df)} filas.")
            return df
    except Exception as e:
        logging.error(f"❌ Ocurrió un error al obtener los datos: {e}")
        return None

# --- BLOQUE DE PRUEBA ---
if __name__ == "__main__":
    logging.info("--- Ejecutando prueba del conector (solo lectura) ---")
    # ... (código de prueba sin cambios)
