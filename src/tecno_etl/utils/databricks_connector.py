"""
Módulo para conectar con Databricks SQL Warehouse.
Permite cargar y guardar datos entre DataFrames de pandas y tablas en Databricks.

Compatible con Python 3.13
Autor: Franco (franco18min@github.com)
"""

import os
import pandas as pd
import logging
from urllib.parse import quote_plus
import sqlalchemy
from pathlib import Path
from dotenv import load_dotenv

# Configuración de logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_env():
    """Carga las variables de entorno desde la ruta específica."""
    env_path = Path(__file__).resolve().parent.parent.parent.parent / "conf" / "env" / ".env"
    if env_path.exists():
        load_dotenv(dotenv_path=env_path)
        logger.info(f"Variables de entorno cargadas desde {env_path}")
    else:
        load_dotenv()
        logger.warning(f"No se encontró {env_path}, se cargó .env por defecto")


load_env()


def get_jdbc_url():
    """
    Construye y retorna la URL JDBC para conectar a Databricks.
    """
    server_hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
    http_path = os.getenv("DATABRICKS_HTTP_PATH")
    token = os.getenv("DATABRICKS_TOKEN")

    if not all([server_hostname, http_path, token]):
        raise ValueError(
            "Faltan variables de entorno de Databricks (DATABRICKS_SERVER_HOSTNAME, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH)")

    # Formato de URL JDBC para Databricks
    jdbc_url = f"jdbc:databricks://{server_hostname}:443/default;transportMode=http;ssl=1;httpPath={http_path};AuthMech=3;UID=token;PWD={token}"
    return jdbc_url


def get_db_engine():
    """
    Crea un engine SQLAlchemy para Databricks usando JDBC.
    Usa el conector JDBC que es más compatible con Python 3.13.
    """
    try:
        # Primero intentamos con pyhive que es más compatible con Python 3.13
        try:
            from pyhive import hive
            jdbc_url = get_jdbc_url()

            engine = sqlalchemy.create_engine(
                f"hive://token:{os.getenv('DATABRICKS_TOKEN')}@{os.getenv('DATABRICKS_SERVER_HOSTNAME')}:443/default",
                connect_args={
                    "http_path": os.getenv("DATABRICKS_HTTP_PATH"),
                    "protocol": "https"
                }
            )
            logger.info("Conexión establecida usando PyHive")
            return engine
        except (ImportError, Exception) as e:
            logger.warning(f"No se pudo usar PyHive: {e}")

        # Segundo intento: SQLAlchemy genérico
        jdbc_url = get_jdbc_url()
        connection_uri = f"databricks://token:{os.getenv('DATABRICKS_TOKEN')}@{os.getenv('DATABRICKS_SERVER_HOSTNAME')}:443"

        engine = sqlalchemy.create_engine(
            connection_uri,
            connect_args={"http_path": os.getenv("DATABRICKS_HTTP_PATH")}
        )
        logger.info("Conexión establecida usando SQLAlchemy genérico")
        return engine

    except Exception as e:
        logger.error(f"Error al crear engine SQLAlchemy: {e}")
        # Fallback: conexión directa con requests
        logger.info("Usando método alternativo de conexión basado en API REST")
        return None


def get_df_from_databricks(catalog: str, schema: str, table_name: str, limit: int = None) -> pd.DataFrame:
    """
    Descarga una tabla de Databricks como DataFrame de pandas.
    Usa múltiples métodos de conexión por si uno falla.
    """
    try:
        # Método 1: SQLAlchemy (más rápido)
        engine = get_db_engine()
        if engine:
            limit_clause = f"LIMIT {limit}" if limit else ""
            sql = f"SELECT * FROM {catalog}.{schema}.{table_name} {limit_clause}"
            logger.info(f"Ejecutando consulta: {sql}")
            df = pd.read_sql(sql, engine)
            return df

        # Método 2: API REST (más compatible pero requiere más paquetes)
        import requests
        import json

        hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        token = os.getenv("DATABRICKS_TOKEN")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")

        api_url = f"https://{hostname}/api/2.0/sql/statements"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        limit_clause = f"LIMIT {limit}" if limit else ""
        sql = f"SELECT * FROM {catalog}.{schema}.{table_name} {limit_clause}"

        data = {
            "statement": sql,
            "warehouse_id": http_path.split("/")[-1]
        }

        response = requests.post(api_url, headers=headers, json=data)
        result = response.json()

        if "results" in result:
            columns = [col["name"] for col in result["results"]["schema"]]
            rows = result["results"]["data"]
            df = pd.DataFrame(rows, columns=columns)
            return df
        else:
            logger.error(f"Error en API REST: {result}")
            return None

    except Exception as e:
        logger.error(f"Error al obtener datos de Databricks: {e}")
        return None


def save_df_to_databricks_fast(df_to_save: pd.DataFrame, catalog: str, schema: str, table_name: str,
                               if_exists: str = "replace") -> bool:
    """
    Guarda un DataFrame de pandas como tabla en Databricks SQL Warehouse.
    Usa múltiples métodos para mayor compatibilidad.
    """
    try:
        # Método 1: SQLAlchemy (más rápido si está disponible)
        engine = get_db_engine()
        if engine:
            full_table_name = f"{catalog}.{schema}.{table_name}"
            logger.info(f"Guardando DataFrame en {full_table_name} (if_exists={if_exists})")

            # Primero crear el esquema si no existe
            with engine.connect() as conn:
                conn.execute(sqlalchemy.text(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"))

            # Guardar el DataFrame
            df_to_save.to_sql(
                name=table_name,
                con=engine,
                schema=f"{catalog}.{schema}",
                if_exists=if_exists,
                index=False,
                method="multi"
            )
            logger.info("Guardado exitoso con SQLAlchemy.")
            return True

        # Método 2: Usar API REST (más compatible)
        import requests
        import json
        import tempfile
        import csv

        hostname = os.getenv("DATABRICKS_SERVER_HOSTNAME")
        token = os.getenv("DATABRICKS_TOKEN")
        http_path = os.getenv("DATABRICKS_HTTP_PATH")

        # 1. Crear esquema si no existe
        api_url = f"https://{hostname}/api/2.0/sql/statements"
        headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        }

        # Crear el esquema si no existe
        create_schema_sql = f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}"
        data = {
            "statement": create_schema_sql,
            "warehouse_id": http_path.split("/")[-1]
        }
        response = requests.post(api_url, headers=headers, json=data)

        # 2. Crear o reemplazar la tabla según if_exists
        if if_exists == "replace":
            drop_sql = f"DROP TABLE IF EXISTS {catalog}.{schema}.{table_name}"
            data = {
                "statement": drop_sql,
                "warehouse_id": http_path.split("/")[-1]
            }
            response = requests.post(api_url, headers=headers, json=data)

        # Crear la tabla
        columns_sql = ", ".join([f"`{col}` STRING" for col in df_to_save.columns])
        create_table_sql = f"CREATE TABLE IF NOT EXISTS {catalog}.{schema}.{table_name} ({columns_sql})"
        data = {
            "statement": create_table_sql,
            "warehouse_id": http_path.split("/")[-1]
        }
        response = requests.post(api_url, headers=headers, json=data)

        # 3. Insertar datos por lotes
        batch_size = 1000  # Ajusta según necesidades
        total_rows = len(df_to_save)

        for i in range(0, total_rows, batch_size):
            batch = df_to_save.iloc[i:i + batch_size]

            # Preparar los valores para la inserción
            values = []
            for _, row in batch.iterrows():
                row_values = []
                for val in row:
                    if pd.isna(val):
                        row_values.append("NULL")
                    elif isinstance(val, (int, float)):
                        row_values.append(str(val))
                    else:
                        row_values.append(f"'{str(val).replace("'", "''")}'")
                values.append(f"({', '.join(row_values)})")

            values_str = ", ".join(values)
            insert_sql = f"INSERT INTO {catalog}.{schema}.{table_name} VALUES {values_str}"

            data = {
                "statement": insert_sql,
                "warehouse_id": http_path.split("/")[-1]
            }
            response = requests.post(api_url, headers=headers, json=data)

            if response.status_code != 200:
                logger.error(f"Error al insertar lote: {response.json()}")
                return False

            logger.info(f"Procesados {i + len(batch)}/{total_rows} registros")

        logger.info("Guardado exitoso con API REST.")
        return True

    except Exception as e:
        logger.error(f"Error al guardar DataFrame en Databricks: {e}")
        return False