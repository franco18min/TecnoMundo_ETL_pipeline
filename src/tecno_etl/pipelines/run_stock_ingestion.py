import logging
import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd

# --- 1. IMPORTACIÓN DE MÓDULOS DEL PROYECTO ---
try:
    from src.tecno_etl.extractors.local_file_extractor import read_file
except ImportError as e:
    logging.basicConfig()
    logging.critical(f"Error Crítico de Importación: {e}.")
    sys.exit(1)

# --- CONFIGURACIÓN INICIAL Y LOGGING ---
try:
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent.parent.parent
    dotenv_path = project_root / 'conf' / 'env' / '.env'
    load_dotenv(dotenv_path=dotenv_path)
except (NameError, FileNotFoundError):
    load_dotenv()

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


# --- 2. FUNCIÓN ORQUESTADORA DEL PIPELINE ---

def run_stock_upload_pipeline(file_path: Path, databricks_volume_path: str, cli_profile: str):
    logger.info(f"--- Iniciando pipeline de carga para: {file_path.name} ---")

    # --- PASO 1: EXTRACT ---
    logger.info("--> Paso 1: EXTRACCIÓN - Leyendo archivo Excel...")
    df_raw, file_type = read_file(file_path)
    if df_raw is None:
        logger.error(f"No se pudo leer el archivo {file_path.name}.")
        return False
    logger.info(f"    Extracción exitosa. {len(df_raw)} filas y {len(df_raw.columns)} columnas encontradas.")

    # --- PASO 2: TRANSFORMACIÓN MÍNIMA ---
    logger.info("--> Paso 2: TRANSFORMACIÓN - Estandarizando nombres de columnas...")

    # Imprimir columnas detectadas para diagnóstico
    logger.info(f"    Columnas detectadas: {df_raw.columns.tolist()}")

    # Mapeo más específico para nombres de columnas con tildes y espacios
    column_rename_map = {}
    for col in df_raw.columns:
        col_str = str(col).lower()

        # Mapeo para código producto
        if "código interno" in col_str or "codigo interno" in col_str or "código" in col_str or "codigo" in col_str:
            column_rename_map[col] = "codigo_producto"
            logger.info(f"    Mapeando '{col}' a 'codigo_producto'")

        # Mapeo para stock
        elif "stock" in col_str:
            column_rename_map[col] = "stock"
            logger.info(f"    Mapeando '{col}' a 'stock'")

    # Renombrar columnas
    df_raw.rename(columns=column_rename_map, inplace=True)
    logger.info(f"    Columnas después de la estandarización: {df_raw.columns.tolist()}")

    # Verificar que las columnas necesarias estén presentes
    if "codigo_producto" not in df_raw.columns or "stock" not in df_raw.columns:
        logger.error(f"No se encontraron las columnas 'codigo_producto' y 'stock' después de la estandarización.")
        logger.error(f"Columnas disponibles: {df_raw.columns.tolist()}")
        return False

    # --- PASO 3: PREPARACIÓN PARA LA CARGA ---
    temp_csv_path = file_path.with_suffix('.csv')
    try:
        # Guardamos el DataFrame completo con las columnas renombradas
        df_raw.to_csv(temp_csv_path, index=False, encoding='utf-8')
        logger.info(f"    Archivo CSV temporal creado en: {temp_csv_path}")
    except Exception as e:
        logger.error(f"    No se pudo crear el archivo CSV temporal: {e}")
        return False

    # --- PASO 4: LOAD ---
    logger.info("--> Paso 4: CARGA - Subiendo archivo a Databricks Volume...")
    target_dbfs_path = f"{databricks_volume_path}/{temp_csv_path.name}"
    command = ["databricks", "fs", "cp", str(temp_csv_path), target_dbfs_path, "--profile", cli_profile, "--overwrite"]

    try:
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info("    ¡Carga a Databricks completada exitosamente!")
    except subprocess.CalledProcessError as e:
        logger.error(f"    Falló la carga a Databricks. Error: {e.stderr}")
        return False
    finally:
        # --- PASO 5: LIMPIEZA ---
        if temp_csv_path.exists():
            os.remove(temp_csv_path)
            logger.info(f"    Archivo CSV temporal eliminado.")

    return True


def main():
    logger.info("--- INICIANDO PIPELINE DE CARGA DE STOCK ---")
    stock_file_path = project_root / 'data' / 'stock' / 'Stock.xlsx'
    DATABRICKS_VOLUME_PATH = "dbfs:/Volumes/workspace/tecnomundo_data_raw/uploads_stock"
    CLI_PROFILE = "dbc-93d9f9d5-b501"

    if not stock_file_path.exists():
        logger.error(f"El archivo de stock no existe: {stock_file_path}")
        return 1

    if run_stock_upload_pipeline(stock_file_path, DATABRICKS_VOLUME_PATH, CLI_PROFILE):
        logger.info("--- PIPELINE FINALIZADO EXITOSAMENTE ---")
    else:
        logger.error("--- PIPELINE FINALIZADO CON ERRORES ---")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())