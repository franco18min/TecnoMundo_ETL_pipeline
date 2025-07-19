import logging
import os
import sys
import subprocess
from pathlib import Path
from dotenv import load_dotenv

# --- 1. IMPORTACIÓN DE MÓDULOS DEL PROYECTO ---
try:
    from src.tecno_etl.extractors.local_file_extractor import read_file
    from src.tecno_etl.transformers.data_normalizer import apply_standard_transformations
except ImportError as e:
    logging.basicConfig()
    logging.critical(
        f"Error Crítico de Importación: {e}. "
        "Asegúrate de que los módulos existan en las rutas correctas "
        "y que el script se ejecute desde la raíz del proyecto."
    )
    sys.exit(1)

# --- CONFIGURACIÓN INICIAL Y LOGGING ---
try:
    script_path = Path(__file__).resolve()
    project_root = script_path.parent.parent.parent.parent
    dotenv_path = project_root / 'conf' / 'env' / '.env'
    if dotenv_path.exists():
        load_dotenv(dotenv_path=dotenv_path)
        logging.info(f"Archivo .env cargado desde: {dotenv_path}")
    else:
        load_dotenv()
except NameError:
    load_dotenv()

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# --- 2. FUNCIÓN ORQUESTADORA DEL PIPELINE ---

def run_update_pipeline(file_path: Path, databricks_volume_path: str, cli_profile: str):
    """
    Orquesta el proceso de pre-procesamiento y carga para un archivo de actualización.
    """
    file_name = file_path.name
    logger.info(f"=================================================================")
    logger.info(f"--- Iniciando pipeline de actualización para el archivo: {file_name} ---")
    logger.info(f"=================================================================")

    # --- PASO 1: EXTRACT ---
    logger.info("--> Paso 1: EXTRACCIÓN - Leyendo archivo Excel local...")
    df_raw, file_type = read_file(file_path)
    if df_raw is None or file_type != 'excel':
        logger.warning(f"El archivo {file_name} no es un Excel válido o no se pudo leer. Omitiendo.")
        return False
    logger.info(f"    Extracción exitosa. Se encontraron {len(df_raw)} filas.")

    # --- PASO 2: TRANSFORM ---
    logger.info("--> Paso 2: TRANSFORMACIÓN - Aplicando reglas de negocio estándar...")
    df_transformed = apply_standard_transformations(df_raw)
    logger.info("    Transformación completada.")

    # --- PASO 3: PREPARACIÓN PARA LA CARGA ---
    logger.info("--> Paso 3: PREPARACIÓN PARA CARGA - Convirtiendo a formato CSV...")
    temp_csv_path = file_path.with_suffix('.csv')
    try:
        df_transformed.to_csv(temp_csv_path, index=False, encoding='utf-8')
        logger.info(f"    Archivo CSV temporal creado en: {temp_csv_path}")
    except Exception as e:
        logger.error(f"    No se pudo crear el archivo CSV temporal: {e}")
        return False

    # --- PASO 4: LOAD (usando la CLI de Databricks) ---
    logger.info("--> Paso 4: CARGA - Subiendo archivo CSV a Databricks Volume...")
    target_dbfs_path = f"{databricks_volume_path}/{temp_csv_path.name}"
    logger.info(f"    Destino en Databricks: {target_dbfs_path}")

    command = [
        "databricks", "fs", "cp",
        str(temp_csv_path),
        target_dbfs_path,
        "--profile", cli_profile,
        "--overwrite"  # Sobrescribe el archivo en el destino si ya existe
    ]

    try:
        # Ejecuta el comando de la CLI y captura la salida
        logger.info(f"    Ejecutando comando de la CLI...")
        result = subprocess.run(command, check=True, capture_output=True, text=True)
        logger.info("    ¡Carga a Databricks completada exitosamente!")
        logger.debug(f"    Salida de la CLI: {result.stdout}")
    except subprocess.CalledProcessError as e:
        logger.error(f"    Falló la carga a Databricks. Error de la CLI: {e.stderr}")
        return False
    finally:
        # --- PASO 5: LIMPIEZA ---
        logger.info("--> Paso 5: LIMPIEZA - Eliminando archivo CSV temporal...")
        if temp_csv_path.exists():
            os.remove(temp_csv_path)
            logger.info(f"    Archivo CSV temporal eliminado: {temp_csv_path}")

    return True


def main():
    """
    Función principal para escanear la carpeta de actualizaciones y procesar los archivos.
    """
    logger.info("=================================================================")
    logger.info("--- INICIANDO PIPELINE DE ACTUALIZACIÓN DE DIMENSIONES ---")
    logger.info("=================================================================")

    # --- CONFIGURACIÓN ---
    local_updates_path = project_root / 'data' / 'updates' / 'dimensions'
    DATABRICKS_VOLUME_PATH = "dbfs:/Volumes/workspace/tecnomundo_data_raw/uploads_dimensions"
    # El nombre del perfil que configuraste con 'databricks auth login'
    CLI_PROFILE = "dbc-93d9f9d5-b501"

    try:
        logger.info(f"Buscando archivos de actualización en: {local_updates_path}")
        if not local_updates_path.exists():
            logger.error(f"La carpeta de actualizaciones no existe: {local_updates_path}")
            return 1

        # Buscamos solo archivos Excel (.xlsx)
        files_to_process = list(local_updates_path.glob('*.xlsx'))

        if not files_to_process:
            logger.warning("No se encontraron archivos Excel de actualización (.xlsx) en la carpeta. Finalizando.")
            return 0

        logger.info(f"Se encontraron {len(files_to_process)} archivo(s) para procesar.")
        processed_count = 0
        for file_path in files_to_process:
            if run_update_pipeline(file_path, DATABRICKS_VOLUME_PATH, CLI_PROFILE):
                processed_count += 1

        logger.info("=================================================================")
        logger.info(
            f"--- PIPELINE FINALIZADO. Se procesaron {processed_count} de {len(files_to_process)} archivos. ---")
        logger.info("=================================================================")

    except Exception as e:
        logger.error(f"Ocurrió un error inesperado en el pipeline: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
