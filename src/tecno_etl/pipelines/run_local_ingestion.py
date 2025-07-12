from pathlib import Path
import logging
import os
import re
import sys
from dotenv import load_dotenv

# --- 1. IMPORTACIÓN DE MÓDULOS DEL PROYECTO ---
try:
    # Importa la función especializada del módulo extractor
    from src.tecno_etl.extractors.local_file_extractor import read_file
    # Importa la función especializada del módulo loader
    from src.tecno_etl.loaders.databricks_table_loader import save_df_as_table
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


# --- 2. FUNCIONES DE TRANSFORMACIÓN Y ORQUESTACIÓN ---

def sanitize_for_table_name(file_name: str) -> str:
    """Limpia un nombre de archivo para usarlo como nombre de tabla SQL."""
    name_without_ext = Path(file_name).stem
    # Reutilizamos la lógica de saneamiento que podría estar en el loader,
    # pero es bueno tenerla aquí para la lógica del pipeline.
    # Para este ejemplo, se asume una función de saneamiento simple.
    temp_name = re.sub(r'[\s\.\-/\\]+', '_', name_without_ext)
    return re.sub(r'[^a-zA-Z0-9_]', '', temp_name).lower()


def run_pipeline_for_file(file_path: Path, target_catalog: str, target_schema: str):
    """Orquesta el proceso ETL para un único archivo local."""
    file_name = file_path.name
    logger.info(f"--- Iniciando pipeline para el archivo local: {file_name} ---")

    # 1. EXTRACT (Usa el extractor importado)
    df, file_type = read_file(file_path)
    if df is None:
        logger.error(f"La extracción falló para {file_name}. Omitiendo archivo.")
        return False

    # 2. TRANSFORM (Lógica de limpieza mínima)
    logger.info("Iniciando transformaciones de datos...")
    # La limpieza de filas vacías ya se hace en el extractor para archivos Excel.
    # Aquí solo nos aseguramos de que los nombres de las columnas sean válidos.
    # La limpieza de columnas se hace en el loader, pero podríamos hacer más aquí si fuera necesario.
    logger.info(f"Transformación completada. {len(df)} filas listas para cargar.")

    # 3. LOAD (Usa el loader importado)
    target_table = sanitize_for_table_name(file_name)
    logger.info(f"Cargando datos en la tabla de Databricks: '{target_catalog}.{target_schema}.{target_table}'")

    # Llama a la función del loader para guardar el DataFrame
    success = save_df_as_table(
        df_to_save=df, catalog=target_catalog, schema=target_schema, table_name=target_table
    )
    if success:
        logger.info(f"¡Carga exitosa para el archivo {file_name}!")
    else:
        logger.error(f"La carga falló para el archivo {file_name}.")
    return success


def main():
    """Función principal para escanear una carpeta local y procesar los archivos."""
    logger.info("--- Iniciando Pipeline de Ingesta de Archivos Locales ---")

    # --- CONFIGURACIÓN ---
    local_data_path = project_root / 'data' / 'raw'
    TARGET_CATALOG = "workspace"
    TARGET_SCHEMA = "tecnomundo_data_dimensions"

    try:
        logger.info(f"Escaneando el directorio local: {local_data_path}")
        if not local_data_path.exists():
            logger.error(f"El directorio de datos local no existe: {local_data_path}")
            return 1

        files_to_process = [f for f in local_data_path.iterdir() if f.is_file()]

        if not files_to_process:
            logger.warning("No se encontraron archivos en el directorio de datos local. Saliendo.")
            return 0

        logger.info(f"Se encontraron {len(files_to_process)} archivo(s) para procesar.")
        processed_count = 0
        for file_path in files_to_process:
            if run_pipeline_for_file(file_path, TARGET_CATALOG, TARGET_SCHEMA):
                processed_count += 1

        logger.info(
            f"--- Pipeline finalizado. Se procesaron {processed_count} de {len(files_to_process)} archivos. ---")

    except Exception as e:
        logger.error(f"Ocurrió un error inesperado en el pipeline: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
