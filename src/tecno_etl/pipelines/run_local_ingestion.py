from pathlib import Path
import logging
import os
import sys
from dotenv import load_dotenv

# --- 1. IMPORTACIÓN DE MÓDULOS DEL PROYECTO ---
# Cada módulo tiene una responsabilidad única (Extraer, Transformar, Cargar).
try:
    from src.tecno_etl.extractors.local_file_extractor import read_file
    from src.tecno_etl.transformers.data_normalizer import apply_standard_transformations
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


# --- 2. FUNCIÓN ORQUESTADORA DEL PIPELINE ---

def run_pipeline_for_file(file_path: Path, target_catalog: str, target_schema: str):
    """
    Orquesta el proceso ETL completo para un único archivo local.
    """
    file_name = file_path.name
    logger.info(f"--- Iniciando pipeline para el archivo local: {file_name} ---")

    # --- PASO 1: EXTRACT ---
    # Llama al módulo extractor para leer los datos del archivo.
    df_raw, _ = read_file(file_path)
    if df_raw is None:
        logger.error(f"La extracción falló para {file_name}. Omitiendo archivo.")
        return False

    # --- PASO 2: TRANSFORM ---
    # Llama al módulo transformador para aplicar todas las reglas de limpieza y normalización.
    # Esto es crucial para asegurar que la tabla de dimensiones esté estandarizada.
    df_transformed = apply_standard_transformations(df_raw)

    # --- PASO 3: LOAD ---
    # Se genera un nombre de tabla dinámico basado en el nombre del archivo.
    table_name = Path(file_name).stem.replace('-', '_').replace(' ', '_').lower()

    logger.info(f"Cargando datos en la tabla de Databricks: '{target_catalog}.{target_schema}.{table_name}'")

    # Llama al módulo loader para guardar el DataFrame transformado.
    success = save_df_as_table(
        df_to_save=df_transformed,
        catalog=target_catalog,
        schema=target_schema,
        table_name=table_name
    )
    if success:
        logger.info(f"¡Carga exitosa para el archivo {file_name}!")
    else:
        logger.error(f"La carga falló para el archivo {file_name}.")
    return success


def main():
    """
    Función principal para procesar el archivo de categorías y cargarlo
    como una tabla de dimensiones en Databricks.
    """
    logger.info("--- Iniciando Pipeline de Ingesta de la Tabla de Dimensiones 'Category' ---")

    # --- CONFIGURACIÓN ---
    # Apuntamos directamente al archivo de categorías.
    category_file_path = project_root / 'data' / 'raw' / 'Category.xlsx'

    # El destino es siempre el schema de dimensiones.
    TARGET_CATALOG = "workspace"
    TARGET_SCHEMA = "tecnomundo_data_dimensions"

    try:
        logger.info(f"Procesando el archivo de dimensiones: {category_file_path}")
        if not category_file_path.exists():
            logger.error(f"El archivo de categorías no existe en la ruta esperada: {category_file_path}")
            return 1

        # Ejecutamos el pipeline solo para este archivo.
        success = run_pipeline_for_file(category_file_path, TARGET_CATALOG, TARGET_SCHEMA)

        if success:
            logger.info("--- Pipeline de Ingesta de Dimensiones finalizado exitosamente. ---")
        else:
            logger.error("--- El Pipeline de Ingesta de Dimensiones falló. ---")
            return 1

    except Exception as e:
        logger.error(f"Ocurrió un error inesperado en el pipeline: {e}", exc_info=True)
        return 1

    return 0


if __name__ == "__main__":
    exit(main())
