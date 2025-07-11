import pandas as pd
from pathlib import Path
import logging

# Obtiene un logger para este módulo específico.
logger = logging.getLogger(__name__)


def read_file(file_path: Path) -> tuple[pd.DataFrame | None, str | None]:
    """
    Lee un archivo desde el sistema de archivos local con robustez añadida.
    Esta función se especializa en la extracción de datos y maneja problemas
    comunes como filas en blanco en Excel y problemas de codificación en CSV.

    Args:
        file_path: La ruta al archivo local.

    Returns:
        Una tupla que contiene el DataFrame y el tipo de archivo detectado ('csv' o 'excel').
        Devuelve (None, None) si ocurre un error.
    """
    logger.info(f"Extrayendo datos desde el archivo local: {file_path}")
    try:
        file_type = None
        df = None

        # Determina el tipo de archivo y lo lee de la manera correspondiente.
        if file_path.suffix.lower() == '.csv':
            file_type = 'csv'
            try:
                # Intenta leer con la codificación estándar UTF-8.
                logger.info("Archivo CSV detectado. Intentando leer con codificación UTF-8.")
                df = pd.read_csv(file_path, sep=None, engine='python')
            except UnicodeDecodeError:
                # Si falla, intenta con una codificación común alternativa.
                logger.warning("La lectura con UTF-8 falló. Reintentando con codificación 'latin1'.")
                df = pd.read_csv(file_path, sep=None, engine='python', encoding='latin1')

        elif file_path.suffix.lower() in ['.xlsx', '.xls']:
            file_type = 'excel'
            logger.info("Archivo Excel detectado. Leyendo con openpyxl.")
            df = pd.read_excel(file_path, engine='openpyxl', sheet_name=0)

            # Precaución para Excel: eliminar filas completamente en blanco.
            if df is not None:
                initial_rows = len(df)
                df.dropna(how='all', inplace=True)
                rows_dropped = initial_rows - len(df)
                if rows_dropped > 0:
                    logger.info(f"Se eliminaron {rows_dropped} filas completamente en blanco del archivo Excel.")
        else:
            logger.warning(f"Formato de archivo no soportado: {file_path}. Será omitido.")

        return df, file_type

    except FileNotFoundError:
        logger.error(f"Error de Extracción: No se encontró el archivo en la ruta local: {file_path}")
        return None, None
    except Exception as e:
        logger.error(f"Ocurrió un error al leer el archivo local: {e}")
        return None, None
