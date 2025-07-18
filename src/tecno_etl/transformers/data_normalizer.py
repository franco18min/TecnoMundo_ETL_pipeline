# src/tecno_etl/transformers/data_normalizer.py
import pandas as pd
import re
import logging
import unicodedata

# Configurar un logger para este módulo
logger = logging.getLogger(__name__)


def _remove_accents(input_str: str) -> str:
    """Función de utilidad interna para eliminar acentos de una cadena de texto."""
    if not isinstance(input_str, str):
        return input_str
    nfkd_form = unicodedata.normalize('NFKD', input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Toma un DataFrame y devuelve uno nuevo con los nombres de columna saneados.
    Ej: 'Comprobante Nº' -> 'comprobante_num'
    """
    renamed_cols = {}
    for col in df.columns:
        new_col_name = _remove_accents(str(col))
        new_col_name = new_col_name.replace('Nº', 'num').replace('º', '')
        new_col_name = re.sub(r'[\s\.\-]+', '_', new_col_name)
        new_col_name = re.sub(r'[^a-zA-Z0-9_]', '', new_col_name).lower()
        renamed_cols[col] = new_col_name

    return df.rename(columns=renamed_cols)


def conform_product_key_name(df: pd.DataFrame) -> pd.DataFrame:
    """
    Busca columnas de clave de producto inconsistentes y las estandariza a 'codigo_producto'.
    """
    df_conformed = df.copy()
    key_mappings = {
        "codigo_interno": "codigo_producto",
        "cdigo": "codigo_producto",
        "codigo": "codigo_producto",
        "id": "codigo_producto"
    }

    for old_key, new_key in key_mappings.items():
        if old_key in df_conformed.columns:
            df_conformed = df_conformed.rename(columns={old_key: new_key})
            logger.info(f"Clave de producto conformada: '{old_key}' -> '{new_key}'")

    return df_conformed


def standardize_product_key_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza el CONTENIDO de la columna 'codigo_producto'.
    - Elimina prefijos de caja (ej. 'A04-').
    - Convierte el código a mayúsculas.
    """
    df_std = df.copy()
    key_col = "codigo_producto"

    if key_col in df_std.columns:
        logger.info(f"Estandarizando formato de la clave '{key_col}'...")
        # Se asegura de que la columna sea de tipo string para las operaciones
        df_std[key_col] = df_std[key_col].astype(str)

        # Expresión regular para encontrar el patrón de prefijo de caja (Letra, 2 dígitos, guion)
        prefix_pattern = r'^[A-Z]\d{2}-'

        # Aplica la transformación: elimina el prefijo y convierte a mayúsculas
        df_std[key_col] = df_std[key_col].str.upper().str.replace(prefix_pattern, '', regex=True)
        logger.info(f"  - Prefijos de caja eliminados y códigos convertidos a mayúsculas.")

    return df_std


def standardize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza todas las columnas de tipo texto (object) en el DataFrame.
    """
    df_std = df.copy()
    for col in df_std.select_dtypes(include=['object']).columns:
        # No volvemos a procesar la clave del producto que ya tiene su propia lógica
        if col != "codigo_producto":
            df_std[col] = df_std[col].astype(str).apply(_remove_accents).str.strip().str.upper()
            logger.info(f"Columna de texto estandarizada (acentos, espacios, mayúsculas): '{col}'")
    return df_std


def conform_product_name_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Busca la columna de nombre de producto y la estandariza a 'nombre_del_producto'.
    """
    df_conformed = df.copy()
    standard_name_col = "nombre_del_producto"
    name_mappings = {
        "nombre_del_articulo": standard_name_col,
        "nombre_del_producto": standard_name_col
    }

    for old_name, new_name in name_mappings.items():
        if old_name in df_conformed.columns:
            df_conformed = df_conformed.rename(columns={old_name: new_name})
            logger.info(f"Nombre de columna de producto conformado: '{old_name}' -> '{new_name}'")

    return df_conformed


def apply_standard_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Función orquestadora que aplica una secuencia de transformaciones estándar.
    """
    logger.info("Aplicando secuencia de transformaciones estándar...")
    # El orden es importante:
    # 1. Normalizar nombres de columna.
    df_transformed = normalize_column_names(df)
    # 2. Conformar el NOMBRE de la columna de la clave.
    df_transformed = conform_product_key_name(df_transformed)
    # 3. Estandarizar el FORMATO de la clave.
    df_transformed = standardize_product_key_format(df_transformed)
    # 4. Conformar el nombre de la columna del nombre del producto.
    df_transformed = conform_product_name_column(df_transformed)
    # 5. Estandarizar el resto de columnas de texto.
    df_transformed = standardize_text_columns(df_transformed)

    logger.info("Secuencia de transformaciones estándar completada.")

    return df_transformed
