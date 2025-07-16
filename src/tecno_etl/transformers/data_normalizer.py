# src/tecno_etl/transformers/data_normalizer.py
import pandas as pd
import re
import logging
import unicodedata

# Configurar un logger para este módulo
logger = logging.getLogger(__name__)


def _remove_accents(input_str: str) -> str:
    """
    Función de utilidad interna para eliminar acentos de una cadena de texto.
    Ej: 'Categoría' -> 'Categoria'
    """
    # Se asegura de que el input sea un string para evitar errores con valores nulos o numéricos.
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
        # Primero, eliminamos acentos del nombre de la columna
        new_col_name = _remove_accents(str(col))
        # Luego, aplicamos las reglas de saneamiento existentes
        new_col_name = new_col_name.replace('º', '').replace('Nº', 'num')
        new_col_name = re.sub(r'[\s\.\-]+', '_', new_col_name)
        new_col_name = re.sub(r'[^a-zA-Z0-9_]', '', new_col_name).lower()
        renamed_cols[col] = new_col_name

    return df.rename(columns=renamed_cols)


def conform_product_key(df: pd.DataFrame) -> pd.DataFrame:
    """
    Busca columnas de clave de producto inconsistentes (ej. 'cdigo', 'cdigo_interno')
    y las estandariza a un único nombre: 'codigo_producto'.
    """
    df_conformed = df.copy()
    key_mappings = {
        "cdigo": "codigo_producto",
        "codigo_interno": "codigo_producto",
        "id": "codigo_producto"
        # Añade aquí otros posibles nombres que puedas encontrar
    }

    for old_key, new_key in key_mappings.items():
        if old_key in df_conformed.columns:
            df_conformed = df_conformed.rename(columns={old_key: new_key})
            logger.info(f"Clave de producto conformada: '{old_key}' -> '{new_key}'")

    return df_conformed


def standardize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza todas las columnas de tipo texto (object) en el DataFrame.
    - Elimina acentos.
    - Elimina espacios en blanco al principio y al final.
    - Convierte el texto a mayúsculas para consistencia.
    """
    df_std = df.copy()
    for col in df_std.select_dtypes(include=['object']).columns:
        # Se asegura de que la columna sea de tipo string para evitar errores con valores nulos
        # y aplica las transformaciones en cadena.
        df_std[col] = df_std[col].astype(str).apply(_remove_accents).str.strip().str.upper()
        logger.info(f"Columna de texto estandarizada (acentos, espacios, mayúsculas): '{col}'")
    return df_std


def conform_product_name_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica reglas de normalización específicas a la columna de nombre de producto.
    """
    df_conformed = df.copy()
    standard_name_col = "nombre_del_producto"

    # Primero, estandarizamos el nombre de la columna que contiene el nombre del artículo/producto.
    if "nombre_del_artculo" in df_conformed.columns:
        df_conformed = df_conformed.rename(columns={"nombre_del_artculo": standard_name_col})
        logger.info(f"Nombre de columna de producto conformado: 'nombre_del_artculo' -> '{standard_name_col}'")

    # Luego, si la columna existe, se pueden aplicar limpiezas al contenido.
    # (Esta es una función placeholder para futuras reglas de negocio)
    if standard_name_col in df_conformed.columns:
        # Ejemplo: podrías añadir lógica para reemplazar "BT" por "BLUETOOTH", etc.
        pass

    return df_conformed


def apply_standard_transformations(df: pd.DataFrame) -> pd.DataFrame:
    """
    Función orquestadora que aplica una secuencia de transformaciones estándar.
    """
    logger.info("Aplicando secuencia de transformaciones estándar...")
    # El orden es importante: primero normalizar nombres de columna, luego el contenido.
    df_transformed = normalize_column_names(df)
    df_transformed = conform_product_key(df_transformed)
    df_transformed = conform_product_name_column(df_transformed)
    df_transformed = standardize_text_columns(df_transformed)
    logger.info("Secuencia de transformaciones estándar completada.")

    return df_transformed
