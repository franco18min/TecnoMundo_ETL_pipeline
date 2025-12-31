# src/tecno_etl/transformers/data_normalizer.py
import logging
import re
import unicodedata

import pandas as pd

# Configurar un logger para este módulo
logger = logging.getLogger(__name__)


def _remove_accents(input_str: str) -> str:
    """Función de utilidad interna para eliminar acentos de una cadena de texto."""
    if not isinstance(input_str, str):
        return input_str
    nfkd_form = unicodedata.normalize("NFKD", input_str)
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])


def sanitize_string(input_str: str) -> str:
    """
    Función central para limpiar y estandarizar una cadena de texto.
    Elimina acentos, reemplaza caracteres especiales y convierte a snake_case.
    """
    s = str(input_str)
    # Reemplazar caracteres especiales antes de normalizar
    s = s.replace("Nº", "num").replace("º", "")

    s = _remove_accents(s)
    s = re.sub(r"[\s\.\-]+", "_", s)
    s = re.sub(r"[^a-zA-Z0-9_]", "", s).lower()
    return s


def normalize_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Toma un DataFrame y devuelve uno nuevo con los nombres de columna saneados.
    Ej: 'Comprobante Nº' -> 'comprobante_num'
    """
    renamed_cols = {}
    for col in df.columns:
        renamed_cols[col] = sanitize_string(col)

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
        "id": "codigo_producto",
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
        prefix_pattern = r"^[A-Z]\d{2}-"

        # Aplica la transformación: elimina el prefijo y convierte a mayúsculas
        df_std[key_col] = df_std[key_col].str.upper().str.replace(prefix_pattern, "", regex=True)
        logger.info("  - Prefijos de caja eliminados y códigos convertidos a mayúsculas.")

    return df_std


def standardize_text_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Estandariza todas las columnas de tipo texto (object) en el DataFrame.
    """
    df_std = df.copy()
    for col in df_std.select_dtypes(include=["object"]).columns:
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
        "nombre_del_producto": standard_name_col,
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


def validate_dataframe(
    df: pd.DataFrame,
    record_model: type,
    report_path: str | None = None,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Valida un DataFrame usando un modelo Pydantic y separa registros válidos de inválidos.

    Args:
        df: DataFrame a validar
        record_model: Modelo Pydantic para validación (ej. CategoryRecord, SalesRecord)
        report_path: Ruta opcional para guardar reporte de errores

    Returns:
        Tupla (df_válido, df_errores) donde:
        - df_válido: DataFrame con registros que pasaron la validación
        - df_errores: DataFrame con información de errores de validación

    Example:
        ```python
        from tecno_etl.validators import SalesRecord

        df_clean, df_errors = validate_dataframe(
            df_raw,
            SalesRecord,
            report_path="reports/validation/sales_errors.csv"
        )
        ```
    """
    logger.info(f"Iniciando validación de {len(df)} registros con {record_model.__name__}")

    valid_records = []
    error_records = []

    for idx, row in df.iterrows():
        try:
            # Intentar validar el registro
            validated = record_model(**row.to_dict())
            valid_records.append(row)
        except Exception as e:
            # Capturar errores de validación
            error_info = {
                "row_number": idx + 1,
                "error_message": str(e),
                "raw_data": row.to_dict(),
            }
            error_records.append(error_info)
            logger.debug(f"Error en fila {idx + 1}: {e}")

    # Crear DataFrames de resultados
    df_valid = pd.DataFrame(valid_records) if valid_records else pd.DataFrame()
    df_errors = pd.DataFrame(error_records) if error_records else pd.DataFrame()

    # Logging de resultados
    total = len(df)
    valid_count = len(df_valid)
    error_count = len(df_errors)

    logger.info(
        f"Validación completada: {valid_count}/{total} registros válidos "
        f"({error_count} errores, {(valid_count / total) * 100:.1f}% éxito)"
    )

    # Guardar reporte de errores si se especifica
    if report_path and not df_errors.empty:
        from pathlib import Path

        report_file = Path(report_path)
        report_file.parent.mkdir(parents=True, exist_ok=True)
        df_errors.to_csv(report_file, index=False, encoding="utf-8")
        logger.info(f"Reporte de errores guardado en: {report_file}")

    return df_valid, df_errors
