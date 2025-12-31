import pytest
import pandas as pd
from src.tecno_etl.transformers.data_normalizer import (
    sanitize_string,
    normalize_column_names,
    conform_product_key_name,
    standardize_product_key_format
)

class TestDataNormalizer:

    def test_sanitize_string(self):
        # Test basic cases
        assert sanitize_string("Hola Mundo") == "hola_mundo"
        assert sanitize_string("  Espacios  ") == "_espacios_"
        
        # Test accents
        assert sanitize_string("Camión") == "camion"
        assert sanitize_string("Árbol") == "arbol"
        
        # Test special characters
        assert sanitize_string("Código Nº") == "codigo_num"
        assert sanitize_string("1º Lugar") == "1_lugar"
        assert sanitize_string("A.B.C.") == "a_b_c_"
        
        # Test complex cases
        assert sanitize_string("  Estación de carga: Nivel 2  ") == "_estacion_de_carga_nivel_2_"

    def test_normalize_column_names(self):
        df = pd.DataFrame({
            "Código del Producto": [1],
            "Fecha de Venta": ["2023-01-01"],
            "  Stock  ": [10]
        })
        
        df_clean = normalize_column_names(df)
        
        expected_columns = ["codigo_del_producto", "fecha_de_venta", "_stock_"]
        assert list(df_clean.columns) == expected_columns

    def test_conform_product_key_name(self):
        # Test renaming known keys
        df1 = pd.DataFrame({"codigo_interno": [1]})
        assert "codigo_producto" in conform_product_key_name(df1).columns
        
        df2 = pd.DataFrame({"cdigo": [1]})
        assert "codigo_producto" in conform_product_key_name(df2).columns
        
        # Test no change if key not found
        df3 = pd.DataFrame({"other": [1]})
        assert "other" in conform_product_key_name(df3).columns

    def test_standardize_product_key_format(self):
        df = pd.DataFrame({
            "codigo_producto": ["a04-123", "b01-xyZ", "simple"]
        })
        
        df_std = standardize_product_key_format(df)
        
        expected_values = ["123", "XYZ", "SIMPLE"]
        assert df_std["codigo_producto"].tolist() == expected_values
