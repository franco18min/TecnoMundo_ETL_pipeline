"""
Módulo para limpieza y procesamiento de datos directamente desde Databricks.

Este módulo se conecta a Databricks, carga una tabla, aplica una serie de
limpiezas y transformaciones, genera reportes de calidad de datos detallados
y guarda el resultado en una nueva tabla dentro de un esquema de datos procesados.

Actualizado: 2025-07-07
Autor: Franco (franco18min@github.com) y Gemini
"""

import pandas as pd
import numpy as np
import math
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List

# --- Importamos las funciones de utilidad que creamos ---
from src.tecno_etl.utils.databricks_connector import get_df_from_databricks, save_df_to_databricks_fast

# Configuración de logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class DataCleaner:
    """Clase principal para la limpieza y procesamiento de datos desde Databricks"""

    def __init__(self, source_catalog: str, source_schema: str, table_name: str):
        """Inicializar el limpiador de datos."""
        self.source_catalog = source_catalog
        self.source_schema = source_schema
        self.table_name = table_name

        self.df = None
        self.df_original = None
        self.filas_problematicas = []
        self.columnas_problematicas = {}

        # Configuración de directorios para reportes
        self.project_root = Path(__file__).resolve().parent.parent.parent.parent
        self.reports_dir = self.project_root / 'reports'
        self._setup_directories()

    def _setup_directories(self) -> None:
        """Valida y crea el directorio de reportes si no existe."""
        try:
            os.makedirs(self.reports_dir, exist_ok=True)
            logger.info(f"Directorio de reportes asegurado en: {self.reports_dir}")
        except OSError as e:
            logger.error(f"No se pudo crear el directorio de reportes: {e}")
            raise

    def _load_data_from_databricks(self) -> bool:
        """Carga la tabla desde Databricks."""
        logger.info(f"Cargando tabla '{self.table_name}' desde '{self.source_catalog}.{self.source_schema}'...")
        self.df = get_df_from_databricks(
            catalog=self.source_catalog,
            schema=self.source_schema,
            table_name=self.table_name
        )
        if self.df is None:
            logger.error("No se pudieron cargar los datos desde Databricks.")
            return False
        self.df_original = self.df.copy()
        logger.info(f"Tabla cargada con {self.df.shape[0]} filas y {self.df.shape[1]} columnas.")
        return True

    def _es_columna_fecha(self, serie: pd.Series) -> bool:
        """Detecta si una columna contiene fechas."""
        if any(palabra in str(serie.name).lower() for palabra in ['fecha', 'date', 'día', 'dia']): return True
        serie_str = serie.astype(str).dropna()
        if len(serie_str) == 0: return False
        return serie_str.str.match(r'\d{1,4}[/-]\d{1,2}[/-]\d{1,4}', na=False).mean() > 0.3

    def _es_columna_numerica(self, serie: pd.Series) -> bool:
        """Detecta si una columna es numérica."""
        return pd.to_numeric(serie, errors='coerce').notna().mean() > 0.5

    def _analizar_columna_problematica(self, columna: str, mask_problematicos: pd.Series) -> Dict:
        """Analiza en detalle una columna problemática, reincorporando la lógica original."""
        valores_problematicos = self.df_original[mask_problematicos][columna]
        valores_unicos_problematicos = valores_problematicos.value_counts()
        tipo_columna = "texto"
        if self._es_columna_fecha(self.df_original[columna]): tipo_columna = "fecha"
        elif self._es_columna_numerica(self.df_original[columna]): tipo_columna = "numérica"
        return {
            'nombre_columna': columna, 'tipo_detectado': tipo_columna,
            'total_valores_problematicos': len(valores_problematicos),
            'porcentaje_problematicos': (len(valores_problematicos) / len(self.df_original)) * 100,
            'valores_unicos_problematicos': valores_unicos_problematicos.to_dict(),
            'indices_problematicos': valores_problematicos.index.tolist(),
            'muestras_valores_problematicos': valores_problematicos.head(10).tolist(),
            'estadisticas_generales': {
                'total_registros': len(self.df_original),
                'valores_nulos': self.df_original[columna].isna().sum(),
                'valores_vacios': (self.df_original[columna].astype(str) == '').sum(),
                'valores_unicos_totales': self.df_original[columna].nunique()
            }
        }

    def _procesar_columna(self, columna: str):
        """Identifica y registra valores problemáticos para cualquier tipo de columna."""
        mask_problematicos = self.df[columna].isna() | (self.df[columna].astype(str).str.lower().isin(['', 'nan', 'null']))
        if mask_problematicos.any():
            self.filas_problematicas.extend(self.df[mask_problematicos].index.tolist())
            self.columnas_problematicas[columna] = self._analizar_columna_problematica(columna, mask_problematicos)

    def _limpiar_datos(self) -> None:
        """Limpia los datos del DataFrame y registra problemas."""
        if self.df is None: return
        logger.info("Iniciando limpieza de datos y análisis de problemas...")
        for columna in self.df.columns:
            self._procesar_columna(columna)
            if self._es_columna_fecha(self.df[columna]):
                self.df[columna] = pd.to_datetime(self.df[columna], errors='coerce').fillna(pd.Timestamp('1900-01-01'))
            elif self._es_columna_numerica(self.df[columna]):
                self.df[columna] = pd.to_numeric(self.df[columna], errors='coerce').fillna(0).apply(lambda x: math.ceil(x)).astype(int)
            else:
                self.df[columna] = self.df[columna].fillna('Sin registro').astype(str)
                self.df.loc[self.df[columna].str.lower().isin(['nan', '', 'null', 'none']), columna] = 'Sin registro'
        self.filas_problematicas = sorted(list(set(self.filas_problematicas)))
        logger.info(f"Análisis completado. {len(self.columnas_problematicas)} columnas con problemas identificados.")

    def exportar_reportes(self):
        """Genera y exporta los reportes de calidad de datos detallados."""
        if not self.columnas_problematicas:
            logger.info("No se encontraron problemas, no se generarán reportes.")
            return
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        self.exportar_reporte_columnas_problematicas(f"reporte_columnas_{self.table_name}_{timestamp}.txt")
        self.exportar_filas_problematicas_excel(f"filas_problematicas_{self.table_name}_{timestamp}.xlsx")

    def exportar_reporte_columnas_problematicas(self, nombre_archivo: str):
        """Exporta un reporte de texto detallado."""
        ruta_reporte = self.reports_dir / nombre_archivo
        with open(ruta_reporte, 'w', encoding='utf-8') as f:
            f.write(f"REPORTE DE ANÁLISIS DE CALIDAD DE DATOS\n")
            f.write(f"Tabla: {self.source_catalog}.{self.source_schema}.{self.table_name}\n")
            f.write(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write("="*70 + "\n")
            for col, analisis in self.columnas_problematicas.items():
                f.write(f"\nCOLUMNA: {col} (Tipo detectado: {analisis['tipo_detectado']})\n")
                f.write("-" * 50 + "\n")
                f.write(f"  - Problemas encontrados: {analisis['total_valores_problematicos']} ({analisis['porcentaje_problematicos']:.2f}%)\n")
                f.write(f"  - Valores únicos problemáticos (top 10):\n")
                for val, count in list(analisis['valores_unicos_problematicos'].items())[:10]:
                    f.write(f"    - '{val}': {count} veces\n")
        logger.info(f"📄 Reporte de texto detallado exportado: {ruta_reporte}")

    def exportar_filas_problematicas_excel(self, nombre_archivo: str):
        """Exporta un archivo Excel con análisis detallado de filas problemáticas."""
        ruta_excel = self.reports_dir / nombre_archivo
        df_problemas = self.crear_df_todas_las_filas_problematicas()
        if df_problemas.empty:
            logger.warning("No hay filas problemáticas para exportar a Excel.")
            return
        with pd.ExcelWriter(ruta_excel, engine='openpyxl') as writer:
            df_problemas.to_excel(writer, sheet_name='Filas_Con_Problemas', index=False)
        logger.info(f"📊 Reporte Excel con {len(df_problemas)} filas problemáticas exportado: {ruta_excel}")

    def crear_df_todas_las_filas_problematicas(self) -> pd.DataFrame:
        """Crea un DataFrame con todas las filas problemáticas para análisis general."""
        if not self.filas_problematicas: return pd.DataFrame()
        indices_unicos = sorted(list(set(self.filas_problematicas)))
        df_problemas = self.df_original.loc[indices_unicos].copy()
        df_problemas.insert(0, 'Indice_Original', indices_unicos)
        columnas_con_problemas = []
        for idx in indices_unicos:
            problemas_fila = [f"{c}({a['tipo_detectado']})" for c, a in self.columnas_problematicas.items() if idx in a['indices_problematicos']]
            columnas_con_problemas.append("; ".join(problemas_fila))
        df_problemas.insert(1, 'Columnas_Problematicas', columnas_con_problemas)
        return df_problemas

    def _save_data_to_databricks(self, target_schema: str, suffix: str = "_cleaned") -> bool:
        """Guarda el DataFrame limpio en una nueva tabla en Databricks."""
        if self.df is None: return False
        new_table_name = f"{self.table_name}{suffix}"
        logger.info(f"Guardando tabla limpia como '{new_table_name}' en '{self.source_catalog}.{target_schema}'")
        return save_df_to_databricks_fast(
            df_to_save=self.df, catalog=self.source_catalog, schema=target_schema, table_name=new_table_name
        )

    def process_table(self, target_schema: str) -> None:
        """Orquesta el proceso completo: cargar, limpiar, reportar y guardar."""
        if self._load_data_from_databricks():
            self._limpiar_datos()
            self.exportar_reportes()
            self._save_data_to_databricks(target_schema=target_schema)

def main():
    """Función principal para ejecutar el limpiador de datos con Databricks."""
    logger.info("--- Iniciando Pipeline de Limpieza de Datos con Databricks ---")
    CATALOGO_ORIGEN = "workspace"
    SCHEMA_ORIGEN = "tecnomundo_data_raw"
    TABLA_A_LIMPIAR = "reporte_de_ventas_por_articulos_2"
    SCHEMA_DESTINO = "tecnomundo_data_processed"
    try:
        limpiador = DataCleaner(
            source_catalog=CATALOGO_ORIGEN, source_schema=SCHEMA_ORIGEN, table_name=TABLA_A_LIMPIAR
        )
        limpiador.process_table(target_schema=SCHEMA_DESTINO)
        logger.info("--- Pipeline de Limpieza Finalizado ---")
    except Exception as e:
        logger.error(f"Ocurrió un error inesperado en el pipeline: {e}", exc_info=True)
        return 1
    return 0

if __name__ == "__main__":
    exit(main())
