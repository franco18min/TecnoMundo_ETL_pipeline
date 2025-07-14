# Este código se ejecuta en una celda de un Notebook de Databricks
import re
from pathlib import Path

# 1. --- Configuración de Rutas ---
# La ruta al archivo crudo que acabas de subir a tu "Volume"
source_file_path = "/Volumes/workspace/tecnomundo_data_raw/uploads_raw/Reporte de ventas por articulos-2.csv"
output_schema = "workspace.tecnomundo_data_raw"

# 2. --- Generación Dinámica del Nombre de la Tabla ---
# Se toma el nombre del archivo de la ruta de origen.
file_name = Path(source_file_path).name

# Se limpia el nombre para que sea un nombre de tabla SQL válido.
table_base_name = Path(source_file_path).stem
sanitized_name = re.sub(r'[\s\.\-]+', '_', table_base_name).lower()
sanitized_name = re.sub(r'[^a-zA-Z0-9_]', '', sanitized_name)

# Se construye el nombre final de la tabla añadiendo el sufijo '_raw'.
output_table_name = f"{output_schema}.{sanitized_name}_raw"

print(f"El archivo de origen es: {file_name}")
print(f"El nombre de la tabla de destino será: {output_table_name}")


# 3. --- Lectura del Archivo Crudo con Spark ---
try:
  # --- Limpieza Previa (Paso Clave para la Solución) ---
  # Se elimina la tabla si existe para asegurar que se cree desde cero con las nuevas propiedades.
  print(f"Asegurando que la tabla de destino '{output_table_name}' no exista...")
  spark.sql(f"DROP TABLE IF EXISTS {output_table_name}")
  print("Limpieza previa completada.")

  print(f"\nIniciando la lectura del archivo: {source_file_path}")
  
  df_raw = (spark.read
            .format("csv")
            .option("header", "true")
            .option("inferSchema", "true")
            .load(source_file_path))
  
  print("Lectura del archivo completada exitosamente.")
  
  # 4. --- Guardado como Tabla Delta con Column Mapping ---
  # En lugar de renombrar las columnas en el DataFrame, le decimos a Delta Lake
  # que active el "Column Mapping" al momento de crear la tabla.
  
  print(f"Iniciando el guardado en la tabla '{output_table_name}' con Column Mapping activado...")
  
  (df_raw.write
   .option("delta.columnMapping.mode", "name") # Permite nombres de columna con caracteres especiales.
   .mode("overwrite") # Usamos overwrite para crear la tabla.
   .saveAsTable(output_table_name))
  
  print(f"¡Éxito! Los datos han sido ingestados y guardados en la tabla '{output_table_name}' manteniendo los nombres de columna originales.")
  
  # 5. --- Verificación ---
  # Al consultar la tabla, verás los nombres originales como 'Comprobante Nº'.
  print("Mostrando una muestra de los datos cargados:")
  final_df = spark.table(output_table_name)
  display(final_df)
  
  print("\nEsquema de la tabla final (nombres de columna originales):")
  final_df.printSchema()

except Exception as e:
  print(f"Ocurrió un error durante el proceso de ingesta: {e}")
