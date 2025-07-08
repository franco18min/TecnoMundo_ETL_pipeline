import os

def generar_estructura_carpeta(ruta, archivo_salida):
    with open(archivo_salida, 'w', encoding='utf-8') as f:
        for raiz, carpetas, archivos in os.walk(ruta):
            nivel = raiz.replace(ruta, '').count(os.sep)
            sangria = ' ' * 4 * nivel
            f.write(f"{sangria}{os.path.basename(raiz)}/\n")
            sub_sangria = ' ' * 4 * (nivel + 1)
            for archivo in archivos:
                f.write(f"{sub_sangria}{archivo}\n")

ruta_proyecto = r'C:\Users\tc\Desktop\TecnoMundo_ETL_pipeline'
archivo_salida = r'C:\Users\tc\Desktop\TecnoMundo_ETL_pipeline\src\tecno_etl\utils\estructura_proyecto.txt'

generar_estructura_carpeta(ruta_proyecto, archivo_salida)
print(f"Estructura guardada en {archivo_salida}")