#!/usr/bin/env python3
"""
Script para optimizar y empaquetar Lambda Bronze
Elimina archivos innecesarios para reducir el tamaño del paquete
"""
import os
import shutil
from pathlib import Path

def clean_package(package_dir: Path):
    """Elimina archivos innecesarios del paquete"""
    print("🧹 Limpiando archivos innecesarios...")
    
    # Patrones de archivos/directorios a eliminar
    patterns_to_remove = [
        '**/__pycache__',
        '**/*.pyc',
        '**/*.pyo',
        '**/*.dist-info',
        '**/*.egg-info',
        '**/tests',
        '**/test',
        '**/.git*',
        # NO eliminar archivos .so - son necesarios para numpy/pandas
    ]
    
    removed_count = 0
    removed_size = 0
    
    for pattern in patterns_to_remove:
        for item in package_dir.glob(pattern):
            try:
                size = sum(f.stat().st_size for f in item.rglob('*') if f.is_file()) if item.is_dir() else item.stat().st_size
                
                if item.is_dir():
                    shutil.rmtree(item)
                else:
                    item.unlink()
                
                removed_count += 1
                removed_size += size
                print(f"  Eliminado: {item.relative_to(package_dir)}")
            except Exception as e:
                print(f"  Error eliminando {item}: {e}")
    
    print(f"✅ Eliminados {removed_count} items ({removed_size / 1024 / 1024:.2f} MB)")

def main():
    project_root = Path(__file__).parent.parent
    bronze_dir = project_root / "lambda_functions" / "bronze_ingestion"
    package_dir = bronze_dir / "package"
    
    if not package_dir.exists():
        print(f"❌ Directorio package no encontrado: {package_dir}")
        return 1
    
    print(f"📦 Optimizando paquete Lambda Bronze...")
    print(f"   Directorio: {package_dir}")
    
    # Calcular tamaño inicial
    initial_size = sum(f.stat().st_size for f in package_dir.rglob('*') if f.is_file())
    print(f"   Tamaño inicial: {initial_size / 1024 / 1024:.2f} MB")
    
    # Limpiar archivos innecesarios
    clean_package(package_dir)
    
    # Calcular tamaño final
    final_size = sum(f.stat().st_size for f in package_dir.rglob('*') if f.is_file())
    print(f"\n📊 Tamaño final: {final_size / 1024 / 1024:.2f} MB")
    print(f"   Reducción: {(initial_size - final_size) / 1024 / 1024:.2f} MB ({(1 - final_size/initial_size)*100:.1f}%)")
    
    if final_size > 50 * 1024 * 1024:
        print(f"\n⚠️  ADVERTENCIA: El paquete aún excede 50MB")
        print(f"   Considera usar deployment vía S3 o reducir más dependencias")
    
    return 0

if __name__ == "__main__":
    exit(main())
