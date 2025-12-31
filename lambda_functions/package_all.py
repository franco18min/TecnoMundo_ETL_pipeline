"""
Script Python para empaquetar Lambdas en Windows (sin necesidad de bash/zip)
"""
import os
import sys
import shutil
import zipfile
from pathlib import Path
import subprocess


def package_lambda(lambda_dir: Path):
    """Empaqueta una función Lambda con sus dependencias"""
    print(f"\n🔧 Empaquetando {lambda_dir.name}...")
    
    # Limpiar archivos anteriores
    package_dir = lambda_dir / "package"
    zip_file = lambda_dir / "function.zip"
    
    if package_dir.exists():
        shutil.rmtree(package_dir)
    if zip_file.exists():
        zip_file.unlink()
    
    # Crear directorio para dependencias
    package_dir.mkdir()
    
    # Instalar dependencias
    requirements_file = lambda_dir / "requirements.txt"
    if requirements_file.exists():
        print("📦 Instalando dependencias...")
        
        # Usar pip con wheels precompilados (sin compilar)
        result = subprocess.run([
            sys.executable, "-m", "pip", "install",
            "-r", str(requirements_file),
            "-t", str(package_dir),
            "--only-binary=:all:",  # Solo usar wheels precompilados
            "--platform", "manylinux2014_x86_64",  # Plataforma de Lambda
            "--python-version", "311",  # Python 3.11
            "--implementation", "cp",
            "--no-deps"  # Instalar sin dependencias primero
        ], capture_output=True, text=True)
        
        if result.returncode != 0:
            print("⚠️ Algunos paquetes no tienen wheels, instalando con dependencias...")
            # Intentar instalación normal para paquetes sin wheels
            subprocess.run([
                sys.executable, "-m", "pip", "install",
                "-r", str(requirements_file),
                "-t", str(package_dir),
                "--upgrade"
            ], check=True)
    
    # Copiar código Lambda
    lambda_function = lambda_dir / "lambda_function.py"
    shutil.copy(lambda_function, package_dir / "lambda_function.py")
    
    # Crear ZIP
    print("📦 Creando archivo ZIP...")
    with zipfile.ZipFile(zip_file, 'w', zipfile.ZIP_DEFLATED) as zipf:
        for root, dirs, files in os.walk(package_dir):
            for file in files:
                file_path = Path(root) / file
                arcname = file_path.relative_to(package_dir)
                zipf.write(file_path, arcname)
    
    # Mostrar tamaño
    size_mb = zip_file.stat().st_size / (1024 * 1024)
    print(f"✅ {lambda_dir.name} empaquetada: {size_mb:.1f} MB")
    print(f"📍 Ubicación: {zip_file}")
    
    return zip_file


def main():
    """Empaquetar todas las Lambdas"""
    lambda_functions_dir = Path(__file__).parent
    
    lambdas = [
        "bronze_ingestion",
        "silver_transformation",
        "gold_enrichment"
    ]
    
    print("=" * 60)
    print("🚀 Empaquetador de Lambdas para Windows")
    print("=" * 60)
    
    for lambda_name in lambdas:
        lambda_dir = lambda_functions_dir / lambda_name
        if lambda_dir.exists():
            try:
                package_lambda(lambda_dir)
            except Exception as e:
                print(f"❌ Error empaquetando {lambda_name}: {e}")
                continue
    
    print("\n" + "=" * 60)
    print("✅ Empaquetado completado")
    print("=" * 60)
    print("\nArchivos .zip creados:")
    for lambda_name in lambdas:
        zip_path = lambda_functions_dir / lambda_name / "function.zip"
        if zip_path.exists():
            size_mb = zip_path.stat().st_size / (1024 * 1024)
            print(f"  - {lambda_name}/function.zip ({size_mb:.1f} MB)")


if __name__ == "__main__":
    main()
