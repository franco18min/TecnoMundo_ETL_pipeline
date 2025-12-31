#!/bin/bash
# Script para empaquetar Lambda Silver con dependencias

echo "🔧 Empaquetando Lambda Silver..."

# Limpiar archivos anteriores
rm -rf package
rm -f function.zip

# Crear directorio para dependencias
mkdir package

# Instalar dependencias
echo "📦 Instalando dependencias..."
pip install -r requirements.txt -t package/ --quiet

# Copiar código Lambda
cp lambda_function.py package/

# Empaquetar todo
cd package
zip -r ../function.zip . > /dev/null
cd ..

# Mostrar tamaño
SIZE=$(du -h function.zip | cut -f1)
echo "✅ Lambda Silver empaquetada: function.zip ($SIZE)"
echo "📍 Ubicación: $(pwd)/function.zip"
