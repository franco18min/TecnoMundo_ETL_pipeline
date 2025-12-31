"""
Script para cargar datos de dimensiones (productos) en DynamoDB
"""
import boto3
import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno
env_path = Path(__file__).parent.parent.parent / "conf" / "env" / ".env.aws"
load_dotenv(dotenv_path=env_path)

# Conectar a DynamoDB
dynamodb = boto3.resource('dynamodb', region_name=os.getenv('AWS_REGION', 'us-east-1'))
table = dynamodb.Table('tecnomundo_dimensions_products')

# Datos de prueba - productos de ejemplo
productos = [
    {
        'codigo_producto': 'PROD001',
        'nombre_del_producto': 'Laptop HP Pavilion 15',
        'categoria': 'Computadoras'
    },
    {
        'codigo_producto': 'PROD002',
        'nombre_del_producto': 'Mouse Logitech M185',
        'categoria': 'Accesorios'
    },
    {
        'codigo_producto': 'PROD003',
        'nombre_del_producto': 'Teclado Mecánico RGB',
        'categoria': 'Accesorios'
    },
    {
        'codigo_producto': 'PROD004',
        'nombre_del_producto': 'Monitor Samsung 24"',
        'categoria': 'Monitores'
    },
    {
        'codigo_producto': 'PROD005',
        'nombre_del_producto': 'Auriculares Sony WH-1000XM4',
        'categoria': 'Audio'
    }
]

print("🔄 Cargando productos a DynamoDB...")

# Cargar productos
with table.batch_writer() as batch:
    for producto in productos:
        batch.put_item(Item=producto)
        print(f"  ✅ {producto['codigo_producto']}: {producto['nombre_del_producto']}")

print(f"\n✅ {len(productos)} productos cargados en 'tecnomundo_dimensions_products'")
print("\n📋 Puedes verificar en: https://console.aws.amazon.com/dynamodb/")
print("   Tabla: tecnomundo_dimensions_products → Explore table items")
