import os
import boto3
from pathlib import Path

# Cargar credenciales
env_path = Path("conf/env/.env.aws")
with open(env_path, 'r') as f:
    for line in f:
        line = line.strip()
        if line and not line.startswith('#') and '=' in line:
            key, value = line.split('=', 1)
            os.environ[key.strip()] = value.strip()

# Conectar a DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
table = dynamodb.Table('tecnomundo_dimensions_products')

# Obtener información de la tabla
try:
    table_info = table.table_status
    print(f"✓ Tabla: {table.table_name}")
    print(f"✓ Estado: {table_info}")
    
    # Escanear algunos items
    response = table.scan(Limit=5)
    print(f"\n📊 Total items escaneados: {response['Count']}")
    print("\n🔍 Primeros 5 productos:")
    for item in response['Items']:
        print(f"  • {item['codigo_producto']}: {item['nombre_del_producto']}")
        print(f"    Categoría: {item['categoria']}")
        print()
    
    # Obtener conteo aproximado
    print(f"📈 Items totales en la tabla (aproximado): {table.item_count}")
    
except Exception as e:
    print(f"❌ Error: {e}")
