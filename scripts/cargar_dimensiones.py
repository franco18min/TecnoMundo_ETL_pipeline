# Script: cargar_dimensiones.py
import os
import boto3
import pandas as pd
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env.aws manualmente
env_path = Path("conf/env/.env.aws")
if env_path.exists():
    print(f"✓ Cargando credenciales desde {env_path}")
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()
    
    # Asegurar que AWS_DEFAULT_REGION esté configurado
    if not os.getenv('AWS_DEFAULT_REGION') and os.getenv('AWS_REGION'):
        os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_REGION')
    
    print(f"   AWS_ACCESS_KEY_ID: {os.getenv('AWS_ACCESS_KEY_ID', 'NO ENCONTRADA')[:10]}...")
    print(f"   AWS_SECRET_ACCESS_KEY: {os.getenv('AWS_SECRET_ACCESS_KEY', 'NO ENCONTRADA')[:5]}... (longitud: {len(os.getenv('AWS_SECRET_ACCESS_KEY', ''))})")
    print(f"   AWS_REGION: {os.getenv('AWS_DEFAULT_REGION', 'NO ENCONTRADA')}")
else:
    print(f"⚠ Archivo {env_path} no encontrado, usando credenciales del sistema")

# Leer archivo
file_path = Path("data/raw/Category.xlsx")
df = pd.read_excel(file_path)

# Eliminar filas vacías
df = df.dropna(subset=['Código Interno', 'Nombre del Artículo', 'Categoría'])

try:
    # Conectar a DynamoDB
    print("Conectando a DynamoDB...")
    dynamodb = boto3.resource('dynamodb', region_name='us-east-1')
    table = dynamodb.Table('tecnomundo_dimensions_products')
    
    # Verificar que la tabla existe
    print(f"Verificando tabla '{table.table_name}'...")
    
    # Cargar cada producto
    loaded_count = 0
    print(f"Cargando {len(df)} productos...")
    
    with table.batch_writer() as batch:
        for idx, row in df.iterrows():
            item = {
                'codigo_producto': str(row['Código Interno']).upper(),
                'nombre_del_producto': str(row['Nombre del Artículo']),
                'categoria': str(row['Categoría'])
            }
            batch.put_item(Item=item)
            loaded_count += 1
            
            # Mostrar progreso cada 100 items
            if loaded_count % 100 == 0:
                print(f"  Cargados {loaded_count} productos...")
    
    print(f"✅ {loaded_count} productos cargados exitosamente en DynamoDB")
    
except Exception as e:
    print(f"❌ Error: {type(e).__name__}")
    print(f"   Mensaje: {str(e)}")
    
    if "NoCredentialsError" in str(type(e).__name__):
        print("\n💡 Solución:")
        print("   1. Verifica que el archivo conf/env/.env.aws contenga:")
        print("      AWS_ACCESS_KEY_ID=tu_access_key")
        print("      AWS_SECRET_ACCESS_KEY=tu_secret_key")
        print("      AWS_DEFAULT_REGION=us-east-1")
        print("   2. O configura las credenciales con: aws configure")
    
    raise
