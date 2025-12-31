import os
from pathlib import Path
from dotenv import load_dotenv

# Cargar variables de entorno desde .env.aws
env_path = Path("conf/env/.env.aws")
if env_path.exists():
    load_dotenv(env_path)
    print(f"✓ Variables de entorno cargadas desde {env_path}")
else:
    print(f"⚠ Archivo {env_path} no encontrado")

# Mostrar credenciales (parcialmente ocultas por seguridad)
access_key = os.getenv('AWS_ACCESS_KEY_ID', 'NO ENCONTRADA')
secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'NO ENCONTRADA')
region = os.getenv('AWS_REGION', 'NO ENCONTRADA')

print("\n📋 Credenciales detectadas:")
print(f"   AWS_ACCESS_KEY_ID: {access_key[:10]}...{access_key[-4:] if len(access_key) > 14 else ''} (longitud: {len(access_key)})")
print(f"   AWS_SECRET_ACCESS_KEY: {secret_key[:5]}...{secret_key[-4:] if len(secret_key) > 9 else ''} (longitud: {len(secret_key)})")
print(f"   AWS_REGION: {region}")

# Validar longitudes esperadas
print("\n✓ Validación:")
if len(access_key) == 20:
    print("   ✓ AWS_ACCESS_KEY_ID tiene la longitud correcta (20 caracteres)")
else:
    print(f"   ❌ AWS_ACCESS_KEY_ID debería tener 20 caracteres, tiene {len(access_key)}")

if len(secret_key) == 40:
    print("   ✓ AWS_SECRET_ACCESS_KEY tiene la longitud correcta (40 caracteres)")
else:
    print(f"   ❌ AWS_SECRET_ACCESS_KEY debería tener 40 caracteres, tiene {len(secret_key)}")

# Intentar conexión básica
print("\n🔌 Probando conexión a AWS...")
try:
    import boto3
    sts = boto3.client('sts', region_name=region)
    identity = sts.get_caller_identity()
    print(f"   ✓ Conexión exitosa!")
    print(f"   Account: {identity['Account']}")
    print(f"   UserId: {identity['UserId']}")
    print(f"   Arn: {identity['Arn']}")
except Exception as e:
    print(f"   ❌ Error de conexión: {type(e).__name__}")
    print(f"   Mensaje: {str(e)}")
