"""
Script para desplegar funciones Lambda a AWS
"""
import os
import boto3
import logging
from pathlib import Path

# Configurar logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cargar credenciales AWS
env_path = Path(__file__).parent.parent / "conf" / "env" / ".env.aws"
if env_path.exists():
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()
    
    if not os.getenv('AWS_DEFAULT_REGION') and os.getenv('AWS_REGION'):
        os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_REGION')

# Cliente Lambda
lambda_client = boto3.client('lambda', region_name=os.getenv('AWS_DEFAULT_REGION', 'us-east-1'))

def deploy_lambda(function_name: str, zip_path: Path):
    """
    Despliega una función Lambda a AWS
    """
    logger.info(f"📦 Desplegando {function_name}...")
    logger.info(f"   Archivo: {zip_path}")
    logger.info(f"   Tamaño: {zip_path.stat().st_size / 1024 / 1024:.2f} MB")
    
    try:
        # Leer el archivo ZIP
        with open(zip_path, 'rb') as f:
            zip_content = f.read()
        
        # Actualizar código de la función
        response = lambda_client.update_function_code(
            FunctionName=function_name,
            ZipFile=zip_content,
            Publish=True
        )
        
        logger.info(f"✅ {function_name} desplegada exitosamente")
        logger.info(f"   Versión: {response['Version']}")
        logger.info(f"   Runtime: {response['Runtime']}")
        logger.info(f"   Tamaño del código: {response['CodeSize']} bytes")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Error desplegando {function_name}: {e}")
        return False


def main():
    """Desplegar todas las funciones Lambda"""
    project_root = Path(__file__).parent.parent
    lambda_dir = project_root / "lambda_functions"
    
    # Funciones a desplegar
    functions = [
        {
            'name': 'tecnomundo-bronze-ingestion',
            'zip': lambda_dir / 'bronze_ingestion' / 'function.zip'
        },
        {
            'name': 'tecnomundo-silver-transformation',
            'zip': lambda_dir / 'silver_transformation' / 'function.zip'
        },
        {
            'name': 'tecnomundo-gold-enrichment',
            'zip': lambda_dir / 'gold_enrichment' / 'function.zip'
        }
    ]
    
    logger.info("🚀 Iniciando deployment de funciones Lambda...")
    logger.info(f"   Región: {os.getenv('AWS_DEFAULT_REGION', 'us-east-1')}")
    logger.info("")
    
    success_count = 0
    failed_count = 0
    
    for func in functions:
        if not func['zip'].exists():
            logger.warning(f"⚠️  {func['name']}: archivo ZIP no encontrado en {func['zip']}")
            logger.info(f"   Ejecuta el script package.sh en {func['zip'].parent}")
            failed_count += 1
            continue
        
        if deploy_lambda(func['name'], func['zip']):
            success_count += 1
        else:
            failed_count += 1
        
        logger.info("")
    
    # Resumen
    logger.info("=" * 60)
    logger.info(f"✅ Funciones desplegadas exitosamente: {success_count}")
    if failed_count > 0:
        logger.info(f"❌ Funciones con errores: {failed_count}")
    logger.info("=" * 60)
    
    return 0 if failed_count == 0 else 1


if __name__ == "__main__":
    exit(main())
