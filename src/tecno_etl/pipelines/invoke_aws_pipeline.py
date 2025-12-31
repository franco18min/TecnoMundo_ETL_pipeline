"""
Script local para invocar el pipeline Lambda en AWS
"""
import base64
import json
import logging
from pathlib import Path
import boto3
import os
from dotenv import load_dotenv

# Cargar variables de entorno manualmente
env_path = Path(__file__).parent.parent.parent.parent / "conf" / "env" / ".env.aws"
if env_path.exists():
    with open(env_path, 'r') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                os.environ[key.strip()] = value.strip()
    
    # Asegurar que AWS_DEFAULT_REGION esté configurado
    if not os.getenv('AWS_DEFAULT_REGION') and os.getenv('AWS_REGION'):
        os.environ['AWS_DEFAULT_REGION'] = os.getenv('AWS_REGION')

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Cliente Lambda
lambda_client = boto3.client('lambda', region_name=os.getenv('AWS_REGION', 'us-east-1'))


def invoke_bronze_lambda(file_path: Path) -> dict:
    """
    Invoca Lambda Bronze con un archivo local.
    """
    logger.info(f"Preparando archivo: {file_path}")
    
    # Leer y codificar archivo
    with open(file_path, 'rb') as f:
        file_bytes = f.read()
    
    file_b64 = base64.b64encode(file_bytes).decode('utf-8')
    
    # Determinar tipo
    file_type = 'excel' if file_path.suffix in ['.xlsx', '.xls'] else 'csv'
    
    # Preparar payload
    payload = {
        'file_content': file_b64,
        'file_name': file_path.name,
        'file_type': file_type
    }
    
    logger.info(f"Invocando Lambda Bronze...")
    logger.info(f"Tamaño del archivo: {len(file_bytes)} bytes")
    
    # Invocar Lambda
    response = lambda_client.invoke(
        FunctionName='tecnomundo-bronze-ingestion',
        InvocationType='RequestResponse',  # Síncrono
        Payload=json.dumps(payload)
    )
    
    # Leer respuesta
    result = json.loads(response['Payload'].read())
    
    logger.info(f"Respuesta Lambda: {result}")
    
    return result


def main():
    """Procesar archivo de ventas"""
    # Ir al root del proyecto (subir 4 niveles desde este archivo)
    project_root = Path(__file__).parent.parent.parent.parent
    
    # Archivo de ventas real
    file_path = project_root / "data" / "raw" / "Reporte de ventas por articulos-2.csv"
    
    logger.info(f"Buscando archivo en: {file_path}")
    
    if not file_path.exists():
        logger.error(f"Archivo no encontrado: {file_path}")
        logger.info("Por favor, coloca un archivo CSV o Excel en data/raw/")
        return 1
    
    result = invoke_bronze_lambda(file_path)
    
    if result.get('statusCode') == 200:
        logger.info("✅ Pipeline iniciado exitosamente")
        logger.info("Los datos fluirán automáticamente: Bronze → Silver → Gold")
        logger.info("Revisa CloudWatch Logs para ver el progreso:")
        logger.info("  - /aws/lambda/tecnomundo-bronze-ingestion")
        logger.info("  - /aws/lambda/tecnomundo-silver-transformation")
        logger.info("  - /aws/lambda/tecnomundo-gold-enrichment")
        return 0
    else:
        logger.error(f"❌ Error: {result}")
        return 1


if __name__ == "__main__":
    exit(main())
