"""
Lambda Bronze: Ingesta de archivos CSV/Excel a DynamoDB Bronze
"""
import json
import base64
import logging
from datetime import datetime
from io import BytesIO
import pandas as pd
import boto3
from botocore.exceptions import ClientError

# Configurar logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Clientes AWS
dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

# Configuración
BRONZE_TABLE = 'tecnomundo_bronze_sales'
SILVER_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/476277674914/tecnomundo-silver-queue'


def sanitize_column_name(col_name: str) -> str:
    """Limpia nombres de columnas para DynamoDB"""
    import re
    import unicodedata
    
    # Reemplazar caracteres especiales
    col_name = col_name.replace('Nº', 'num').replace('º', '')
    
    # Eliminar acentos
    nfkd = unicodedata.normalize('NFKD', col_name)
    no_accents = ''.join([c for c in nfkd if not unicodedata.combining(c)])
    
    # Limpiar
    sanitized = re.sub(r'[\s\.\-]+', '_', no_accents)
    sanitized = re.sub(r'[^a-zA-Z0-9_]', '', sanitized).lower()
    
    return sanitized


def lambda_handler(event, context):
    """
    Handler principal de Lambda Bronze.
    
    Evento esperado:
    {
        "file_content": "base64_encoded_csv_or_excel",
        "file_name": "ventas.csv",
        "file_type": "csv"  # o "excel"
    }
    """
    try:
        logger.info("=== Lambda Bronze Ingestion Iniciada ===")
        
        # 1. Extraer datos del evento
        file_content_b64 = event['file_content']
        file_name = event['file_name']
        file_type = event.get('file_type', 'csv')
        
        # 2. Decodificar archivo
        file_bytes = base64.b64decode(file_content_b64)
        logger.info(f"Archivo decodificado: {file_name} ({len(file_bytes)} bytes)")
        
        # 3. Leer con pandas
        if file_type == 'csv':
            df = pd.read_csv(BytesIO(file_bytes))
        else:  # excel
            df = pd.read_excel(BytesIO(file_bytes), engine='openpyxl')
        
        logger.info(f"DataFrame cargado: {len(df)} filas, {len(df.columns)} columnas")
        
        # 4. Sanitizar nombres de columnas
        df.columns = [sanitize_column_name(col) for col in df.columns]
        logger.info(f"Columnas sanitizadas: {list(df.columns)}")
        
        # 5. Generar file_id único
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        file_id = f"{file_name.split('.')[0]}_{timestamp}"
        
        # 6. Escribir a DynamoDB Bronze
        table = dynamodb.Table(BRONZE_TABLE)
        
        with table.batch_writer() as batch:
            for idx, row in df.iterrows():
                item = {
                    'file_id': file_id,
                    'row_id': f"row_{idx:05d}",
                    'loaded_at': datetime.now().isoformat(),
                    **row.to_dict()
                }
                
                # Convertir NaN a None
                item = {k: (None if pd.isna(v) else v) for k, v in item.items()}
                
                batch.put_item(Item=item)
        
        logger.info(f"✅ {len(df)} registros escritos en {BRONZE_TABLE}")
        
        # 7. Enviar mensaje a SQS Silver Queue
        sqs_message = {
            'file_id': file_id,
            'row_count': len(df),
            'timestamp': datetime.now().isoformat()
        }
        
        sqs.send_message(
            QueueUrl=SILVER_QUEUE_URL,
            MessageBody=json.dumps(sqs_message)
        )
        
        logger.info(f"✅ Mensaje enviado a SQS Silver Queue")
        
        # 8. Retornar resultado
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Bronze ingestion completada',
                'file_id': file_id,
                'rows_processed': len(df)
            })
        }
        
    except Exception as e:
        logger.error(f"❌ Error en Bronze Lambda: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
