"""
Lambda Silver: Limpieza y validación de datos
"""
import json
import logging
from datetime import datetime
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')
sqs = boto3.client('sqs')

# Configuración
BRONZE_TABLE = 'tecnomundo_bronze_sales'
SILVER_TABLE = 'tecnomundo_silver_sales'
GOLD_QUEUE_URL = 'https://sqs.us-east-1.amazonaws.com/476277674914/tecnomundo-gold-queue'


def clean_and_validate_row(row: dict) -> dict:
    """
    Limpia y valida un registro individual.
    Aplica las mismas transformaciones que tu notebook Silver.
    """
    cleaned = {}
    
    # Validar y limpiar fecha
    fecha_raw = row.get('fecha')
    try:
        # Intentar parsear fecha
        if isinstance(fecha_raw, str):
            from dateutil import parser
            fecha_parsed = parser.parse(fecha_raw)
            cleaned['fecha'] = fecha_parsed.strftime('%Y-%m-%d')
        else:
            cleaned['fecha'] = '1900-01-01'  # Fecha por defecto si falla
    except:
        cleaned['fecha'] = '1900-01-01'
    
    # Validar y limpiar numéricos
    for col in ['cantidad', 'precio_un_', 'ganancia', 'subtotal']:
        val = row.get(col)
        try:
            cleaned[col] = int(float(val)) if val else 0
        except:
            cleaned[col] = 0
    
    # Limpiar texto
    cleaned['comprobante_num'] = str(row.get('comprobante_num', 'SIN_REGISTRO'))
    
    # Estandarizar código de producto (MAYÚSCULAS, sin prefijos)
    codigo = str(row.get('codigo', ''))
    # Eliminar prefijos tipo "A04-"
    import re
    if re.match(r'^[A-Z]\d{2}-', codigo):
        codigo = codigo.split('-', 1)[1]
    cleaned['codigo_producto'] = codigo.upper()
    
    return cleaned


def lambda_handler(event, context):
    """
    Handler de Lambda Silver.
    Triggered por SQS cuando Bronze completa.
    """
    try:
        logger.info("=== Lambda Silver Transformation Iniciada ===")
        
        # 1. Leer mensaje de SQS
        for record in event['Records']:
            message_body = json.loads(record['body'])
            file_id = message_body['file_id']
            
            logger.info(f"Procesando file_id: {file_id}")
            
            # 2. Leer datos de Bronze
            bronze_table = dynamodb.Table(BRONZE_TABLE)
            response = bronze_table.query(
                KeyConditionExpression='file_id = :fid',
                ExpressionAttributeValues={':fid': file_id}
            )
            
            bronze_items = response['Items']
            logger.info(f"Leídos {len(bronze_items)} registros de Bronze")
            
            # 3. Limpiar y validar cada registro
            silver_table = dynamodb.Table(SILVER_TABLE)
            valid_count = 0
            
            with silver_table.batch_writer() as batch:
                for item in bronze_items:
                    try:
                        cleaned = clean_and_validate_row(item)
                        
                        # Crear sale_id único
                        sale_id = f"{cleaned['comprobante_num']}#{cleaned['codigo_producto']}"
                        
                        silver_item = {
                            'fecha': cleaned['fecha'],
                            'sale_id': sale_id,
                            'comprobante_num': cleaned['comprobante_num'],
                            'codigo_producto': cleaned['codigo_producto'],
                            'cantidad': cleaned['cantidad'],
                            'precio_un_': cleaned['precio_un_'],
                            'ganancia': cleaned['ganancia'],
                            'subtotal': cleaned['subtotal'],
                            'processed_at': datetime.now().isoformat()
                        }
                        
                        batch.put_item(Item=silver_item)
                        valid_count += 1
                        
                    except Exception as e:
                        logger.warning(f"Error procesando fila: {e}")
                        continue
            
            logger.info(f"✅ {valid_count} registros escritos en Silver")
            
            # 4. Enviar mensaje a Gold Queue
            sqs.send_message(
                QueueUrl=GOLD_QUEUE_URL,
                MessageBody=json.dumps({
                    'file_id': file_id,
                    'row_count': valid_count,
                    'timestamp': datetime.now().isoformat()
                })
            )
            
            logger.info("✅ Mensaje enviado a Gold Queue")
        
        return {'statusCode': 200}
        
    except Exception as e:
        logger.error(f"❌ Error en Silver Lambda: {str(e)}", exc_info=True)
        raise
