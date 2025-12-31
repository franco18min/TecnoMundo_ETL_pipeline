"""
Lambda Gold: Enriquecimiento con dimensiones
"""
import json
import logging
from datetime import datetime
import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)

dynamodb = boto3.resource('dynamodb')

# Configuración
SILVER_TABLE = 'tecnomundo_silver_sales'
GOLD_TABLE = 'tecnomundo_gold_sales'
DIMENSIONS_TABLE = 'tecnomundo_dimensions_products'


def lambda_handler(event, context):
    """
    Handler de Lambda Gold.
    Enriquece datos de Silver con información de dimensiones.
    """
    try:
        logger.info("=== Lambda Gold Enrichment Iniciada ===")
        
        for record in event['Records']:
            message = json.loads(record['body'])
            file_id = message['file_id']
            
            logger.info(f"Procesando file_id: {file_id}")
            
            # 1. Leer datos de Silver (filtrar por timestamp reciente)
            silver_table = dynamodb.Table(SILVER_TABLE)
            # Simplificado: escanear últimos registros
            # En producción, usar query con GSI por timestamp
            response = silver_table.scan(Limit=1000)
            silver_items = response['Items']
            
            logger.info(f"Leídos {len(silver_items)} registros de Silver")
            
            # 2. Leer dimensiones
            dim_table = dynamodb.Table(DIMENSIONS_TABLE)
            dimensions = {}
            
            # Cargar todas las dimensiones en memoria (OK para volumen bajo)
            dim_response = dim_table.scan()
            for dim in dim_response['Items']:
                dimensions[dim['codigo_producto']] = dim
            
            logger.info(f"Cargadas {len(dimensions)} dimensiones")
            
            # 3. Enriquecer y escribir a Gold
            gold_table = dynamodb.Table(GOLD_TABLE)
            enriched_count = 0
            not_found_count = 0
            
            with gold_table.batch_writer() as batch:
                for item in silver_items:
                    codigo = item['codigo_producto']
                    
                    # Buscar dimensión
                    dim = dimensions.get(codigo)
                    
                    gold_item = {
                        **item,  # Todos los campos de Silver
                        'nombre_del_producto': dim.get('nombre_del_producto', 'NO_ENCONTRADO') if dim else 'NO_ENCONTRADO',
                        'categoria': dim.get('categoria', 'SIN_CATEGORIA') if dim else 'SIN_CATEGORIA',
                        'enriched_at': datetime.now().isoformat()
                    }
                    
                    batch.put_item(Item=gold_item)
                    
                    if dim:
                        enriched_count += 1
                    else:
                        not_found_count += 1
            
            logger.info(f"✅ Gold completado: {enriched_count} enriquecidos, {not_found_count} sin dimensión")
        
        return {'statusCode': 200}
        
    except Exception as e:
        logger.error(f"❌ Error en Gold Lambda: {str(e)}", exc_info=True)
        raise
