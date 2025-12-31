"""
Script para consultar datos de la capa Gold en DynamoDB
"""
import os
import boto3
import pandas as pd
from pathlib import Path

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

# Conectar a DynamoDB
dynamodb = boto3.resource('dynamodb', region_name='us-east-1')

# Consultar tabla Gold (resultado final)
gold_table = dynamodb.Table('tecnomundo_gold_sales')

print("🔍 Consultando tabla Gold...")

try:
    response = gold_table.scan()
    
    # Convertir a DataFrame para análisis
    df_gold = pd.DataFrame(response['Items'])
    
    if len(df_gold) == 0:
        print("⚠️  La tabla Gold está vacía")
        print("   Verifica que el pipeline haya completado el procesamiento")
    else:
        print("\n📊 Datos en Gold Layer:")
        print("=" * 100)
        
        # Mostrar columnas disponibles
        print(f"\nColumnas disponibles: {df_gold.columns.tolist()}")
        
        # Mostrar primeras filas con columnas relevantes
        columnas_mostrar = []
        for col in ['fecha', 'codigo_producto', 'nombre_del_producto', 'categoria', 'subtotal']:
            if col in df_gold.columns:
                columnas_mostrar.append(col)
        
        if columnas_mostrar:
            print(f"\nPrimeras 10 filas:")
            print(df_gold[columnas_mostrar].head(10).to_string(index=False))
        
        # Resumen
        print("\n" + "=" * 100)
        print("\n📈 RESUMEN:")
        print(f"✅ Total ventas: {len(df_gold)}")
        
        if 'subtotal' in df_gold.columns:
            # Convertir subtotal a numérico
            df_gold['subtotal_num'] = pd.to_numeric(df_gold['subtotal'], errors='coerce')
            total_facturado = df_gold['subtotal_num'].sum()
            print(f"💰 Total facturado: ${total_facturado:,.2f}")
        
        if 'codigo_producto' in df_gold.columns:
            print(f"📦 Productos únicos: {df_gold['codigo_producto'].nunique()}")
        
        if 'categoria' in df_gold.columns:
            print(f"🏷️  Categorías únicas: {df_gold['categoria'].nunique()}")
        
        # Top 5 productos más vendidos
        if 'nombre_del_producto' in df_gold.columns:
            print("\n🏆 Top 5 productos más vendidos:")
            top_productos = df_gold['nombre_del_producto'].value_counts().head(5)
            for i, (producto, cantidad) in enumerate(top_productos.items(), 1):
                print(f"   {i}. {producto}: {cantidad} ventas")
        
        # Ventas por categoría
        if 'categoria' in df_gold.columns and 'subtotal_num' in df_gold.columns:
            print("\n📊 Ventas por categoría:")
            ventas_categoria = df_gold.groupby('categoria')['subtotal_num'].agg(['sum', 'count'])
            ventas_categoria = ventas_categoria.sort_values('sum', ascending=False)
            for categoria, row in ventas_categoria.head(10).iterrows():
                print(f"   {categoria}: ${row['sum']:,.2f} ({int(row['count'])} ventas)")

except Exception as e:
    print(f"❌ Error al consultar la tabla: {e}")
    print(f"\nDetalles del error: {type(e).__name__}")
    
    # Verificar si la tabla existe
    try:
        table_status = gold_table.table_status
        print(f"\n✓ La tabla existe y está en estado: {table_status}")
    except:
        print("\n❌ La tabla 'tecnomundo_gold_sales' no existe")
        print("   Verifica que el pipeline haya creado la tabla correctamente")
