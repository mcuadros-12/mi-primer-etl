import pandas as pd
import glob
import os

# Verificar archivos 
archivos = glob.glob('data/ecommerce_*.csv') # Asegurate que la ruta data/ sea correcta en tu PC
if not archivos:
    print("❌ No se encontraron archivos en data/")
else:
    print(f"✅ Archivos encontrados: {len(archivos)}")

# Cargar CSV
df_orders = pd.read_csv('data/ecommerce_orders.csv')
df_order_items = pd.read_csv('data/ecommerce_order_items.csv')
df_customers = pd.read_csv('data/ecommerce_customers.csv')
df_products = pd.read_csv('data/ecommerce_products.csv')

# --- Exploramos dataframe --- 

print(f"\n📈 Resumen:")
print(f'Orders: {len(df_orders)} filas, {len(df_orders.columns)} columnas.')
print(f'Orders items: {len(df_order_items)} filas.')
print(f'Customers: {len(df_customers)} filas.')
print(f'Products: {len(df_products)} filas.')

print("\n🔍 Primeras filas de orders:")
print(df_orders.head())
print("\n📋 Info de orders:")
print(df_orders.info())

# --- LIMPIEZA ---

#Vemos si hay columnas nulas
print(f"\nValores nulos por columnas:")
print(df_orders.isnull().sum())

# Rellenamos con string vacío en lugar de borrar la fila
df_orders['notes'] = df_orders['notes'].fillna('')
df_orders['promotion_id'] = df_orders['promotion_id'].fillna(0)

#Visualizamos sin strings vacios
print("\nLista con valores NaN rellenas")
print(df_orders.head())

#Verificamos tipos de datos
print('\n Tipos de datos:')
print(df_orders.dtypes)

# Conversión de tipos
df_orders['order_date'] = pd.to_datetime(df_orders['order_date'])
df_orders['total_amount'] = pd.to_numeric(df_orders['total_amount'], errors='coerce')
df_orders['discount_percent'] = pd.to_numeric(df_orders['discount_percent'], errors='coerce')

#Luego de la corrección de datos
print("\n Tipos de datos después de correcciones: ")
print(df_orders.dtypes)

#Verificamos valores duplicados 
duplicados_id= df_orders.duplicated(subset= ['order_id']).sum()
print(f'\n Order_id duplicadas: {duplicados_id}')

# Eliminar duplicados reales
df_orders_clean = df_orders.drop_duplicates()
df_orders_clean = df_orders_clean.sort_values('order_date').drop_duplicates(subset=['order_id'], keep='last')


# --- LÓGICA DE NEGOCIO ---

# 2. CORRECCIÓN: Filtrar órdenes canceladas para el análisis de ventas
# Creamos un DF auxiliar solo con ventas efectivas
df_ventas_validas = df_orders_clean[df_orders_clean['status'] != 'cancelado'].copy()

# PREGUNTA 1: Top 5 clientes (Usando ventas válidas)
ventas_cliente = df_ventas_validas.groupby('customer_id').agg({
    'total_amount': 'sum',
    'order_id': 'count'
}).rename(columns={'total_amount': 'total_gastado', 'order_id': 'cantidad_ordenes'})

# Reseteamos el índice para que customer_id sea una columna al guardar
ventas_cliente = ventas_cliente.reset_index().sort_values('total_gastado', ascending=False)

print("\n🏆 Top 5 clientes:")
print(ventas_cliente.head())

# PREGUNTA 2: Producto más vendido
# Filtramos los items para que coincidan solo con órdenes válidas
items_validos = df_order_items[df_order_items['order_id'].isin(df_ventas_validas['order_id'])]

productos_vendidos = items_validos.groupby('product_id')['quantity'].sum().reset_index()
productos_vendidos = productos_vendidos.sort_values('quantity', ascending=False)
print(f"\n📦 Producto más vendido ID: {productos_vendidos.iloc[0]['product_id']}")

# PREGUNTA 3: Evolución mensual de ventas

df_orders_clean['mes'] = df_orders_clean['order_date'].dt.to_period('M')
ventas_mes = df_orders_clean.groupby('mes')['total_amount'].sum().reset_index()
ventas_mes.columns = ['mes', 'total_ventas']
print("\n📈 Ventas por mes:")
print(ventas_mes)

# --- GUARDADO ---

# Guardar métricas en CSV
ventas_cliente.to_csv('output/ventas_por_cliente.csv', index=False)
ventas_mes.to_csv('output/ventas_por_mes.csv', index=False)

# Guardar datos limpios
df_orders_clean.to_csv('output/orders_clean.csv', index=False)

print("✅ Archivos CSV guardados en output/")

# Guardar en Parquet
df_orders_clean.to_parquet('output/orders_clean.parquet', index=False)

# Comparar tamaños
csv_size = os.path.getsize('output/orders_clean.csv') / 1024
parquet_size = os.path.getsize('output/orders_clean.parquet') / 1024

print(f"Tamaño CSV: {csv_size:.1f} KB")
print(f"Tamaño Parquet: {parquet_size:.1f} KB")
print(f"Parquet es {csv_size/parquet_size:.1f}x más chico")