import pandas as pd
import glob
import os

#Verificar que los archivos se hayan descargado
archivos = glob.glob('data/ecommerce_*.csv')
if not archivos:
    print("❌ No se descargaron los archivos, intentá descargarlos y moverlo a la carpeta data")
    print("   Deberías tener: ecommerce_orders.csv, ecommerce_customers.csv, etc.")
else:
    print(f" Archivos encontrados: {len(archivos)}")
    for i in sorted(archivos):
        print(f" - {os.path.basename(i)}")
        
#Cargar CSV principales
df_orders = pd.read_csv('data/ecommerce_orders.csv')
df_order_items = pd.read_csv('data/ecommerce_order_items.csv')
df_customers = pd.read_csv('data/ecommerce_customers.csv')
df_products = pd.read_csv('data/ecommerce_products.csv')

# Explorar
print(f"\n📈 Resumen:")
print(f'Orders: {len(df_orders)} filas, {len(df_orders.columns)} columnas.')
print(f'Orders items: {len(df_order_items)} filas.')
print(f'Customers: {len(df_customers)} filas.')
print(f'Products: {len(df_products)} filas.')

print("\n🔍 Primeras filas de orders:")
print(df_orders.head())
print("\n📋 Info de orders:")
print(df_orders.info())

#Ver nulos en columnas
print(f"\nValores nulos por columnas:")
print(df_orders.isnull().sum())

# Decisión: ¿eliminar o rellenar?
# Si son pocos (<5%), podemos eliminar
# Si son muchos, mejor rellenar con un valor por defecto

#Eliminamos valores nulos de notas
df_orders_clean = df_orders.dropna(subset= ['notes']).copy()

#Rellenamos valores nulos de promotion_id
df_orders_clean.loc[:, 'promotion_id'] = df_orders_clean['promotion_id'].fillna(0)

#Valores luego de corregir nulos
print(f"Filas antes: {len(df_orders)}, después: {len(df_orders_clean)}")

#Verificar duplicados
duplicados = df_orders_clean.duplicated().sum()
print(f"Valores duplicados encontrados: {duplicados}")


#Verificar duplicados en columnas especificas
duplicados_id= df_orders_clean.duplicated(subset= ['order_id']).sum()
print(f'\n Order_id duplicadas: {duplicados_id}')

# Eliminar duplicados
df_orders_clean = df_orders_clean.drop_duplicates()

# Si hay IDs duplicados, quedarse con el más reciente
df_orders_clean = df_orders_clean.sort_values('order_date').drop_duplicates(
    subset=['order_id'], 
    keep='last')

# Ver tipos actuales
print(df_orders_clean.dtypes)

#Convertir fechas
df_orders_clean['order_date'] = pd.to_datetime(df_orders_clean['order_date'])

#Convertir a float
df_orders_clean['discount_percent'] = df_orders_clean['discount_percent'].astype('float64')
df_orders_clean['shipping_cost'] = df_orders_clean['shipping_cost'].astype('float64')

#Convertimos status
df_orders_clean['status'] = df_orders_clean['status'].astype('category')
df_orders_clean['payment_method'] = df_orders_clean['payment_method'].astype('category')

# Asegurar que los números sean numéricos
df_orders_clean['total_amount'] = pd.to_numeric(df_orders_clean['total_amount'], errors='coerce')

# Verificar
print("\nTipos después de conversión:")
print(df_orders_clean.dtypes)

# PREGUNTA 1: Top 5 clientes por gasto total
# Agrupamos por customer_id y sumamos total_amount
ventas_cliente = df_orders_clean.groupby('customer_id').agg({
  'total_amount': 'sum',
  'order_id': 'count'
}).rename(columns={'total_amount': 'total_gastado', 'order_id': 'cantidad_ordenes'})
ventas_cliente = ventas_cliente.sort_values('total_gastado', ascending=False)
print("🏆 Top 5 clientes:")
print(ventas_cliente.head())

# PREGUNTA 2: Producto más vendido
# Primero unimos orders con order_items para tener quantity
# Agrupamos por product_id y sumamos quantity
productos_vendidos = df_order_items.groupby('product_id')['quantity'].sum().sort_values(ascending=False)
print(f"\n📦 Producto más vendido: ID {productos_vendidos.idxmax()} ({productos_vendidos.max()} unidades)")

# PREGUNTA 3: Evolución mensual de ventas
# Agrupamos por mes y sumamos total_amount
df_orders_clean['mes'] = df_orders_clean['order_date'].dt.to_period('M')
ventas_mes = df_orders_clean.groupby('mes')['total_amount'].sum().reset_index()
ventas_mes.columns = ['mes', 'total_ventas']
print("\n📈 Ventas por mes:")
print(ventas_mes)


#Guardar métricas en CSV
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
