
readme_content = """ 
# Mi Primer ETL con Python

## Descripción
Pipeline ETL que procesa datos de e-commerce para generar métricas de ventas.

## Cómo correr
```bash
pip install pandas pyarrow
python etl.py
```

## Decisiones de limpieza
- **Nulos**: Eliminé filas sin customer_id, product_id (campos críticos)
- **Duplicados**: Eliminé duplicados por order_id, quedándome con el más reciente
- **Tipos**: Convertí order_date a datetime, total_amount a numerico, shipping_cost, discount_percent a float

## Output
- `ventas_por_cliente.csv`: Total gastado y cantidad de órdenes por cliente
- `ventas_por_mes.csv`: Ventas totales por mes
- `orders_clean.parquet`: Dataset limpio en formato optimizado

## Autor
[Matias Emiliano Cuadros] - [16/12/2025]
"""

with open('README.md', 'w') as f:
    f.write(readme_content)

print("✅ README.md creado")