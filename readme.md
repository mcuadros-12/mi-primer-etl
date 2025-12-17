readme_content = """
# Mi Primer ETL con Python

## Tecnologías
Lenguaje == python 3.14
Libreria == pandas

## Descripción
Este proyecto implementa un Pipeline ETL (Extract, Transform, Load) simple en Python utilizando la librería Pandas. El objetivo es procesar un conjunto de datos de e-commerce
para limpiar los datos transaccionales y generar métricas clave de negocio (ventas por cliente y evolución mensual).

## Cómo correr

```bash
pip install pandas pyarrow
python etl.py
```

## Decisiones de limpieza
- **Nulos**: Rellene notes con '' para mantener un volumen de datos validos, promotion_id fue pasado a int y rellenado con 0 para manejo eficiente y compatibilidad con Parquet.
- **Duplicados**: Eliminé duplicados, conservando el registro más reciente (basado en order_date) para manejar posibles reenvíos o errores de registro.
- **Tipos**: Convertí order_date a datetime y total_amount, discount_percent y shipping_cost a float.

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