import os
import sys
from pyspark.sql import functions as F

RUTA_RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if RUTA_RAIZ not in sys.path:
    sys.path.append(RUTA_RAIZ)

from src.utils.spark_session import get_spark_session


def construir_tablas_gold(df):
    """Recibe el DataFrame de Silver y devuelve un diccionario {nombre_tabla: DataFrame}.

    No lee ni escribe archivos: así se puede probar con datos de ejemplo.
    Regla de negocio: ingresos y unidades se calculan solo con órdenes Completed.
    """
    completadas = df.filter(F.col("status") == "Completed")

    # PREGUNTA 1: ¿Cuáles son los productos más vendidos por región?
    productos_por_region = (
        completadas.groupBy("region", "product_name")
        .agg(F.sum("quantity").alias("unidades_vendidas"))
        .orderBy("region", F.desc("unidades_vendidas"))
    )

    # PREGUNTA 2: ¿Qué categorías generan más ingresos?
    ingresos_por_categoria = (
        completadas.groupBy("category")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("ingresos_totales"),
            F.count("order_id").alias("ordenes_completadas"),
        )
        .orderBy(F.desc("ingresos_totales"))
    )

    # PREGUNTA 3: ¿Cuál es la tasa de devoluciones por canal de venta?
    # Se calcula sobre TODAS las órdenes del canal (incluye canceladas).
    devoluciones_por_canal = (
        df.groupBy("channel")
        .agg(
            F.count("order_id").alias("total_ordenes"),
            F.sum(F.when(F.col("status") == "Returned", 1).otherwise(0)).alias("ordenes_devueltas"),
        )
        .withColumn(
            "tasa_devolucion_porcentaje",
            F.round(F.col("ordenes_devueltas") / F.col("total_ordenes") * 100, 2),
        )
        .orderBy(F.desc("tasa_devolucion_porcentaje"))
    )

    # PREGUNTA 4: ¿Cómo evolucionan las ventas mes a mes?
    ventas_mensuales = (
        completadas
        .withColumn("year", F.year("order_date"))
        .withColumn("month", F.month("order_date"))
        .groupBy("year", "month")
        .agg(F.round(F.sum("total_amount"), 2).alias("ingresos_totales"))
        .orderBy("year", "month")
    )

    return {
        "productos_por_region": productos_por_region,
        "ingresos_por_categoria": ingresos_por_categoria,
        "devoluciones_por_canal": devoluciones_por_canal,
        "ventas_mensuales": ventas_mensuales,
    }


def aggregate_gold():
    spark = get_spark_session()
    try:
        silver_path = os.path.join(RUTA_RAIZ, "data", "silver", "sales_silver")
        gold_path = os.path.join(RUTA_RAIZ, "data", "gold")

        print(f"Leyendo datos limpios de Silver: {silver_path}")
        df = spark.read.format("delta").load(silver_path)

        for nombre, tabla in construir_tablas_gold(df).items():
            print(f"Generando tabla Gold: {nombre}")
            (
                tabla.write.format("delta")
                .mode("overwrite")
                .option("overwriteSchema", "true")
                .save(os.path.join(gold_path, nombre))
            )

        print("¡Agregaciones a Gold completadas con éxito!")
    finally:
        spark.stop()


if __name__ == "__main__":
    aggregate_gold()
                              