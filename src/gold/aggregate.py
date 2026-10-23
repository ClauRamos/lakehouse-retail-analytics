import os
import sys
from pyspark.sql import functions as F

RUTA_RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if RUTA_RAIZ not in sys.path:
    sys.path.append(RUTA_RAIZ)

from src.utils.spark_session import get_spark_session

def aggregate_gold():
    spark = get_spark_session()
    try:
        silver_path = os.path.join(RUTA_RAIZ, "data", "silver", "sales_silver")
        gold_path = os.path.join(RUTA_RAIZ, "data", "gold")
        
        print(f"Leyendo datos limpios de Silver: {silver_path}")
        df = spark.read.format("delta").load(silver_path)
        
        # ---------------------------------------------------------
        # PREGUNTA 1: ¿Cuáles son los productos más vendidos por región?
        # ---------------------------------------------------------
        print("Generando Tabla 1: Top productos por región...")
        df_p1 = df.groupBy("region", "product_name").agg(
            F.sum("quantity").alias("unidades_vendidas")
        ).orderBy("region", F.desc("unidades_vendidas"))
        
        df_p1.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/productos_por_region")
        
        # ---------------------------------------------------------
        # PREGUNTA 2: ¿Qué categorías generan más ingresos?
        # ---------------------------------------------------------
        print("Generando Tabla 2: Ingresos por categoría...")
        # Filtramos solo las completadas para que los ingresos sean reales
        df_completadas = df.filter(F.col("status") == "Completed")
        
        df_p2 = df_completadas.groupBy("category").agg(
            F.sum("total_amount").alias("ingresos_totales")
        ).orderBy(F.desc("ingresos_totales"))
        
        df_p2.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/ingresos_por_categoria")
        
        # ---------------------------------------------------------
        # PREGUNTA 3: ¿Cuál es la tasa de devoluciones por canal de venta?
        # ---------------------------------------------------------
        print("Generando Tabla 3: Tasa de devoluciones por canal...")
        # Agrupamos por canal y contamos cuántas fueron devueltas vs el total
        df_p3 = df.groupBy("channel").agg(
            F.count("order_id").alias("total_ordenes"),
            F.sum(F.when(F.col("status") == "Returned", 1).otherwise(0)).alias("ordenes_devueltas")
        ).withColumn(
            "tasa_devolucion_porcentaje", 
            F.round((F.col("ordenes_devueltas") / F.col("total_ordenes")) * 100, 2)
        ).orderBy(F.desc("tasa_devolucion_porcentaje"))
        
        df_p3.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/devoluciones_por_canal")
        
        # ---------------------------------------------------------
        # PREGUNTA 4: ¿Cómo evolucionan las ventas mes a mes?
        # ---------------------------------------------------------
        print("Generando Tabla 4: Evolución mensual de ventas...")
        # Extraemos el año y el mes de la fecha
        df_p4 = df_completadas.withColumn("year", F.year("order_date")) \
                              .withColumn("month", F.month("order_date")) \
                              .groupBy("year", "month").agg(
                                  F.sum("total_amount").alias("ingresos_totales")
                              ).orderBy("year", "month")
                              
        df_p4.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(f"{gold_path}/ventas_mensuales")
        
        print("¡Agregaciones a Gold completadas con éxito!")
        
    finally:
        spark.stop()

if __name__ == "__main__":
    aggregate_gold()