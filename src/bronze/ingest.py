import sys
import os


ruta_actual = os.path.dirname(os.path.abspath(__file__))
ruta_raiz = os.path.abspath(os.path.join(ruta_actual, '../..'))
if ruta_raiz not in sys.path:
    sys.path.append(ruta_raiz)
# --------------------

from src.utils.spark_session import get_spark_session

def ingest_to_bronze():
    spark = get_spark_session()
    raw_path = "data/raw/sales_raw.csv"
    bronze_path = "data/bronze/sales_bronze"

    print(f"Leyendo datos crudos desde: {raw_path}")
    df = spark.read.csv(raw_path, header=True, inferSchema=True, quote='"', escape='"')

    print(f"Escribiendo en capa Bronze: {bronze_path}")
    df.write.format("delta").mode("overwrite").save(bronze_path)

    print("¡Ingesta a Bronze completada con éxito!")
    spark.stop()