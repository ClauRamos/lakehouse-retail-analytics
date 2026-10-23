import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

load_dotenv()  # lee JAVA_HOME / HADOOP_HOME del archivo .env si existe


def _preparar_entorno():
    # Usar siempre el Python del venv activo
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)

    # Solo en Windows: Spark necesita winutils.exe dentro de HADOOP_HOME\bin
    hadoop_home = os.getenv("HADOOP_HOME")
    if os.name == "nt" and hadoop_home:
        os.environ["PATH"] = str(Path(hadoop_home) / "bin") + os.pathsep + os.environ["PATH"]


def get_spark_session(app_name="LakehouseRetailAnalytics"):
    _preparar_entorno()

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .master("local[*]")
    )
    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
