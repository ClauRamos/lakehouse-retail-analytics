import os
import sys
from pathlib import Path

import pytest
from dotenv import load_dotenv
from pyspark.sql import SparkSession

# Misma preparación de entorno que src/utils/spark_session.py (Java, Hadoop y Python del venv),
# pero sin Delta: los tests solo prueban transformaciones, no lectura ni escritura de tablas.
load_dotenv(Path(__file__).resolve().parent.parent / ".env")
os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
if os.name == "nt" and os.getenv("HADOOP_HOME"):
    os.environ["PATH"] = str(Path(os.environ["HADOOP_HOME"]) / "bin") + os.pathsep + os.environ["PATH"]


@pytest.fixture(scope="session")
def spark():
    """Una sola sesión de Spark local para todos los tests (arrancar Spark es lo más lento)."""
    session = (
        SparkSession.builder
        .master("local[1]")
        .appName("tests")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()