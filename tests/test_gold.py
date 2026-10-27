from datetime import date

import pytest
from pyspark.sql.types import DateType, DoubleType, IntegerType, StringType, StructField, StructType

from src.gold.aggregate import construir_tablas_gold

ESQUEMA = StructType([
    StructField("order_id", StringType()),
    StructField("order_date", DateType()),
    StructField("region", StringType()),
    StructField("category", StringType()),
    StructField("channel", StringType()),
    StructField("product_name", StringType()),
    StructField("quantity", IntegerType()),
    StructField("total_amount", DoubleType()),
    StructField("status", StringType()),
])


def fila(**cambios):
    """Una orden Completed válida; cada test cambia solo lo que quiere probar."""
    base = dict(
        order_id="ORD-1", order_date=date(2024, 1, 15), region="Norte", category="Electronics",
        channel="Online", product_name="Laptop Pro 15", quantity=1, total_amount=100.0, status="Completed",
    )
    base.update(cambios)
    return base


def tablas(spark, filas):
    datos = [tuple(f[campo.name] for campo in ESQUEMA) for f in filas]
    resultado = construir_tablas_gold(spark.createDataFrame(datos, ESQUEMA))
    return {nombre: [r.asDict() for r in df.collect()] for nombre, df in resultado.items()}


def test_genera_las_cuatro_tablas(spark):
    assert set(tablas(spark, [fila()])) == {
        "productos_por_region", "ingresos_por_categoria", "devoluciones_por_canal", "ventas_mensuales",
    }


def test_ingresos_solo_cuentan_ordenes_completadas(spark):
    t = tablas(spark, [
        fila(order_id="ORD-1", total_amount=100.0, status="Completed"),
        fila(order_id="ORD-2", total_amount=50.0, status="Cancelled"),
        fila(order_id="ORD-3", total_amount=70.0, status="Returned"),
    ])
    assert len(t["ingresos_por_categoria"]) == 1
    categoria = t["ingresos_por_categoria"][0]
    assert categoria["ingresos_totales"] == pytest.approx(100.0)
    assert categoria["ordenes_completadas"] == 1


def test_unidades_vendidas_solo_cuentan_ordenes_completadas(spark):
    t = tablas(spark, [
        fila(order_id="ORD-1", quantity=3, status="Completed"),
        fila(order_id="ORD-2", quantity=5, status="Cancelled"),
    ])
    assert t["productos_por_region"][0]["unidades_vendidas"] == 3


def test_tasa_de_devolucion_por_canal(spark):
    t = tablas(spark, [
        fila(order_id="ORD-1", channel="Online", status="Completed"),
        fila(order_id="ORD-2", channel="Online", status="Completed"),
        fila(order_id="ORD-3", channel="Online", status="Cancelled"),
        fila(order_id="ORD-4", channel="Online", status="Returned"),
        fila(order_id="ORD-5", channel="In-Store", status="Completed"),
        fila(order_id="ORD-6", channel="In-Store", status="Completed"),
    ])
    por_canal = {r["channel"]: r for r in t["devoluciones_por_canal"]}
    assert por_canal["Online"]["total_ordenes"] == 4
    assert por_canal["Online"]["ordenes_devueltas"] == 1
    assert por_canal["Online"]["tasa_devolucion_porcentaje"] == pytest.approx(25.0)
    assert por_canal["In-Store"]["tasa_devolucion_porcentaje"] == pytest.approx(0.0)


def test_ventas_mensuales_agrupa_por_mes_y_excluye_no_completadas(spark):
    t = tablas(spark, [
        fila(order_id="ORD-1", order_date=date(2024, 1, 5), total_amount=100.0),
        fila(order_id="ORD-2", order_date=date(2024, 1, 20), total_amount=50.0),
        fila(order_id="ORD-3", order_date=date(2024, 1, 25), total_amount=999.0, status="Cancelled"),
        fila(order_id="ORD-4", order_date=date(2024, 2, 3), total_amount=70.0),
    ])
    meses = [(r["year"], r["month"], r["ingresos_totales"]) for r in t["ventas_mensuales"]]
    assert meses == [(2024, 1, pytest.approx(150.0)), (2024, 2, pytest.approx(70.0))]