import pytest
from pyspark.sql.types import DoubleType, IntegerType, StringType, StructField, StructType

from src.silver.clean import limpiar_silver

ESQUEMA = StructType([
    StructField("order_id", StringType()),
    StructField("customer_id", StringType()),
    StructField("product_id", StringType()),
    StructField("product_name", StringType()),
    StructField("region", StringType()),
    StructField("category", StringType()),
    StructField("quantity", IntegerType()),
    StructField("unit_price", DoubleType()),
    StructField("discount", DoubleType()),
    StructField("total_amount", DoubleType()),
])


def fila(**cambios):
    """Una fila válida de Bronze; cada test cambia solo lo que quiere probar."""
    base = dict(
        order_id="ORD-1", customer_id="C1", product_id="P001", product_name="Laptop Pro 15",
        region="Norte", category="Electronics", quantity=1, unit_price=100.0,
        discount=0.0, total_amount=100.0,
    )
    base.update(cambios)
    return base


def limpiar(spark, filas):
    datos = [tuple(f[campo.name] for campo in ESQUEMA) for f in filas]
    return limpiar_silver(spark.createDataFrame(datos, ESQUEMA))


def unica_fila(resultado):
    filas = resultado.collect()
    assert len(filas) == 1
    return filas[0]


# --- Filas inválidas y duplicadas ---

def test_elimina_filas_sin_order_id(spark):
    resultado = limpiar(spark, [fila(), fila(order_id=None, product_id="P002")])
    assert resultado.count() == 1


def test_elimina_duplicados_exactos(spark):
    resultado = limpiar(spark, [fila(), fila()])
    assert resultado.count() == 1


def test_no_elimina_filas_distintas(spark):
    resultado = limpiar(spark, [fila(order_id="ORD-1"), fila(order_id="ORD-2")])
    assert resultado.count() == 2


# --- Cantidad ---

def test_recupera_cantidad_negativa_desde_total_amount(spark):
    # 10 unidades a $100 con 10% de descuento = $900; el dato crudo traía -1
    r = unica_fila(limpiar(spark, [fila(quantity=-1, discount=0.10, total_amount=900.0)]))
    assert r["quantity"] == 10
    assert r["quantity_corrected"] is True


def test_cantidad_valida_no_se_modifica(spark):
    r = unica_fila(limpiar(spark, [fila(quantity=3, total_amount=300.0)]))
    assert r["quantity"] == 3
    assert r["quantity_corrected"] is False


# --- Descuento ---

def test_deduce_descuento_nulo_desde_total_amount(spark):
    # 2 x $100 = $200; el total es $190, o sea que hubo 5% de descuento
    r = unica_fila(limpiar(spark, [fila(quantity=2, discount=None, total_amount=190.0)]))
    assert r["discount"] == pytest.approx(0.05)
    assert r["discount_imputed"] is True


def test_descuento_nulo_sin_descuento_real_queda_en_cero(spark):
    r = unica_fila(limpiar(spark, [fila(quantity=2, discount=None, total_amount=200.0)]))
    assert r["discount"] == pytest.approx(0.0)


def test_descuento_existente_no_se_modifica(spark):
    r = unica_fila(limpiar(spark, [fila(discount=0.15, total_amount=85.0)]))
    assert r["discount"] == pytest.approx(0.15)
    assert r["discount_imputed"] is False


def test_descarta_fila_con_cantidad_negativa_y_descuento_nulo(spark):
    # Sin descuento no se puede saber cuál era la cantidad real
    resultado = limpiar(spark, [fila(quantity=-1, discount=None, total_amount=900.0)])
    assert resultado.count() == 0


# --- Textos ---

def test_recupera_product_name_desde_product_id(spark):
    resultado = limpiar(spark, [
        fila(order_id="ORD-1", product_id="P008", product_name='Monitor 27"'),
        fila(order_id="ORD-2", product_id="P008", product_name=None),
        fila(order_id="ORD-3", product_id="P008", product_name="   "),
    ])
    nombres = {r["product_name"] for r in resultado.collect()}
    assert nombres == {'Monitor 27"'}


def test_product_name_sin_catalogo_queda_unknown(spark):
    r = unica_fila(limpiar(spark, [fila(product_id="P999", product_name=None)]))
    assert r["product_name"] == "UNKNOWN"


def test_customer_id_nulo_o_vacio_queda_unknown(spark):
    resultado = limpiar(spark, [
        fila(order_id="ORD-1", customer_id=None),
        fila(order_id="ORD-2", customer_id="  "),
        fila(order_id="ORD-3", customer_id="C77"),
    ])
    clientes = {r["order_id"]: r["customer_id"] for r in resultado.collect()}
    assert clientes == {"ORD-1": "UNKNOWN", "ORD-2": "UNKNOWN", "ORD-3": "C77"}


def test_normaliza_espacios_y_mayusculas(spark):
    r = unica_fila(limpiar(spark, [fila(region="  norte ", category=" electronics", product_name=" iPhone ")]))
    assert r["region"] == "Norte"
    assert r["category"] == "Electronics"
    assert r["product_name"] == "iPhone"  # solo trim: no se toca la mayúscula de las marcas


# --- Control general ---

def test_total_amount_es_coherente_en_todo_el_resultado(spark):
    resultado = limpiar(spark, [
        fila(order_id="A", quantity=-1, discount=0.10, total_amount=900.0),  # cantidad recuperada
        fila(order_id="B", quantity=2, discount=None, total_amount=190.0),   # descuento deducido
        fila(order_id="C", quantity=3, discount=0.20, total_amount=240.0),   # fila normal
    ])
    assert resultado.count() == 3
    for r in resultado.collect():
        esperado = r["quantity"] * r["unit_price"] * (1 - r["discount"])
        assert esperado == pytest.approx(r["total_amount"], abs=0.02)