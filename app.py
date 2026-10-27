from pathlib import Path

import pandas as pd
import streamlit as st
from deltalake import DeltaTable

st.set_page_config(page_title="Lakehouse Retail Analytics", page_icon="🏪", layout="wide")

RUTA_GOLD = Path(__file__).parent / "data" / "gold"
TABLAS = [
    "ingresos_por_categoria",
    "ventas_mensuales",
    "productos_por_region",
    "devoluciones_por_canal",
]


def fmt_entero(n):
    """12345 -> 12.345"""
    return f"{n:,.0f}".replace(",", ".")


def fmt_pesos(n):
    """4200714.89 -> $4.200.714,89"""
    return "$" + f"{n:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")


@st.cache_data(ttl=300)
def load_data(table_name: str) -> pd.DataFrame:
    """Lee una tabla Delta de la capa Gold y la devuelve como DataFrame de pandas.
    El caché dura 5 minutos: si volvés a correr el pipeline, el dashboard se actualiza solo."""
    return DeltaTable(str(RUTA_GOLD / table_name)).to_pandas()


st.title("Dashboard de Ventas Retail")
st.markdown("Este dashboard consume las tablas de la **Capa Gold** generadas por el pipeline de PySpark.")

# Si falta alguna tabla, avisamos y frenamos antes de que el resto del código falle
faltantes = [t for t in TABLAS if not (RUTA_GOLD / t / "_delta_log").exists()]
if faltantes:
    st.error(
        f"Faltan tablas de la capa Gold: {', '.join(faltantes)}. "
        "Corré primero el pipeline: `python -m src.pipeline`"
    )
    st.stop()

df_ingresos = load_data("ingresos_por_categoria")
df_mensual = load_data("ventas_mensuales")
df_productos = load_data("productos_por_region")
df_devoluciones = load_data("devoluciones_por_canal")

# --- SECCIÓN 1: KPIs ---
st.header("📈 KPIs principales")

ingresos = df_ingresos["ingresos_totales"].sum()
ordenes_completadas = df_ingresos["ordenes_completadas"].sum()
total_ordenes = df_devoluciones["total_ordenes"].sum()
ordenes_devueltas = df_devoluciones["ordenes_devueltas"].sum()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Ingresos (órdenes completadas)", fmt_pesos(ingresos))
col2.metric("Órdenes completadas", fmt_entero(ordenes_completadas))
col3.metric("Órdenes promedio", fmt_pesos(ingresos / ordenes_completadas))
col4.metric(
    "Órdenes devueltas",
    f"{ordenes_devueltas / total_ordenes * 100:.1f} %".replace(".", ","),
    help=f"{fmt_entero(ordenes_devueltas)} devueltas sobre {fmt_entero(total_ordenes)} órdenes registradas (incluye canceladas).",
)

st.divider()

# --- SECCIÓN 2: Gráficos ---
col_izq, col_der = st.columns(2)

with col_izq:
    st.subheader("💰 Ingresos por categoría")
    st.bar_chart(df_ingresos, x="category", y="ingresos_totales", sort="-ingresos_totales", color="#1f77b4")

with col_der:
    st.subheader("📅 Evolución mensual de ventas")
    # Unimos año y mes en un texto ordenable (2023-01, 2023-02, ...)
    df_mensual = df_mensual.assign(
        periodo=df_mensual["year"].astype(str) + "-" + df_mensual["month"].astype(str).str.zfill(2)
    ).sort_values("periodo")
    st.line_chart(df_mensual, x="periodo", y="ingresos_totales", color="#ff7f0e")

st.divider()

# --- SECCIÓN 3: Productos por región ---
st.subheader("🛍️ Ranking de productos por región")
regiones = sorted(df_productos["region"].unique())
region_seleccionada = st.selectbox("Seleccioná una región:", regiones)

df_filtrado = (
    df_productos[df_productos["region"] == region_seleccionada]
    .sort_values("unidades_vendidas", ascending=False)
    .head(10)
)
st.dataframe(
    df_filtrado[["product_name", "unidades_vendidas"]],
    hide_index=True,
    width="stretch",
    column_config={
        "product_name": "Producto",
        "unidades_vendidas": st.column_config.NumberColumn("Unidades vendidas (completadas)"),
    },
)

st.divider()

# --- SECCIÓN 4: Devoluciones ---
st.subheader("📦 Tasa de devoluciones por canal")
st.dataframe(
    df_devoluciones.sort_values("tasa_devolucion_porcentaje", ascending=False),
    hide_index=True,
    width="stretch",
    column_config={
        "channel": "Canal",
        "total_ordenes": "Órdenes registradas",
        "ordenes_devueltas": "Órdenes devueltas",
        "tasa_devolucion_porcentaje": st.column_config.NumberColumn("Tasa de devolución", format="%.2f %%"),
    },
)
st.caption("Tasa de devolución = órdenes devueltas / órdenes registradas del canal (incluye canceladas).")

st.caption("Dashboard generado a partir de la arquitectura Medallion (Bronze → Silver → Gold).")