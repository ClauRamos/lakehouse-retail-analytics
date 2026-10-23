import streamlit as st
import pandas as pd
import os
from deltalake import DeltaTable

# Configuración de la página
st.set_page_config(page_title="Lakehouse Retail Analytics", page_icon="🏪", layout="wide")

st.title( "Dashboard de Ventas Retail")
st.markdown("Este dashboard consume las tablas de la **Capa Gold** generadas por el pipeline de PySpark.")

RUTA_GOLD = "data/gold"

# Función para leer tablas Delta (usando deltalake)
@st.cache_data
def load_data(table_name):
    ruta = os.path.join(RUTA_GOLD, table_name)
    if not os.path.exists(ruta):
        st.error(f"No se encontró la tabla {table_name}. Corré el pipeline primero.")
        return pd.DataFrame()
    # Leemos la tabla Delta y la pasamos a Pandas
    dt = DeltaTable(ruta)
    return dt.to_pandas()

# Cargar datos
df_ingresos = load_data("ingresos_por_categoria")
df_mensual = load_data("ventas_mensuales")
df_productos = load_data("productos_por_region")
df_devoluciones = load_data("devoluciones_por_canal")

# --- SECCIÓN 1: KPIs Generales ---
st.header("📈 KPIs Principales")
col1, col2, col3 = st.columns(3)

with col1:
    total_ingresos = df_ingresos["ingresos_totales"].sum()
    st.metric(label="Ingresos Totales", value=f"${total_ingresos:,.2f}")

with col2:
    st.metric(label="Categorías Analizadas", value=len(df_ingresos))

with col3:
    total_ordenes = df_devoluciones["total_ordenes"].sum()
    st.metric(label="Total de Órdenes", value=f"{total_ordenes:,.0f}")

st.divider()

# --- SECCIÓN 2: Gráficos ---
col_izq, col_der = st.columns(2)

with col_izq:
    st.subheader("💰 Ingresos por Categoría")
    st.bar_chart(df_ingresos.set_index("category")["ingresos_totales"], color="#1f77b4")

with col_der:
    st.subheader("📅 Evolución Mensual de Ventas")
    # Para evitar problemas con fechas, unimos año y mes en un texto
    df_mensual["periodo"] = df_mensual["year"].astype(str) + "-" + df_mensual["month"].astype(str).str.zfill(2)
    st.line_chart(df_mensual.set_index("periodo")["ingresos_totales"], color="#ff7f0e")
st.divider()

# --- SECCIÓN 3: Tablas Detalladas ---
st.subheader("🛍️ Top Productos por Región")
# Selector de región
regiones = df_productos["region"].unique()
region_seleccionada = st.selectbox("Seleccioná una Región:", regiones)

df_filtrado = df_productos[df_productos["region"] == region_seleccionada].head(10)
st.dataframe(df_filtrado, use_container_width=True)

st.divider()

# --- SECCIÓN 4: Devoluciones ---
st.subheader("📦 Tasa de Devoluciones por Canal")
st.dataframe(df_devoluciones, use_container_width=True)

st.caption("Dashboard generado automáticamente a partir de la arquitectura Medallion (Bronze -> Silver -> Gold).")