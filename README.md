#  Lakehouse Retail Analytics

> Pipeline de datos end-to-end con arquitectura Medallion (Bronze/Silver/Gold) para análisis de ventas retail, construido con PySpark, Delta Lake y Databricks.

[![Python](https://img.shields.io/badge/Python-3.11+-blue.svg)](https://www.python.org/)
[![PySpark](https://img.shields.io/badge/PySpark-3.5-orange.svg)](https://spark.apache.org/)
[![Delta Lake](https://img.shields.io/badge/Delta_Lake-3.0-green.svg)](https://delta.io/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

##  Tabla de contenidos

- [ ¿Qué resuelve este proyecto?](#-qué-resuelve-este-proyecto)
- [ Arquitectura](#️-arquitectura)
- [ Stack tecnológico](#️-stack-tecnológico)
- [ Estructura del proyecto](#-estructura-del-proyecto)
- [ Cómo ejecutarlo](#-cómo-ejecutarlo)
- [ Resultados](#-resultados)
- [ Tests](#-tests)
- [ Licencia](#-licencia)

##  ¿Qué resuelve este proyecto?

Este proyecto implementa un **pipeline de datos end-to-end** que transforma datos crudos de ventas retail en tablas analíticas listas para consumo, siguiendo la **arquitectura Medallion**:


### Preguntas de negocio que responde

- ¿Cuáles son los productos más vendidos por región?
- ¿Qué categorías generan más ingresos?
- ¿Cuál es la tasa de devoluciones por canal de venta?
- ¿Cómo evolucionan las ventas mes a mes?

##  Arquitectura

El proyecto sigue la **arquitectura Medallion**, un patrón de diseño de datos recomendado por Databricks que organiza los datos en tres capas con niveles crecientes de calidad:

**Bronze** | **Silver** | **Gold**  

### Flujo del pipeline

Fuentes (CSV, APIs, DBs)
↓
 BRONZE → Ingesta cruda + metadata
↓
 SILVER → Limpieza, tipado, deduplicación
↓
 GOLD → Agregaciones de negocio

'''text

## 🛠️ Stack tecnológico

| **Lenguaje** | Python 3.11+ | Lógica del pipeline |
| **Procesamiento** | PySpark | Transformaciones distribuidas |
| **Almacenamiento** | Delta Lake | Formato ACID con versionado |
| **Cloud/IDE** | Databricks Community | Desarrollo y pruebas |
| **Orquestación** | Prefect | Ejecutar las capas en orden |
| **Testing** | pytest | Validar cada capa |
| **CI/CD** | GitHub Actions | Correr tests automáticamente |
| **Visualización** | Streamlit / Power BI | Dashboard final |
| **Control de versiones** | Git + GitHub | Historial del código |

##  Estructura del proyecto
lakehouse-retail-analytics/
│
├── data/ # Datos en cada capa del pipeline
│ ├── raw/ # Datos originales (nunca se modifican)
│ ├── bronze/ # Datos ingestados crudos + metadata
│ ├── silver/ # Datos limpios y validados
│ └── gold/ # Datos agregados para negocio
│
├── notebooks/ # Prototipos en Databricks
│
├── src/ # Código de producción
│ ├── config/ # Configuración (paths, constantes)
│ ├── utils/ # Utilidades (logger, spark session)
│ ├── bronze/ # Lógica de ingesta
│ ├── silver/ # Lógica de limpieza
│ └── gold/ # Lógica de agregación
│
├── tests/ # Tests automatizados con pytest
├── docs/ # Documentación y diagramas
│
├── .github/workflows/ # CI/CD con GitHub Actions
│
├── .gitignore # Archivos que Git no debe versionar
├── requirements.txt # Dependencias del proyecto
├── LICENSE # Licencia MIT
└── README.md # Este archivo

text

##  Cómo ejecutarlo

### Requisitos previos

- Python 3.11+
- Java 17 (necesario para PySpark)
- Git

### Instalación

```bash
# 1. Clonar el repositorio
git clone https://github.com/ClauRamos/lakehouse-retail-analytics.git
cd lakehouse-retail-analytics

# 2. Crear entorno virtual
python -m venv venv
venv\Scripts\activate          # Windows
# source venv/bin/activate     # Mac/Linux

# 3. Instalar dependencias
pip install -r requirements.txt
Ejecución
bash
# Correr el pipeline completo
python -m src.pipeline

# Correr solo una capa
python -m src.bronze.ingest
python -m src.silver.clean
python -m src.gold.aggregate

# Correr los tests
pytest tests/
 Resultados
(Próximamente: capturas de dashboards y tablas Gold)


(Próximamente: descripción de los tests implementados)

 Licencia
Este proyecto está bajo la licencia MIT. Ver LICENSE para más detalles.

text

---

