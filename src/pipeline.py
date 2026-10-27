import os
import sys
import time

RUTA_RAIZ = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if RUTA_RAIZ not in sys.path:
    sys.path.append(RUTA_RAIZ)

from src.bronze.ingest import ingest_to_bronze
from src.silver.clean import clean_silver
from src.gold.aggregate import aggregate_gold

ETAPAS = [
    ("Bronze", ingest_to_bronze),
    ("Silver", clean_silver),
    ("Gold", aggregate_gold),
]


def run_pipeline():
    """Ejecuta Bronze -> Silver -> Gold en orden. Si una etapa falla, se detiene."""
    inicio_total = time.time()

    for nombre, etapa in ETAPAS:
        print(f"\n=== Etapa {nombre} ===")
        inicio = time.time()
        try:
            etapa()
        except Exception as error:
            print(f"\nLa etapa {nombre} falló: {error}")
            print("Se detiene el pipeline: las etapas siguientes dependen de esta.")
            sys.exit(1)
        print(f"Etapa {nombre} terminada en {time.time() - inicio:.1f} s")

    print(f"\nPipeline completo en {time.time() - inicio_total:.1f} s")


if __name__ == "__main__":
    run_pipeline()