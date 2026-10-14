"""
DAG Orquestador: Reliable Music Batch Pipeline
Proyecto: Reliable Music Batch Pipeline - Workshop 2

"""

from datetime import datetime, timedelta
from airflow.sdk import dag, task, Param

# Importación de la lógica de negocio desacoplada (src/)
from src.extract import extract_spotify_data, extract_grammys_data
from src.validation import (
    validate_spotify_raw_data,
    validate_grammys_raw_data,
    validate_prepared_data,
)
from src.transform import transform_and_integrate_data
from src.load import load_dw_data


@dag(
    dag_id="reliable_music_pipeline",
    schedule=None,
    start_date=datetime(2026, 10, 7),
    catchup=False,
    tags=["workshop", "etl", "taskflow", "medallion", "modular"],
    params={
        "simulate_critical_failure": Param(
            False,
            type="boolean",
            description="True = Prueba B (Fallo crítico en compuerta cruda RQ-04). False = Normal."
        ),
        "simulate_prepared_failure": Param(
            False,
            type="boolean",
            description="True = Prueba C (Fallo crítico en compuerta preparada RQ-03, bloqueando load_dw). False = Normal."
        )
    }
)
def music_etl_pipeline():

    # =========================================================================
    # CAPA BRONCE: RAMA SPOTIFY (Extracción y Compuerta Cruda)
    # =========================================================================
    @task(retries=0)  # Fallo determinista de archivo local: sin reintentos
    def extract_spotify(**context) -> str:
        """Ingesta del dataset Spotify desde archivo plano CSV hacia formato Parquet."""
        simulate_failure = context["params"].get("simulate_critical_failure", False)
        return extract_spotify_data(simulate_failure=simulate_failure)

    @task(retries=0)  # Fallo determinista de calidad de datos: parada inmediata
    def validate_spotify_raw(spotify_raw_path: str) -> str:
        """Compuerta de Calidad GX: Evalúa completitud (RQ-01) y rangos matemáticos (RQ-02)."""
        return validate_spotify_raw_data(spotify_raw_path)

    # =========================================================================
    # CAPA BRONCE: RAMA GRAMMYS (Extracción Relacional y Compuerta Cruda)
    # =========================================================================
    @task(retries=2, retry_delay=timedelta(seconds=10))  # Operativo transitorio (red/BD): reintento limitado
    def extract_grammys(**context) -> str:
        """Ingesta de Grammys desde base relacional MySQL (tabla grammy_source)."""
        simulate_failure = context["params"].get("simulate_critical_failure", False)
        return extract_grammys_data(simulate_failure=simulate_failure)

    @task(retries=0)  # Fallo determinista de calidad de datos: parada inmediata
    def validate_grammys_raw(grammys_raw_path: str) -> str:
        """Compuerta de Calidad GX: Evalúa sujeto premiado (RQ-04) y victoria (RQ-05)."""
        return validate_grammys_raw_data(grammys_raw_path)

    # =========================================================================
    # CAPA PLATA: TRANSFORMACIÓN, INTEGRACIÓN Y COMPUERTA PREPARADA
    # =========================================================================
    @task(retries=0)  # Cómputo determinista en CPU: sin reintentos
    def transform_and_integrate(spotify_raw_path: str, grammys_raw_path: str, **context) -> str:
        """Deduplicación (RQ-03), normalización, cruce N:1, centinela y modelado dimensional."""
        simulate_failure = context["params"].get("simulate_prepared_failure", False)
        return transform_and_integrate_data(spotify_raw_path, grammys_raw_path, simulate_failure=simulate_failure)

    @task(retries=0)  # Fallo determinista de calidad de datos: parada inmediata
    def validate_prepared(prepared_path: str) -> str:
        """Compuerta de Calidad GX: Evalúa unicidad (RQ-03) e integridad referencial (RQ-06)."""
        return validate_prepared_data(prepared_path)

    # =========================================================================
    # CAPA ORO: CARGA DIMENSIONAL EN DATA WAREHOUSE
    # =========================================================================
    @task(retries=2, retry_delay=timedelta(seconds=10))  # Operativo transitorio de base de datos: reintento limitado
    def load_dw(prepared_path: str) -> str:
        """Carga masiva e idempotente en tablas dimensionales y hechos del DW en MySQL."""
        return load_dw_data(prepared_path)

    # =========================================================================
    # TOPOLOGÍA Y DEPENDENCIAS DEL FLUJO (Medallion Architecture)
    # =========================================================================
    # 1. Ejecución paralela e independiente de las ramas de origen
    sp_raw = extract_spotify()
    sp_validated = validate_spotify_raw(sp_raw)

    gr_raw = extract_grammys()
    gr_validated = validate_grammys_raw(gr_raw)

    # 2. Convergencia en Capa Plata una vez superadas ambas compuertas
    prepared_data = transform_and_integrate(sp_validated, gr_validated)
    prepared_validated = validate_prepared(prepared_data)

    # 3. Carga final en Capa Oro (Data Warehouse)
    load_dw(prepared_validated)


# Instanciación del DAG
reliable_music_pipeline = music_etl_pipeline()