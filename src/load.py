"""
Módulo de Carga Dimensional al Data Warehouse (Capa Oro)
Proyecto: Reliable Music Batch Pipeline - Workshop 2

Responsabilidad:
- Cargar los datos transformados y validados al Data Warehouse relacional (MySQL).
- Garantizar idempotencia y reejecución segura:
  * El DAG puede ejecutarse múltiples veces sobre el mismo período sin duplicar datos ni fallar por violación de claves.
  * Uso de transacciones para truncar tablas dimensionales y de hechos de forma segura (SET FOREIGN_KEY_CHECKS = 0).
- Inserción masiva optimizada por lotes (chunksize).
- Auditoría mediante registro de métricas de filas cargadas por tabla.
"""

import os
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook


def load_dw_data(prepared_path: str) -> str:
    """
    CAPA ORO - CARGA AL DATA WAREHOUSE
    Parámetros:
    - prepared_path: Ruta del Parquet de hechos validados (prepared_fact_tracks.parquet).
    Retorna:
    - Mensaje de confirmación de carga exitosa con métricas.
    """
    print("[LOAD DW] Iniciando carga dimensional idempotente al Data Warehouse MySQL...")
    hook = MySqlHook(mysql_conn_id='mysql_dw')
    engine = hook.get_sqlalchemy_engine()
    staging_dir = '/opt/airflow/data/staging'

    # -----------------------------------------------------------------
    # 1. LECTURA DE ARTEFACTOS DIMENSIONALES PREPARADOS
    # -----------------------------------------------------------------
    dim_artist = pd.read_parquet(os.path.join(staging_dir, 'dim_artist.parquet'))
    dim_date = pd.read_parquet(os.path.join(staging_dir, 'dim_date.parquet'))
    dim_genre = pd.read_parquet(os.path.join(staging_dir, 'dim_genre.parquet'))
    fact_tracks = pd.read_parquet(prepared_path)

    # -----------------------------------------------------------------
    # 2. LIMPIEZA TRANSACCIONAL IDEMPOTENTE (Sección 7.3)
    # -----------------------------------------------------------------
    print("[LOAD DW] Aplicando vaciado transaccional seguro de tablas...")
    with engine.begin() as conn:
        conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS = 0;")
        conn.exec_driver_sql("TRUNCATE TABLE fact_tracks;")
        conn.exec_driver_sql("TRUNCATE TABLE dim_artist;")
        conn.exec_driver_sql("TRUNCATE TABLE dim_date;")
        conn.exec_driver_sql("TRUNCATE TABLE dim_genre;")
        conn.exec_driver_sql("SET FOREIGN_KEY_CHECKS = 1;")

    # -----------------------------------------------------------------
    # 3. INSERCIÓN MASIVA POR LOTES (CHUNKSIZE)
    # -----------------------------------------------------------------
    print("[LOAD DW] Insertando dimensiones...")
    dim_artist[['artist_id_sk', 'artist_name', 'spotify_popularity']].to_sql(
        'dim_artist', con=engine, if_exists='append', index=False, chunksize=5000
    )
    dim_date[['date_id_sk', 'year', 'decade']].to_sql(
        'dim_date', con=engine, if_exists='append', index=False
    )
    dim_genre[['genre_id_sk', 'genre_name']].to_sql(
        'dim_genre', con=engine, if_exists='append', index=False
    )

    print("[LOAD DW] Insertando tabla de hechos fact_tracks...")
    fact_tracks.to_sql(
        'fact_tracks', con=engine, if_exists='append', index=False, chunksize=10000
    )

    summary = (
        f"Carga al DW exitosa: "
        f"dim_artist ({len(dim_artist)}), "
        f"dim_date ({len(dim_date)}), "
        f"dim_genre ({len(dim_genre)}), "
        f"fact_tracks ({len(fact_tracks)})"
    )
    print(f"[LOAD DW] {summary}")
    return summary
