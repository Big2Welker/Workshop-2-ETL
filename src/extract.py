"""
Módulo de Extracción de Datos (Capa Bronce)
Proyecto: Reliable Music Batch Pipeline - Workshop 2
"""

import os
import pandas as pd
from airflow.providers.mysql.hooks.mysql import MySqlHook


def extract_spotify_data(simulate_failure: bool = False) -> str:
    """
    Extrae el dataset de Spotify desde el archivo CSV plano.
    Parámetros:
    - simulate_failure: Si es False (Test A), entrega lote limpio. Si es True (Test B), preserva anomalías.
    Retorna:
    - Ruta del archivo Parquet crudo generado en el volumen compartido.
    """
    print("[EXTRACT SPOTIFY] Iniciando lectura de spotify_dataset.csv...")
    input_path = '/opt/airflow/data/spotify_dataset.csv'
    output_path = '/opt/airflow/data/staging/spotify_raw.parquet'
    os.makedirs('/opt/airflow/data/staging', exist_ok=True)
    
    df = pd.read_csv(input_path)
    
    # En Test A (simulate_failure=False), se entrega el lote que satisface la política (sin nulos)
    # En Test B (simulate_failure=True), se conserva el lote crudo con sus anomalías
    if not simulate_failure:
        df = df.dropna(subset=['track_name', 'artists'])
        
    df.to_parquet(output_path, index=False)
    print(f"[EXTRACT SPOTIFY] Éxito: {len(df)} filas almacenadas en {output_path}")
    return output_path


def extract_grammys_data(simulate_failure: bool = False) -> str:
    """
    Extrae datos históricos de Grammys directamente desde la base de datos MySQL (tabla grammy_source).
    No lee el archivo CSV directamente, cumpliendo con la Sección 4.3 del taller.
    Parámetros:
    - simulate_failure: Si es True (Test B), extrae la tabla completa incluyendo los 6 nulos en 'nominee'.
    Retorna:
    - Ruta del archivo Parquet crudo generado en el volumen compartido.
    """
    print("[EXTRACT GRAMMYS] Conectando a MySQL mediante MySqlHook (mysql_dw)...")
    hook = MySqlHook(mysql_conn_id='mysql_dw')
    engine = hook.get_sqlalchemy_engine()
    output_path = '/opt/airflow/data/staging/grammys_raw.parquet'
    os.makedirs('/opt/airflow/data/staging', exist_ok=True)
    
    if simulate_failure:
        # En Test B se extrae la tabla completa incluyendo los 6 registros nulos en 'nominee' (detona RQ-04)
        query = "SELECT * FROM grammy_source"
    else:
        # En Test A se extrae el lote limpio que satisface la política crítica
        query = "SELECT * FROM grammy_source WHERE nominee IS NOT NULL AND nominee != ''"
        
    df = pd.read_sql(query, con=engine)
    df.to_parquet(output_path, index=False)
    print(f"[EXTRACT GRAMMYS] Éxito: {len(df)} filas extraídas desde MySQL hacia {output_path}")
    return output_path
