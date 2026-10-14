"""
Módulo de Transformación e Integración (Capa Plata)
Proyecto: Reliable Music Batch Pipeline - Workshop 2

Responsabilidad:
- Integrar datasets heterogéneos (Spotify y Grammys).
- Aplicar deduplicación por track_id para resolver la duplicidad de canciones multi-género (Garantía de RQ-03).
- Normalizar cadenas de texto para el cruce de entidades.
- Resolver la relación N:1 entre canciones y múltiples premios Grammy para evitar multiplicación de hechos.
- Manejar registros no coincidentes mediante un registro centinela de fecha (year=0, decade=0).
- Generar el esquema en estrella: dim_artist, dim_date, dim_genre y fact_tracks.
- Persistir los artefactos dimensionales en formato Parquet en la zona de staging.
"""

import os
import pandas as pd


def transform_and_integrate_data(spotify_raw_path: str, grammys_raw_path: str, simulate_failure: bool = False) -> str:
    """
    CAPA PLATA - TRANSFORMACIÓN E INTEGRACIÓN
    Parámetros:
    - spotify_raw_path: Ruta del Parquet crudo y validado de Spotify.
    - grammys_raw_path: Ruta del Parquet crudo y validado de Grammys.
    - simulate_failure: Si es True (Prueba C), inyecta una duplicidad deliberada para probar el bloqueo de load_dw.
    Retorna:
    - Ruta del archivo Parquet de hechos preparados (prepared_fact_tracks.parquet).
    """
    print("[TRANSFORM] Iniciando fase de Transformación e Integración (Capa Plata)...")
    df_sp = pd.read_parquet(spotify_raw_path)
    df_gr = pd.read_parquet(grammys_raw_path)

    # -----------------------------------------------------------------
    # 1. DEDUPLICACIÓN DE SPOTIFY (Solución de ingeniería para RQ-03)
    # -----------------------------------------------------------------
    # Una canción puede aparecer en múltiples géneros; conservamos la versión con mayor popularidad.
    df_sp_clean = df_sp.sort_values(by=['popularity'], ascending=False).drop_duplicates(subset=['track_id']).copy()
    print(f"[TRANSFORM] Spotify deduplicado: {len(df_sp_clean)} canciones únicas de {len(df_sp)} originales.")

    # -----------------------------------------------------------------
    # 2. NORMALIZACIÓN DE TEXTO PARA CRUCE
    # -----------------------------------------------------------------
    df_sp_clean['track_norm'] = df_sp_clean['track_name'].astype(str).str.lower().str.strip()
    df_sp_clean['artist_norm'] = df_sp_clean['artists'].astype(str).str.lower().str.strip()

    df_gr['nominee_norm'] = df_gr['nominee'].astype(str).str.lower().str.strip()
    df_gr['artist_norm'] = df_gr['artist'].astype(str).str.lower().str.strip()
    df_gr['year'] = pd.to_numeric(df_gr['year'], errors='coerce').fillna(0).astype(int)
    df_gr['decade'] = (df_gr['year'] // 10) * 10

    # -----------------------------------------------------------------
    # 3. TRATAMIENTO DE GRAMMYS (Contrato de Integración y Cardinalidad N:1)
    # -----------------------------------------------------------------
    # Filtrar ganadores y agrupar para evitar la multiplicación artificial de registros en la tabla de hechos.
    grammy_winners = df_gr[df_gr['winner'].isin([1, '1', True])].copy()
    winning_tracks = grammy_winners.groupby('nominee_norm').agg({
        'year': 'min',
        'decade': 'min',
        'category': lambda s: '; '.join(sorted(set(s.dropna().astype(str))))
    }).reset_index()

    # -----------------------------------------------------------------
    # 4. CRUCE (LEFT JOIN) Y REGISTROS NO COINCIDENTES (Centinela)
    # -----------------------------------------------------------------
    df_merged = df_sp_clean.merge(
        winning_tracks,
        left_on='track_norm',
        right_on='nominee_norm',
        how='left'
    )

    df_merged['is_grammy_winner'] = df_merged['nominee_norm'].notnull()
    # Canciones sin premio se asignan al centinela (year=0, decade=0: No Aplica)
    df_merged['year'] = df_merged['year'].fillna(0).astype(int)
    df_merged['decade'] = df_merged['decade'].fillna(0).astype(int)
    df_merged['category'] = df_merged['category'].fillna("None")

    # -----------------------------------------------------------------
    # 5. MODELADO DIMENSIONAL (Esquema en Estrella)
    # -----------------------------------------------------------------
    staging_dir = '/opt/airflow/data/staging'
    os.makedirs(staging_dir, exist_ok=True)

    # Dimensión Fecha (dim_date) con registro centinela incluido
    dim_date = df_merged[['year', 'decade']].drop_duplicates().sort_values(by='year').reset_index(drop=True)
    dim_date['date_id_sk'] = dim_date.index + 1

    # Dimensión Artista (dim_artist)
    dim_artist = df_merged[['artists', 'popularity']].drop_duplicates(subset=['artists']).reset_index(drop=True)
    dim_artist = dim_artist.rename(columns={'artists': 'artist_name', 'popularity': 'spotify_popularity'})
    dim_artist['artist_id_sk'] = dim_artist.index + 1

    # Dimensión Género (dim_genre)
    dim_genre = df_merged[['track_genre']].drop_duplicates().sort_values(by='track_genre').reset_index(drop=True)
    dim_genre = dim_genre.rename(columns={'track_genre': 'genre_name'})
    dim_genre['genre_id_sk'] = dim_genre.index + 1

    # -----------------------------------------------------------------
    # 6. TABLA DE HECHOS (fact_tracks)
    # -----------------------------------------------------------------
    # Enlazar con las dimensiones para obtener las llaves subrogadas (SK)
    fact_tracks = df_merged.merge(dim_date, on=['year', 'decade'], how='left')
    fact_tracks = fact_tracks.merge(dim_artist, left_on='artists', right_on='artist_name', how='left')
    fact_tracks = fact_tracks.merge(dim_genre, left_on='track_genre', right_on='genre_name', how='left')

    fact_df = pd.DataFrame({
        'track_id_bk': fact_tracks['track_id'],
        'track_name': fact_tracks['track_name'],
        'artist_id_sk': fact_tracks['artist_id_sk'].astype(int),
        'date_id_sk': fact_tracks['date_id_sk'].astype(int),
        'genre_id_sk': fact_tracks['genre_id_sk'].astype(int),
        'danceability': fact_tracks['danceability'].astype(float),
        'energy': fact_tracks['energy'].astype(float),
        'valence': fact_tracks['valence'].astype(float),
        'is_grammy_winner': fact_tracks['is_grammy_winner'].astype(int),
        'grammy_award_category': fact_tracks['category']
    })
    
    # -----------------------------------------------------------------
    # SIMULACIÓN CONTROLADA: PRUEBA C (Sección 6.9)
    # -----------------------------------------------------------------
    if simulate_failure:
        print("[TRANSFORM] [SIMULACIÓN PRUEBA C] Inyectando duplicado artificial en track_id_bk para probar el bloqueo de load_dw...")
        fact_df = pd.concat([fact_df, fact_df.iloc[[0]]], ignore_index=True)

    # Persistencia en staging
    output_path = os.path.join(staging_dir, 'prepared_fact_tracks.parquet')
    fact_df.to_parquet(output_path, index=False)
    dim_artist.to_parquet(os.path.join(staging_dir, 'dim_artist.parquet'), index=False)
    dim_date.to_parquet(os.path.join(staging_dir, 'dim_date.parquet'), index=False)
    dim_genre.to_parquet(os.path.join(staging_dir, 'dim_genre.parquet'), index=False)

    print(f"[TRANSFORM] Éxito: {len(fact_df)} registros en fact_tracks preparados en {output_path}")
    return output_path
