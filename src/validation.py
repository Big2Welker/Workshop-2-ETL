"""
Módulo de Validación de Calidad de Datos (Great Expectations 1.x)
Proyecto: Reliable Music Batch Pipeline - Workshop 2

Responsabilidad:
- Implementar los 5 bloques secuenciales de Great Expectations para evaluar la calidad de datos.
- Actuar como compuertas de calidad (Quality Gates / Circuit Breakers) en Capa Bronce y Capa Plata.
- Política de Fallos:
  * Severidad 'Critical': Detener el pipeline con excepción (ValueError) para congelar tareas downstream.
  * Severidad 'Warning': Registrar la anomalía en logs y reportes JSON, pero permitir que el flujo continúe.
- Persistir reportes estructurados JSON en /opt/airflow/docs/evidence/ para auditoría.
"""

import json
import logging
import os
from datetime import datetime
import pandas as pd
import great_expectations as gx

# Silenciar logs internos de nivel debug/info de Great Expectations
logging.getLogger("great_expectations").setLevel(logging.WARNING)


def save_gx_report(val_result, report_path: str) -> None:
    """
    Persiste los resultados de validación de Great Expectations en un archivo JSON estructurado.
    Permite auditar el cumplimiento de reglas, severidades, detalles técnicos y estados de éxito.
    """
    os.makedirs(os.path.dirname(report_path), exist_ok=True)
    report = {
        "suite_name": getattr(val_result, "suite_name", "unknown_suite"),
        "timestamp": datetime.utcnow().isoformat(),
        "overall_success": bool(val_result.success),
        "statistics": {
            "total_expectations": len(val_result.results),
            "successful_expectations": sum(1 for r in val_result.results if r.success),
            "failed_expectations": sum(1 for r in val_result.results if not r.success),
        },
        "results": [
            {
                "rule_id": r.expectation_config.meta.get("rule_id", "N/A"),
                "severity": r.expectation_config.meta.get("severity", "N/A"),
                "expectation_type": r.expectation_config.type,
                "success": bool(r.success),
                "details": r.result
            }
            for r in val_result.results
        ]
    }
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    print(f"[GX REPORT] Reporte persistido exitosamente en: {report_path}")


def validate_spotify_raw_data(spotify_raw_path: str) -> str:
    """
    CAPA BRONCE - COMPUERTA DE CALIDAD: SPOTIFY RAW
    Responsabilidad:
    - Responder: ¿Son los datos entrantes de Spotify lo suficientemente seguros para continuar?
    - Evaluar reglas:
      * RQ-01 (Critical): No nulos en 'track_name' ni 'artists'.
      * RQ-02 (Critical): Métricas acústicas ('danceability', 'energy', 'valence') en rango [0.0, 1.0].
    - Detener el pipeline si alguna regla crítica falla (Circuit Breaker).
    
    Implementación de los 5 Bloques de Great Expectations:
    - Bloque 1: Data Context (efímero en memoria RAM).
    - Bloque 2: Data Source & Data Asset (Pandas batch definition).
    - Bloque 3: Expectation Suite (spotify_raw_suite).
    - Bloque 4: Validation Definition (enlace estructural datos-suite).
    - Bloque 5: Checkpoint / Run (evaluación contra el DataFrame real).
    """
    print(f"[VAL SPOTIFY RAW] Iniciando validación cruda desde {spotify_raw_path}...")
    df = pd.read_parquet(spotify_raw_path)

    # -------------------------------------------------------------
    # [BLOQUE 1 GX: DATA CONTEXT] Administrador central en memoria RAM
    # -------------------------------------------------------------
    context = gx.get_context(mode="ephemeral")

    # -------------------------------------------------------------
    # [BLOQUE 2 GX: DATA SOURCE & DATA ASSET] Conexión al motor de datos
    # -------------------------------------------------------------
    data_source = context.data_sources.add_pandas("spotify_raw_source")
    data_asset = data_source.add_dataframe_asset(name="spotify_raw_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("spotify_raw_batch")

    # -------------------------------------------------------------
    # [BLOQUE 3 GX: EXPECTATION SUITE] Reglas de calidad y severidades
    # -------------------------------------------------------------
    suite = context.suites.add(gx.ExpectationSuite(name="spotify_raw_suite"))
    
    # Regla RQ-01: Completitud de metadatos base (Severidad: Critical)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="track_name", meta={"rule_id": "RQ-01", "severity": "Critical"}
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="artists", meta={"rule_id": "RQ-01", "severity": "Critical"}
        )
    )
    
    # Regla RQ-02: Rango matemático de métricas acústicas [0.0 - 1.0] (Severidad: Critical)
    for col in ['danceability', 'energy', 'valence']:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=0.0, max_value=1.0, meta={"rule_id": "RQ-02", "severity": "Critical"}
            )
        )

    # -------------------------------------------------------------
    # [BLOQUE 4 GX: VALIDATION DEFINITION] Enlace estructural
    # -------------------------------------------------------------
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="spotify_raw_val_def", data=batch_definition, suite=suite)
    )

    # -------------------------------------------------------------
    # [BLOQUE 5 GX: CHECKPOINT / EJECUCIÓN] Evaluación en memoria
    # -------------------------------------------------------------
    val_result = val_def.run(batch_parameters={"dataframe": df})

    # Guardar reporte estructurado
    report_path = "/opt/airflow/docs/evidence/gx_spotify_raw_results.json"
    save_gx_report(val_result, report_path)

    # Circuit Breaker: Evaluar severidad
    if not val_result.success:
        print("[VAL SPOTIFY RAW] ¡Fallo de calidad detectado en compuerta cruda de Spotify!")
        critical_failure = False
        for result in val_result.results:
            if not result.success:
                meta = result.expectation_config.meta
                print(f"  - Regla {meta.get('rule_id')} ({meta.get('severity')}) falló: {result.expectation_config.type}")
                if meta.get("severity") == "Critical":
                    critical_failure = True
        if critical_failure:
            raise ValueError("[CIRCUIT BREAKER] Validación cruda de Spotify falló en regla CRÍTICA (RQ-01/RQ-02). Flujo detenido.")

    print("[VAL SPOTIFY RAW] Compuerta cruda de Spotify aprobada exitosamente.")
    return spotify_raw_path


def validate_grammys_raw_data(grammys_raw_path: str) -> str:
    """
    CAPA BRONCE - COMPUERTA DE CALIDAD: GRAMMYS RAW
    Responsabilidad:
    - Responder: ¿Son los datos entrantes de Grammys lo suficientemente seguros para continuar?
    - Evaluar reglas:
      * RQ-04 (Critical): No nulos en 'nominee' (sujeto galardonado).
      * RQ-05 (Warning): Integridad del indicador 'winner' en conjunto permitido [1, '1', True, 'True'].
    - Detener el pipeline si falla RQ-04 (Critical) (utilizado en Test B para simulación controlada).
    - Registrar advertencia si falla RQ-05 (Warning) sin detener la ejecución.
    
    Implementación de los 5 Bloques de Great Expectations:
    - Bloque 1: Data Context (efímero en memoria RAM).
    - Bloque 2: Data Source & Data Asset (Pandas batch definition).
    - Bloque 3: Expectation Suite (grammys_raw_suite).
    - Bloque 4: Validation Definition (enlace estructural datos-suite).
    - Bloque 5: Checkpoint / Run (evaluación contra el DataFrame real).
    """
    print(f"[VAL GRAMMYS RAW] Iniciando validación cruda desde {grammys_raw_path}...")
    df = pd.read_parquet(grammys_raw_path)

    # -------------------------------------------------------------
    # [BLOQUE 1 GX: DATA CONTEXT] Administrador central en memoria RAM
    # -------------------------------------------------------------
    context = gx.get_context(mode="ephemeral")

    # -------------------------------------------------------------
    # [BLOQUE 2 GX: DATA SOURCE & DATA ASSET] Conexión al motor de datos
    # -------------------------------------------------------------
    data_source = context.data_sources.add_pandas("grammys_raw_source")
    data_asset = data_source.add_dataframe_asset(name="grammys_raw_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("grammys_raw_batch")

    # -------------------------------------------------------------
    # [BLOQUE 3 GX: EXPECTATION SUITE] Reglas de calidad y severidades
    # -------------------------------------------------------------
    suite = context.suites.add(gx.ExpectationSuite(name="grammys_raw_suite"))
    
    # Regla RQ-04: Integridad del sujeto premiado (Severidad: Critical)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="nominee", meta={"rule_id": "RQ-04", "severity": "Critical"}
        )
    )
    
    # Regla RQ-05: Consistencia de indicador de victoria (Severidad: Warning)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="winner", value_set=['1', 1, True, "True"], meta={"rule_id": "RQ-05", "severity": "Warning"}
        )
    )

    # -------------------------------------------------------------
    # [BLOQUE 4 GX: VALIDATION DEFINITION] Enlace estructural
    # -------------------------------------------------------------
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="grammys_raw_val_def", data=batch_definition, suite=suite)
    )

    # -------------------------------------------------------------
    # [BLOQUE 5 GX: CHECKPOINT / EJECUCIÓN] Evaluación en memoria
    # -------------------------------------------------------------
    val_result = val_def.run(batch_parameters={"dataframe": df})

    # Guardar reporte estructurado
    report_path = "/opt/airflow/docs/evidence/gx_grammys_raw_results.json"
    save_gx_report(val_result, report_path)

    # Circuit Breaker: Evaluar severidad
    if not val_result.success:
        print("[VAL GRAMMYS RAW] ¡Fallo de calidad detectado en compuerta cruda de Grammys!")
        critical_failure = False
        for result in val_result.results:
            if not result.success:
                meta = result.expectation_config.meta
                print(f"  - Regla {meta.get('rule_id')} ({meta.get('severity')}) falló: {result.expectation_config.type}")
                if meta.get("severity") == "Critical":
                    critical_failure = True
                elif meta.get("severity") == "Warning":
                    print(f"  [ALERTA WARNING] La regla {meta.get('rule_id')} falló pero la política permite continuar.")
                    
        if critical_failure:
            raise ValueError("[CIRCUIT BREAKER] Validación cruda de Grammys falló en regla CRÍTICA (RQ-04). Proceso detenido.")

    print("[VAL GRAMMYS RAW] Compuerta cruda de Grammys aprobada exitosamente.")
    return grammys_raw_path


def validate_prepared_data(prepared_path: str) -> str:
    """
    CAPA PLATA - COMPUERTA DE CALIDAD: DATOS PREPARADOS
    Responsabilidad:
    - Responder: ¿Son los datos transformados dimensionalmente íntegros antes de insertarlos en el DW?
    - Evaluar reglas:
      * RQ-03 (Critical): Unicidad de la clave de negocio 'track_id_bk' (sin duplicados).
      * RQ-06 (Critical): Integridad referencial: no nulos en llaves foráneas ('artist_id_sk', 'date_id_sk', 'genre_id_sk').
    - Detener el flujo si alguna regla falla (impide contaminar el Data Warehouse).
    
    Implementación de los 5 Bloques de Great Expectations:
    - Bloque 1: Data Context (efímero en memoria RAM).
    - Bloque 2: Data Source & Data Asset (Pandas batch definition).
    - Bloque 3: Expectation Suite (prepared_suite).
    - Bloque 4: Validation Definition (enlace estructural datos-suite).
    - Bloque 5: Checkpoint / Run (evaluación contra el DataFrame real).
    """
    print(f"[VAL PREPARED] Iniciando validación de datos preparados desde {prepared_path}...")
    df = pd.read_parquet(prepared_path)

    # -------------------------------------------------------------
    # [BLOQUE 1 GX: DATA CONTEXT] Administrador central en memoria RAM
    # -------------------------------------------------------------
    context = gx.get_context(mode="ephemeral")

    # -------------------------------------------------------------
    # [BLOQUE 2 GX: DATA SOURCE & DATA ASSET] Conexión al motor de datos
    # -------------------------------------------------------------
    data_source = context.data_sources.add_pandas("prepared_source")
    data_asset = data_source.add_dataframe_asset(name="prepared_asset")
    batch_definition = data_asset.add_batch_definition_whole_dataframe("prepared_batch")

    # -------------------------------------------------------------
    # [BLOQUE 3 GX: EXPECTATION SUITE] Reglas de calidad y severidades
    # -------------------------------------------------------------
    suite = context.suites.add(gx.ExpectationSuite(name="prepared_suite"))
    
    # Regla RQ-03: Unicidad estricta de canciones (Severidad: Critical)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeUnique(
            column="track_id_bk", meta={"rule_id": "RQ-03", "severity": "Critical"}
        )
    )
    
    # Regla RQ-06: Integridad referencial de dimensiones (Severidad: Critical)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="artist_id_sk", meta={"rule_id": "RQ-06", "severity": "Critical"}
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="date_id_sk", meta={"rule_id": "RQ-06", "severity": "Critical"}
        )
    )
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToNotBeNull(
            column="genre_id_sk", meta={"rule_id": "RQ-06", "severity": "Critical"}
        )
    )
    
    # Regla RQ-02: Conservación de rangos de métricas acústicas [0.0 - 1.0] (Severidad: Critical)
    for col in ['danceability', 'energy', 'valence']:
        suite.add_expectation(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=col, min_value=0.0, max_value=1.0, meta={"rule_id": "RQ-02", "severity": "Critical"}
            )
        )
        
    # Regla RQ-07: Dominio binario del indicador de premio [0, 1] (Severidad: Critical)
    suite.add_expectation(
        gx.expectations.ExpectColumnValuesToBeInSet(
            column="is_grammy_winner", value_set=[0, 1], meta={"rule_id": "RQ-07", "severity": "Critical"}
        )
    )

    # -------------------------------------------------------------
    # [BLOQUE 4 GX: VALIDATION DEFINITION] Enlace estructural
    # -------------------------------------------------------------
    val_def = context.validation_definitions.add(
        gx.ValidationDefinition(name="prepared_val_def", data=batch_definition, suite=suite)
    )

    # -------------------------------------------------------------
    # [BLOQUE 5 GX: CHECKPOINT / EJECUCIÓN] Evaluación en memoria
    # -------------------------------------------------------------
    val_result = val_def.run(batch_parameters={"dataframe": df})

    # Guardar reporte estructurado
    report_path = "/opt/airflow/docs/evidence/gx_prepared_results.json"
    save_gx_report(val_result, report_path)

    # Circuit Breaker: Evaluar severidad
    if not val_result.success:
        print("[VAL PREPARED] ¡Fallo de calidad detectado en compuerta de datos preparados!")
        for result in val_result.results:
            if not result.success:
                meta = result.expectation_config.meta
                print(f"  - Regla {meta.get('rule_id')} ({meta.get('severity')}) falló: {result.expectation_config.type}")
        raise ValueError("[CIRCUIT BREAKER] Validación de datos preparados falló en compuerta de calidad (RQ-03/RQ-06). Proceso detenido.")

    print("[VAL PREPARED] Compuerta preparada aprobada exitosamente. Datos dimensionalmente listos para DW.")
    return prepared_path
