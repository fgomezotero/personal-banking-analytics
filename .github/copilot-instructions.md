# Instrucciones de Copilot para este proyecto

## Contexto del proyecto

Este repositorio implementa un pipeline ETL/ELT con Meltano para consolidar movimientos bancarios personales desde archivos locales (CSV/XLS) hacia data warehouse analiticos OLAP BigQuery). El objetivo es unificar datos de diferentes bancos en un flujo reproducible que permita revisar localmente y cargar a BigQuery para analisis financiero posterior.

Objetivo operativo:

- Ingestar archivos bancarios locales (CSV/XLS).
- Validar salida local en JSONL antes de cargar a BigQuery.
- Cargar datos de fuentes a la capa bronze del data warehouse para analisis posterior.

Arquitectura de datos (regla del proyecto):

- Se adopta arquitectura medallion.
- La ingesta desde fuentes bancarias aterriza primero en el schema/dataset `bronze`.

Estado actual del workspace:

- El centro de configuracion es `meltano.yml`.
- No hay implementacion activa en `extract/`, `transform/`, `load/`, `orchestrate/`, `analyze/`, `notebook/` (solo placeholders con `.gitkeep`).
- Existen datos de Itau y Scotia en `data/`.
- Santander esta configurado, pero no hay carpeta con datos fuente (`data/santander/debito`).

## Fuentes y formatos

- Itau:
  - Origen: `data/itau/debito`
  - Formato: `.xls`
  - Tap: `tap-itau` (hereda de `tap-spreadsheets`)
  - Detalles clave: `format: excel`, `worksheet: Estado de Cuenta`, `skip_rows: 6`

- Scotia:
  - Origen: `data/scotia/debito`
  - Formato: `.csv`
  - Tap principal usado: `tap-scotia` (hereda de `tap-spreadsheets`)
  - Detalles clave: `table_name: scotia_debito`, `primary_keys: [Comprobante]`, `skip_rows: 0`

## Cargas y destinos

- Local:
  - Loader: `target-jsonl`
  - Salida: carpeta `output/`
  - Archivo timestamped cuando `do_timestamp_file: true`

- BigQuery (Arquitectura Medallion):
  - Loader: `target-bigquery`
  - Proyecto: `finanzas-personales-457115`
  - Dataset destino: `bronze` (capa de ingesta de fuentes)
  - Credenciales: `secrets/finanzas-personales.json`
  - Metodo: `batch_job`

## Convenciones de datos importantes

- Para `itau_debito`, se aplica `stream_maps` en `target-bigquery` para:
  - conservar columnas seleccionadas,
  - descartar no mapeadas (`__else__: __NULL__`),
  - filtrar conceptos de cierre/apertura (`SALDO ANTERIOR`, `SALDO FINAL`).

- En muestras de JSONL se observa metadata de procedencia tipo Singer (`_smart_source_file`, `_smart_source_lineno`).

## Comandos de trabajo recomendados

Regla obligatoria para agentes y colaboradores:

- Antes de ejecutar cualquier comando `meltano`, activar el entorno conda `meltano`.
- En automatizaciones o ejecuciones no interactivas, preferir `conda run -n meltano <comando>`.

Primera vez o cambio de entorno:

```bash
conda activate meltano
python -m pip install --upgrade pip
pip install meltano
meltano install --clean
```

Pruebas locales (salida JSONL):

```bash
conda run -n meltano meltano run tap-itau target-jsonl
conda run -n meltano meltano run tap-scotia target-jsonl
```

Carga a BigQuery:

```bash
conda run -n meltano meltano run tap-itau target-bigquery
conda run -n meltano meltano run tap-scotia target-bigquery
```

## Checklist de validacion despues de cambios

Al modificar configuracion o pipeline, validar siempre:

1. La ejecucion se realiza dentro del entorno conda `meltano`.
2. `meltano run ...` finaliza sin errores.
3. Se generan archivos en `output/` al usar `target-jsonl`.
4. Los campos esperados existen (`fecha`, `concepto`, `debito`/`credito`, `saldo`).
5. La metadata de origen sigue presente cuando corresponda.
6. La carga a BigQuery inserta filas nuevas en `bronze`.

## Validación y Scripts de Análisis

Después de cada carga a BigQuery, ejecutar scripts de validación en orden:

Validación rápida (después de carga inmediata):

```bash
conda run -n meltano python3 quality/analysis/01_validate_shift.py [banco] [YYYY-MM]
```

Auditoría completa (investigación de data shift o anomalías):

1. `01_validate_shift.py`: Reconciliación completa bronze→silver→gold
2. `02_audit_flow.py`: Auditar filtrado en cada transformación
3. `03_analyze_investment_cycles.py`: Detectar ciclos débito→crédito
4. `04_monthly_investment_impact.py`: Impacto mensual de ciclos 6M
5. `05_deep_investment_analysis.py`: Análisis profundo sin restricción temporal
6. `06_monthly_impact_analysis.py`: Decisión final sobre data shift legítimo

Documentación completa: ver [quality/analysis/README.md](../quality/analysis/README.md)

Reglas para scripts de análisis:

- Todos los scripts usan sandbox de lectura BigQuery (no modifican datos)
- Todos requieren `secrets/finanzas-personales.json` accesible en raíz
- Ejecutar siempre con `conda run -n meltano` para consistencia
- No están diseñados para automatización diaria; son herramientas de investigación

## Guardrails para colaboradores y agentes

- Priorizar cambios en `meltano.yml` y documentacion antes que crear scripts innecesarios.
- No ejecutar `meltano` fuera del entorno conda `meltano`.
- No modificar archivos de `data/` ni `output/` salvo solicitud explicita del usuario.
- No exponer ni copiar secretos en respuestas o logs.
- No imprimir contenido de `secrets/finanzas-personales.json`.
- Si una tarea requiere Santander, confirmar primero existencia de `data/santander/debito`.
- Mantener README y estas instrucciones alineadas cuando cambie el flujo.
- Los scripts en `quality/analysis/` están organizados para ejecución secuencial (01→06); no modificarlos ni crear duplicados.

## Errores comunes y diagnostico rapido

- No se procesan archivos:
  - Verificar ruta, extension y `skip_initial`.

- Problemas con BigQuery:
  - Revisar `credentials_path`, `project` y `dataset` en `meltano.yml`.

- Diferencias de esquema:
  - Revisar `field_names` en taps CSV y mapeo `stream_maps` en el target.

## Alcance de futuras implementaciones

Si se agregan scripts en `extract/`, `transform/`, `load/`, `orchestrate/` o notebooks:

- preferir funciones pequenas y testeables,
- mantener separacion entre ingesta, limpieza y analisis,
- conservar Meltano como orquestador primario mientras no se defina otro scheduler.
