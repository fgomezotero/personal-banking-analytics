# Finanzas Personales - Pipeline de Ingesta Bancaria

Proyecto ETL/ELT con Meltano para consolidar movimientos bancarios desde archivos locales (CSV/XLS) hacia BigQuery.

## Objetivo

Unificar movimientos de cuentas bancarias personales en un flujo reproducible para:

- validar localmente la ingesta en JSONL,
- cargar datos fuente al data warehouse,
- habilitar analisis financiero posterior.

## Arquitectura de datos

- Se adopta arquitectura medallion.
- La ingesta desde fuentes bancarias aterriza primero en el dataset `bronze`.
- Capa actual implementada: `bronze`.

## Estado actual del proyecto

- El centro de configuracion es `meltano.yml`.
- No hay implementacion activa en `extract/`, `transform/`, `load/`, `orchestrate/`, `analyze/`, `notebook/` (solo `.gitkeep`).
- Existen datos fuente para Itau y Scotia en `data/`.
- Santander esta configurado en Meltano, pero no tiene datos cargados en `data/santander/debito`.

## Fuentes configuradas

- Itau:
  - Ruta: `data/itau/debito`
  - Formato: `.xls`
 	- Tap: `tap-itau` (via `tap-spreadsheets`)
 	- Config relevante: `format: excel`, `worksheet: Estado de Cuenta`, `skip_rows: 6`

- Scotia:
  - Ruta: `data/scotia/debito`
  - Formato: `.csv`
 	- Tap: `tap-scotia` (via `tap-spreadsheets`)
 	- Config relevante: `table_name: scotia_debito`, `primary_keys: [Comprobante]`, `skip_rows: 0`

## Destinos configurados

- Local:
  - Loader: `target-jsonl`
  - Carpeta de salida: `output/`
  - Nombre de archivo con timestamp (`do_timestamp_file: true`)

- BigQuery:
  - Loader: `target-bigquery`
  - Project: `finanzas-personales-457115`
  - Dataset: `bronze`
  - Metodo: `batch_job`
  - Credenciales: `secrets/finanzas-personales.json`

## Convenciones de transformacion

Para `itau_debito`, `target-bigquery` aplica `stream_maps` para:

- conservar columnas seleccionadas,
- descartar columnas no mapeadas (`__else__: __NULL__`),
- filtrar registros de saldo de apertura/cierre (`SALDO ANTERIOR`, `SALDO FINAL`).

La salida JSONL preserva metadata de origen Singer, por ejemplo:

- `_smart_source_file`
- `_smart_source_lineno`

## Regla obligatoria de ejecucion

Antes de ejecutar cualquier comando de Meltano, usar el entorno conda `meltano`.

- En sesiones interactivas: `conda activate meltano`
- En ejecuciones no interactivas: `conda run -n meltano <comando>`

## Setup inicial

```bash
conda activate meltano
python -m pip install --upgrade pip
pip install meltano
meltano install --clean
```

## Ejecucion rapida

1. Validar fuentes

- `data/itau/debito`
- `data/scotia/debito`

1. Prueba local (JSONL)

```bash
conda run -n meltano meltano run tap-itau target-jsonl
conda run -n meltano meltano run tap-scotia target-jsonl
```

1. Carga a BigQuery (bronze)

```bash
conda run -n meltano meltano run tap-itau target-bigquery
conda run -n meltano meltano run tap-scotia target-bigquery
```

## Checklist de validacion por corrida

1. La ejecucion se realiza en el entorno conda `meltano`.
2. `meltano run ...` finaliza sin errores.
3. Se genera archivo nuevo en `output/` con `target-jsonl`.
4. Campos esperados presentes: `fecha`, `concepto`, `debito`/`credito`, `saldo`.
5. Se mantiene metadata de origen (`_smart_source_file`, `_smart_source_lineno`).
6. Se insertan filas nuevas en BigQuery dataset `bronze`.

## Troubleshooting rapido

1. No se procesan archivos

- Verificar ruta, extension y `skip_initial`.

1. Problemas con BigQuery

- Revisar `credentials_path`, `project` y `dataset` en `meltano.yml`.

1. Diferencias de esquema

- Revisar `field_names` en taps CSV y mapeo `stream_maps` del target.

## Guardrails

- Priorizar cambios en `meltano.yml` y documentacion antes de crear scripts.
- No ejecutar Meltano fuera del entorno conda `meltano`.
- No modificar `data/` ni `output/` salvo solicitud explicita.
- No exponer ni copiar secretos en logs o respuestas.
- Si se trabaja con Santander, confirmar primero existencia de `data/santander/debito`.

## Runbook diario (5 minutos)

1. Copiar nuevos estados de cuenta a `data/itau/debito` y/o `data/scotia/debito`.
2. Ejecutar prueba local con `target-jsonl`.
3. Revisar rapidamente 20-30 filas en `output/`.
4. Ejecutar carga a BigQuery (`bronze`).
5. Verificar filas nuevas y cerrar corrida.
