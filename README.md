# Finanzas Personales - Pipeline de Ingesta Bancaria

Proyecto ETL/ELT con Meltano para consolidar movimientos bancarios desde archivos locales (CSV/XLS) hacia BigQuery.

## Objetivo

Unificar movimientos de cuentas bancarias personales en un flujo reproducible para:

- validar localmente la ingesta en JSONL,
- cargar datos fuente al data warehouse,
- habilitar analisis financiero posterior.

## Arquitectura de datos

- Arquitectura medallion.
- La ingesta aterriza en la capa `bronze`.
- Dataset actual en BigQuery: `bronze`.

## Fuente de verdad de configuracion

Toda la configuracion del pipeline vive en `meltano.yml`.

## Fuentes configuradas

- Itau (`tap-itau`)
  - Origen: `data/itau/debito/debito_xlsx/*.xlsx`
  - Tipo: excel
  - Hoja: `Estado de Cuenta`
  - `skip_rows: 6`
  - Nota: los `.xls` se convierten a `.xlsx` con `extract/convert_itau_xls_to_xlsx.py`.

- Scotia (`tap-scotia`)
  - Origen: `data/scotia/debito/*.csv`
  - Tipo: csv
  - Tabla: `scotia_debito`
  - `skip_rows: 0`

- BBVA (`tap-bbva`)
  - Origen: `data/bbva/debito/*.csv`
  - Tipo: csv
  - Tabla: `bbva_debito`
  - `skip_rows: 5`

## Destinos configurados

- Local (`target-jsonl`)
  - Salida: `output/`
  - Archivos timestamped (`do_timestamp_file: true`)

- BigQuery (`target-bigquery`, variant `z3z1ma`)
  - Proyecto: `finanzas-personales-457115`
  - Dataset: `bronze`
  - Metodo: `batch_job`
  - Credenciales: `secrets/finanzas-personales.json`
  - Estrategia de recarga por stream: `overwrite` para `scotia_debito`, `itau_debito` y `bbva_debito`

## Reglas de transformacion (stream_maps)

- `itau_debito`: filtra filas de saldo (`SALDO ANTERIOR` y `SALDO FINAL`).
- `bbva_debito`: filtra filas vacias del CSV (`fecha is not None`).

## Requisitos de ejecucion

Usar siempre el entorno conda `meltano` antes de ejecutar Meltano.

- Interactivo: `conda activate meltano`
- No interactivo: `conda run -n meltano <comando>`

## Setup inicial

```bash
conda activate meltano
python -m pip install --upgrade pip
pip install -r requirements.txt
meltano install --clean
```

## Flujo recomendado (JSONL primero)

1. Probar extraccion local con JSONL:

```bash
conda run -n meltano meltano run tap-itau target-jsonl
conda run -n meltano meltano run tap-scotia target-jsonl
conda run -n meltano meltano run tap-bbva target-jsonl
```

1. Validar rapidamente archivos en `output/`.

2. Cargar a BigQuery:

```bash
conda run -n meltano meltano run pre-itau:convert_xls tap-itau target-bigquery
conda run -n meltano meltano run tap-scotia target-bigquery
conda run -n meltano meltano run tap-bbva target-bigquery
```

## Checklist de validacion

1. La corrida se hace dentro del entorno `meltano`.
2. `meltano run ...` termina sin errores.
3. En pruebas locales se generan JSONL en `output/`.
4. Campos esperados presentes (`fecha`, `concepto`, `debito`/`credito`, `saldo` segun banco).
5. Hay filas en BigQuery despues de la carga.

## Guardrails

- No ejecutar Meltano fuera del entorno conda `meltano`.
- No modificar `data/` ni `output/` salvo solicitud explicita.
- No exponer secretos ni imprimir contenido de `secrets/finanzas-personales.json`.
- Priorizar cambios en `meltano.yml` y documentacion antes de agregar scripts nuevos.
