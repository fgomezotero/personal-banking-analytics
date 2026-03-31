# DBT Transform Layer

Este directorio contiene la capa de transformacion analitica (silver y gold) sobre los datos ingestados en BigQuery desde Meltano.

## Objetivo

- Estandarizar movimientos bancarios en un modelo canonico (`silver.stg_movimientos`).
- Construir tablas analiticas (`gold`) para reporting y KPIs:
  - `gold.dim_bank`
  - `gold.dim_date`
  - `gold.fact_transactions`
  - `gold.agg_monthly_cashflow`
  - `gold.agg_monthly_cashflow_total`

## Requisitos

- Conda disponible en el sistema.
- Entorno conda `meltano` creado.
- Dependencias Python instaladas (incluye dbt):
  - `dbt-core==1.8.8`
  - `dbt-bigquery==1.8.3`
- Credenciales de BigQuery en:
  - `secrets/finanzas-personales.json`

## Estructura importante

- `dbt_project.yml`: configuracion del proyecto dbt.
- `profiles.yml`: conexion BigQuery para este workspace.
- `macros/`: utilidades SQL reutilizables (`parse_amount`, `parse_mixed_date`, etc.).
- `models/sources.yml`: definicion de fuentes bronze.
- `models/silver/`: capa de estandarizacion.
- `models/gold/`: capas dimensionales y agregados de negocio.
- `models/schema.yml`: tests de calidad.

## Regla de schemas

El proyecto define un override en `macros/generate_schema_name.sql` para evitar el comportamiento por defecto de dbt en BigQuery (concatenar schema base + custom schema).

Resultado esperado:

- modelos silver -> dataset `silver`
- modelos gold -> dataset `gold`

## Comandos base

Ejecutar siempre dentro del entorno conda `meltano`.

### 1. Validar conexion

```bash
conda run -n meltano dbt debug --project-dir transform --profiles-dir transform
```

### 2. Ejecutar todos los modelos

```bash
conda run -n meltano dbt run --project-dir transform --profiles-dir transform
```

### 3. Ejecutar modelos seleccionados

```bash
conda run -n meltano dbt run --project-dir transform --profiles-dir transform --select stg_movimientos
conda run -n meltano dbt run --project-dir transform --profiles-dir transform --select fact_transactions
```

### 4. Ejecutar tests

```bash
conda run -n meltano dbt test --project-dir transform --profiles-dir transform
```

### 5. Build completo (run + test)

```bash
conda run -n meltano dbt build --project-dir transform --profiles-dir transform
```

## Documentacion y linaje

### Generar artefactos de docs

```bash
conda run -n meltano dbt docs generate --project-dir transform --profiles-dir transform
```

Esto genera (entre otros):

- `target/index.html`
- `target/manifest.json`
- `target/catalog.json`

### Servir docs localmente

```bash
conda run -n meltano dbt docs serve --project-dir transform --profiles-dir transform --port 8081 --no-browser
```

Abrir en navegador:

- `http://127.0.0.1:8081`

## Fuentes esperadas en BigQuery

Dataset `bronze`:

- `itau_debito`
- `scotia_debito`
- `bbva_debito`

Si falta una fuente, `dbt run` puede fallar al resolver `source()`.

## Calidad de datos cubierta por tests

- `not_null` en claves y columnas criticas.
- `unique` en dimensiones y `transaction_id`.
- `relationships` de `fact_transactions` con `dim_bank` y `dim_date`.
- `accepted_values` para `bank_code` y `movement_type`.

## Troubleshooting rapido

### Error de credenciales

Verificar `keyfile` en `profiles.yml`:

- `secrets/finanzas-personales.json`

### Modelos en schema no esperado

Verificar existencia y contenido de:

- `macros/generate_schema_name.sql`

### Cero filas en salida

Revisar:

- parsing de fechas (`parse_mixed_date`)
- parsing de montos (`parse_amount`)
- que las tablas bronze tengan datos actuales

## Buenas practicas operativas

- No commitear `target/`, `logs/`, `dbt_packages/` ni `.user.yml`.
- No exponer el contenido de credenciales en logs o docs.
- Ejecutar primero `dbt debug` ante cambios de entorno.
- Usar `dbt build` antes de merge para validar transformaciones y tests.
