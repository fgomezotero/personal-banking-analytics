# DBT Transform Layer

This directory contains the analytical transformation layer (silver and gold) on top of data ingested into BigQuery from Meltano.

## Goal

- Standardize banking movements in a canonical model (`silver.stg_movimientos`).
- Build analytical tables (`gold`) for reporting and KPIs:
  - `gold.dim_bank`
  - `gold.dim_date`
  - `gold.fact_transactions`
  - `gold.agg_monthly_cashflow`
  - `gold.agg_monthly_cashflow_total`

## Requirements

- Conda available on the system.
- `meltano` conda environment created.
- Python dependencies installed (including dbt):
  - `dbt-core==1.8.8`
  - `dbt-bigquery==1.8.3`
- BigQuery credentials in:
  - `secrets/finanzas-personales.json`

## Important Structure

- `dbt_project.yml`: dbt project configuration.
- `profiles.yml`: BigQuery connection for this workspace.
- `macros/`: reusable SQL utilities (`parse_amount`, `parse_mixed_date`, etc.).
- `models/sources.yml`: bronze source definitions.
- `models/silver/`: standardization layer.
- `models/gold/`: dimensional and business aggregate layers.
- `models/schema.yml`: data-quality tests.

## Schema Rule

The project defines an override in `macros/generate_schema_name.sql` to avoid dbt's default BigQuery behavior (base schema + custom schema concatenation).

Expected result:

- silver models -> dataset `silver`
- gold models -> dataset `gold`

## Core Commands

Always run inside the `meltano` conda environment.

### 1. Validate connection

```bash
conda run -n meltano dbt debug --project-dir transform --profiles-dir transform
```

### 2. Run all models

```bash
conda run -n meltano dbt run --project-dir transform --profiles-dir transform
```

### 3. Run selected models

```bash
conda run -n meltano dbt run --project-dir transform --profiles-dir transform --select stg_movimientos
conda run -n meltano dbt run --project-dir transform --profiles-dir transform --select fact_transactions
```

### 4. Run tests

```bash
conda run -n meltano dbt test --project-dir transform --profiles-dir transform
```

### 5. Full build (run + test)

```bash
conda run -n meltano dbt build --project-dir transform --profiles-dir transform
```

## Documentation and Lineage

### Generate docs artifacts

```bash
conda run -n meltano dbt docs generate --project-dir transform --profiles-dir transform
```

This generates (among others):

- `target/index.html`
- `target/manifest.json`
- `target/catalog.json`

### Serve docs locally

```bash
conda run -n meltano dbt docs serve --project-dir transform --profiles-dir transform --port 8081 --no-browser
```

Open in browser:

- `http://127.0.0.1:8081`

## Expected BigQuery Sources

Dataset `bronze`:

- `itau_debito`
- `scotia_debito`
- `bbva_debito`

If a source is missing, `dbt run` may fail while resolving `source()`.

## Data Quality Covered by Tests

- `not_null` on keys and critical columns.
- `unique` on dimensions and `transaction_id`.
- `relationships` from `fact_transactions` to `dim_bank` and `dim_date`.
- `accepted_values` for `bank_code` and `movement_type`.

## Quick Troubleshooting

### Credential error

Verify `keyfile` in `profiles.yml`:

- `secrets/finanzas-personales.json`

### Models in unexpected schema

Verify existence and content of:

- `macros/generate_schema_name.sql`

### Zero rows in output

Check:

- date parsing (`parse_mixed_date`)
- amount parsing (`parse_amount`)
- that bronze tables contain current data

## Operational Best Practices

- Do not commit `target/`, `logs/`, `dbt_packages/`, or `.user.yml`.
- Do not expose credential contents in logs or docs.
- Run `dbt debug` first after environment changes.
- Use `dbt build` before merge to validate transformations and tests.
