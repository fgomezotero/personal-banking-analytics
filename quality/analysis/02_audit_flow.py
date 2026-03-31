#!/usr/bin/env python3
# pylint: disable=import-error,broad-exception-caught
"""
Data Flow Audit: Muestra qué se filtra en cada fase Bronze->Silver->Gold

PROPOSITO:
  Audita el flujo de datos real en cada capa medallion.
  Calcula cuántas filas se pierden en cada transformación y por qué.

USO:
  conda run -n meltano python3 quality/analysis/02_audit_flow.py [banco] [YYYY-MM]

EJEMPLOS:
  # Auditar Scotia octubre 2024
  conda run -n meltano python3 quality/analysis/02_audit_flow.py scotia 2024-10

  # Auditar Itaú mayo 2024
  conda run -n meltano python3 quality/analysis/02_audit_flow.py itau 2024-05

OUTPUT ESPERADO:
  Visualización en cascada del pipeline:
  BRONZE (X filas)
    ↓ Parsing + Filter
  SILVER (Y filas) [OK|LOSE]
    ↓ Deduplication
  FACT (Z filas) [OK|LOSE]
    ↓ Aggregation
  GOLD (N filas)

COMO INTERPRETAR RESULTADOS:
  [OK] = No se perdieron filas en esa etapa (esperado)
  [LOSE] = Se filtraron filas (normal; detalles debajo)

  % de filtrado: Porcentaje de filas descartadas
  Common causes:
  - Bronze→Silver: Filas vacías vs schema, tipos inválidos
  - Silver→Fact: Deduplicación de movimientos duplicados e ingestion
  - Fact→Gold: Agregación (esperado: muchas filas se agrupan en pocas)

  ⚠️  Si LOSE > 10% sin justificación, investigar stream_maps o dbt transforms

GUARDRAILS:
  • Esperar pérdida de filas en Fact→Gold (es agregación)
  • Si Bronze→Silver tiene LOSE alto, revisar meltano.yml stream_maps
  • Si Silver→Fact tiene LOSE alto, revisar dbt deduplicación
  • Ejecutar después de 01_validate_shift.py para contexto
"""
from google.cloud import bigquery
import json
import sys

with open("secrets/finanzas-personales.json", encoding="utf-8") as f:
    creds = json.load(f)

client = bigquery.Client.from_service_account_info(
    creds, project="finanzas-personales-457115"
)

if len(sys.argv) > 2:
    bank = sys.argv[1]
    year_month = sys.argv[2]
else:
    bank = "itau"
    year_month = "2024-05"

print(f'\n{"="*70}')
print(f"DATA FLOW AUDIT: {bank.upper()} - {year_month}")
print(f'{"="*70}')

try:
    # Mapeo de tabla por banco
    bronze_table = {
        "itau": "itau_debito",
        "scotia": "scotia_debito",
        "bbva": "bbva_debito",
    }.get(bank)

    if not bronze_table:
        print(f'ERROR: Banco "{bank}" no soportado')
        sys.exit(1)

    # BRONZE
    q1 = f"""SELECT COUNT(*) as total FROM `finanzas-personales-457115.bronze.{bronze_table}` WHERE FORMAT_DATE("%Y-%m", PARSE_DATE("%d/%m/%Y", fecha)) = "{year_month}" """
    r1 = list(client.query(q1).result())[0]
    bronze_count = r1["total"]

    # SILVER
    q2 = f"""SELECT COUNT(*) as total FROM `finanzas-personales-457115.silver.stg_movimientos` WHERE bank_code = "{bank}" AND FORMAT_DATE("%Y-%m", transaction_date) = "{year_month}" """
    r2 = list(client.query(q2).result())[0]
    silver_count = r2["total"]

    # FACT (pre-agregacion)
    q3 = f"""SELECT COUNT(*) as total FROM `finanzas-personales-457115.gold.fact_transactions` WHERE bank_code = "{bank}" AND FORMAT_DATE("%Y-%m", transaction_date) = "{year_month}" """
    r3 = list(client.query(q3).result())[0]
    fact_count = r3["total"]

    # GOLD
    q4 = f"""SELECT COALESCE(movements_count, 0) as total FROM `finanzas-personales-457115.gold.agg_monthly_cashflow` WHERE bank_code = "{bank}" AND year_month = "{year_month}" """
    r4 = list(client.query(q4).result())
    gold_count = r4[0]["total"] if r4 else 0

    print(f"\nBRONZE ({bronze_table}):          {bronze_count:5} rows")
    print("  |  (Parsing + Filter)    ")

    status1 = "OK" if silver_count == bronze_count else "LOSE"
    filtered1 = bronze_count - silver_count if bronze_count > 0 else 0
    pct1 = 100 * filtered1 / bronze_count if bronze_count > 0 else 0

    print(f"SILVER (stg_movimientos):   {silver_count:5} rows [{status1}]")
    if bronze_count > 0 and filtered1 > 0:
        print(f"  {filtered1} rows filtered ({pct1:.1f}%)")

    print("  |  (Deduplication)       ")

    status2 = "OK" if fact_count == silver_count else "LOSE"
    filtered2 = silver_count - fact_count if silver_count > 0 else 0
    pct2 = 100 * filtered2 / silver_count if silver_count > 0 else 0

    print(f"FACT (fact_transactions):   {fact_count:5} rows [{status2}]")
    if silver_count > 0 and filtered2 > 0:
        print(f"  {filtered2} rows filtered ({pct2:.1f}%)")

    print("  |  (Aggregation)         ")
    print(f"GOLD (agg_monthly):         {gold_count:5} rows")

    if bronze_count != silver_count or silver_count != fact_count:
        print("\nWARNING: Data loss detected in pipeline")
        if bronze_count != silver_count:
            print(f"  - Bronze to Silver: {filtered1} rows lost ({pct1:.1f}%)")
        if silver_count != fact_count:
            print(f"  - Silver to Fact: {filtered2} rows lost ({pct2:.1f}%)")

except Exception as e:
    print(f"\nERROR: {e}")
    sys.exit(3)
