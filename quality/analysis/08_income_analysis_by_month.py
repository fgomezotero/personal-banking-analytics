#!/usr/bin/env python3
# pylint: disable=import-error,broad-exception-caught
"""
Income Analysis by Month: analiza los incomes de un mes especifico

PROPOSITO:
    Devuelve el resumen de incomes para un YYYY-MM dado.
    Incluye total del mes, distribucion por banco y top descripciones.

USO:
    conda run -n meltano python3 quality/analysis/08_income_analysis_by_month.py [YYYY-MM]

EJEMPLOS:
    conda run -n meltano python3 quality/analysis/08_income_analysis_by_month.py 2025-04
"""
from google.cloud import bigquery
import json
import sys

with open("secrets/finanzas-personales.json", encoding="utf-8") as file_handle:
    creds = json.load(file_handle)

client = bigquery.Client.from_service_account_info(
    creds, project="finanzas-personales-457115"
)

if len(sys.argv) != 2:
    print(
        "Uso: conda run -n meltano python3 quality/analysis/08_income_analysis_by_month.py YYYY-MM"
    )
    sys.exit(2)

year_month = sys.argv[1]

print(f'\n{"="*86}')
print(f"INCOME ANALYSIS BY MONTH: {year_month}")
print(f'{"="*86}')

summary_query = f"""
SELECT
  COUNT(*) AS income_rows,
  SUM(income_amount) AS total_income,
  COUNT(DISTINCT bank_code) AS banks,
  COUNT(DISTINCT description) AS descriptions
FROM `finanzas-personales-457115.gold.fact_transactions`
WHERE movement_type = 'income'
  AND FORMAT_DATE('%Y-%m', transaction_date) = '{year_month}'
"""

by_bank_query = f"""
SELECT
  bank_code,
  COUNT(*) AS rows_count,
  SUM(income_amount) AS total_income
FROM `finanzas-personales-457115.gold.fact_transactions`
WHERE movement_type = 'income'
  AND FORMAT_DATE('%Y-%m', transaction_date) = '{year_month}'
GROUP BY bank_code
ORDER BY total_income DESC
"""

by_description_query = f"""
SELECT
  description,
  COUNT(*) AS rows_count,
  SUM(income_amount) AS total_income
FROM `finanzas-personales-457115.gold.fact_transactions`
WHERE movement_type = 'income'
  AND FORMAT_DATE('%Y-%m', transaction_date) = '{year_month}'
GROUP BY description
ORDER BY total_income DESC
LIMIT 10
"""

try:
    summary_rows = list(client.query(summary_query).result())
    if not summary_rows:
        print("\n(No se pudo obtener resumen)")
        sys.exit(3)

    summary = summary_rows[0]
    income_rows = int(summary["income_rows"] or 0)
    total_income = float(summary["total_income"] or 0)
    bank_count = int(summary["banks"] or 0)
    description_count = int(summary["descriptions"] or 0)

    if income_rows == 0:
        print("\n(No hay incomes para el mes solicitado)")
        sys.exit(0)

    print(f"\nIncome rows:         {income_rows}")
    print(f"Total income:        ${total_income:,.2f}")
    print(f"Banks involved:      {bank_count}")
    print(f"Descriptions:        {description_count}")

    bank_rows = list(client.query(by_bank_query).result())
    print(f'\n{"="*86}')
    print("BY BANK")
    print(f'{"="*86}')
    print("Bank       | Rows  | Total income")
    print("-" * 40)
    for row in bank_rows:
        print(
            f'{row["bank_code"]:10} | {int(row["rows_count"] or 0):5} | ${float(row["total_income"] or 0):12,.2f}'
        )

    description_rows = list(client.query(by_description_query).result())
    print(f'\n{"="*86}')
    print("TOP DESCRIPTIONS")
    print(f'{"="*86}')
    print("Total income   | Rows  | Description")
    print("-" * 86)
    for row in description_rows:
        description = (row["description"] or "")[:50]
        print(
            f'${float(row["total_income"] or 0):12,.2f} | {int(row["rows_count"] or 0):5} | {description}'
        )

    sys.exit(0)

except Exception as error:
    print(f"\n❌ Error: {error}")
    sys.exit(3)
