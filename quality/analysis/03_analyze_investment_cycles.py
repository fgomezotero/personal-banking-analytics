#!/usr/bin/env python3
# pylint: disable=import-error
"""
Investment Cycle Analyzer: detects retention-recovery cycles in Itau

PURPOSE:
  Identifies investment/retention movements and their corresponding recoveries.
  Understanding complete cycles (debit->credit) helps explain monthly data shift.

USAGE:
  conda run -n meltano python3 quality/analysis/03_analyze_investment_cycles.py

EXPECTED OUTPUT:
  1. Description list of investment-like movements found
  2. Closed cycles list (debit + corresponding credit)
  3. Debits without visible recovery yet

  Example:
  ✅ 2024-05-01 -> 2024-11-01 (184 days): $100,000.00
     Debit:  LETRA DE CAMBIO 6M
     Credit: RESCATE LETRA...

  ❌ 2024-06-15 (without recovery): $50,000.00
     Debit:  RETENCION RETORNO

HOW TO INTERPRET RESULTS:
  ✅ Complete cycles: normal; they explain month-level debit/credit differences
  ❌ Debits without recovery: they may be:
     a) Open cycles (recovery expected in a future month)
     b) Real spending movements (not retention)
     c) Fees or manual adjustments

  If >50% have no recovery, investigate manual movements or adjustments

GUARDRAILS:
  • Run after scripts 01 and 02 for context
  • Cycles > 180 days are truncated (see script 05 for flexible cycles)
  • This script only analyzes Itau (other banks may not include investments)
  • If recoveries are in the future, see 04_monthly_investment_impact.py
"""
from google.cloud import bigquery
import json

with open("secrets/finanzas-personales.json", encoding="utf-8") as f:
    creds = json.load(f)

client = bigquery.Client.from_service_account_info(
    creds, project="finanzas-personales-457115"
)

print(f'\n{"="*70}')
print("INVESTMENT CYCLE ANALYZER: ITAU")
print(f'{"="*70}')

# Find investment/retention-like movements
q_concepts = """
SELECT DISTINCT description
FROM `finanzas-personales-457115.silver.stg_movimientos`
WHERE bank_code = 'itau'
  AND (LOWER(description) LIKE '%inversion%'
    OR LOWER(description) LIKE '%reten%'
    OR LOWER(description) LIKE '%rescate%'
    OR LOWER(description) LIKE '%plazo%'
    OR LOWER(description) LIKE '%fondo%'
    OR LOWER(description) LIKE '%renta%')
ORDER BY description
"""

print("\nDescriptions found in Itau (potential investments):")
print("-" * 70)
results = list(client.query(q_concepts).result())
if results:
    for row in results:
        print(f'  • {row["description"]}')
else:
    print("  (None found)")

# Complete-cycle analysis: match debit and credit with similar amount
q_cycles = """
WITH itau_movements AS (
  SELECT
    transaction_date,
    description,
    COALESCE(debit_amount, 0) as debit_amt,
    COALESCE(credit_amount, 0) as credit_amt,
    DATE_DIFF(CURRENT_DATE(), transaction_date, DAY) as days_ago
  FROM `finanzas-personales-457115.silver.stg_movimientos`
  WHERE bank_code = 'itau'
)
SELECT
  debit_mov.transaction_date as debit_date,
  debit_mov.description as debit_desc,
  debit_mov.debit_amt as amount,
  credit_mov.transaction_date as credit_date,
  credit_mov.description as credit_desc,
  DATE_DIFF(credit_mov.transaction_date, debit_mov.transaction_date, DAY) as cycle_days
FROM itau_movements debit_mov
LEFT JOIN itau_movements credit_mov
  ON ABS(debit_mov.debit_amt - credit_mov.credit_amt) < 0.01
  AND credit_mov.transaction_date >= debit_mov.transaction_date
  AND DATE_DIFF(credit_mov.transaction_date, debit_mov.transaction_date, DAY) BETWEEN 0 AND 180
WHERE debit_mov.debit_amt > 0
ORDER BY debit_mov.transaction_date DESC
LIMIT 20
"""

print("\n\nInvestment-Recovery Cycles (latest 20):")
print("-" * 70)
results = list(client.query(q_cycles).result())

matched = 0
unmatched = 0

for row in results:
    if row["credit_date"]:
        matched += 1
        print(
            f'✅ {row["debit_date"].strftime("%Y-%m-%d")} -> {row["credit_date"].strftime("%Y-%m-%d")} ({row["cycle_days"]:3d} days): ${row["amount"]:,.2f}'
        )
        print(f'   Debit:   {row["debit_desc"][:50]}')
        print(f'   Credit:  {row["credit_desc"][:50]}')
    else:
        unmatched += 1
        print(
            f'❌ {row["debit_date"].strftime("%Y-%m-%d")} (without recovery):           ${row["amount"]:,.2f}'
        )
        print(f'   Debit:   {row["debit_desc"][:50]}')

print(f'\n{"="*70}')
print("SUMMARY:")
print(f"  Complete cycles (debit->credit): {matched}")
print(f"  Debits without recovery:         {unmatched}")
print(
    "\nNOTE: This analysis explains legitimate retentions that impact month-to-month balances."
)
print('      If you have long cycles (>90 days), you will see "unbalanced" months.')
