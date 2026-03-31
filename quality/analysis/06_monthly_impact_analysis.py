#!/usr/bin/env python3
# pylint: disable=import-error
"""
Monthly Impact Analysis: validates data-shift behavior considering investment cycles

PURPOSE:
  Global monthly impact analysis. Shows which months have debit/credit
  imbalance and whether it is explained by investment cycles.

USAGE:
  conda run -n meltano python3 quality/analysis/06_monthly_impact_analysis.py

EXPECTED OUTPUT:
  Two sections:

  1. Investment/Retention Movements by Month
     Investment-like movements filtered by month:
     2024-05 | LETRA DE CAMBIO... | 1 ops | D:$100,000.00 C:$0.00

  2. Monthly Debit vs Credit (last 12 months)
     Monthly comparison table:
     2024-05 | 25 trans | D:$250,000 | C:$200,000 | Net:$50,000 ⚠️ DEBIT
     2024-06 | 22 trans | D:$180,000 | C:$280,000 | Net:-$100,000 ✓

  Net interpretation:
  • Positive (MORE DEBITS): month with more spending/investment outflow than income
  • Negative (MORE CREDITS): month with more income than spending
  • High (±$100k+): likely investment-cycle open/close behavior

HOW TO INTERPRET RESULTS:
  ⚠️ DEBIT (Net > threshold): month with expense-side imbalance
    Common valid cases:
    a) 6-month investment cycle started
    b) Tax withholding or commissions
    c) Real cash spending (non-investment)

  ✓ BALANCED: month without significant imbalance

DATA SHIFT interpretation:
  If Net > $100k in month A and Net < -$100k in month A+6:
  -> this is usually LEGITIMATE shift caused by investment cycles
  -> not necessarily an ETL issue

GUARDRAILS:
  • Run after scripts 01-05 for full context
  • Latest month may look incomplete when cycles are still open
  • Compare Net against investment movements for proper context
  • If Net is erratic without explanation, inspect silver transformations
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
    focus_month = sys.argv[2]
elif len(sys.argv) > 1:
    bank = sys.argv[1]
    focus_month = None
else:
    bank = "itau"
    focus_month = None

ALERT_THRESHOLD = 10000

print(f'\n{"="*70}')
print("MONTHLY IMPACT: Data Shift due to Multi-Month Cycles")
print(f'{"="*70}')
print(f"Bank: {bank}")
if focus_month:
    print(f"Focus month: {focus_month}")

# First, identify historical investment/retention-like movements in the target bank
q_all_investments = f"""
SELECT
  FORMAT_DATE('%Y-%m', transaction_date) as ym,
  description,
  COUNT(*) as cnt,
  SUM(COALESCE(debit_amount, 0) + COALESCE(credit_amount, 0)) as total_amt,
  SUM(COALESCE(debit_amount, 0)) as debit_amt,
  SUM(COALESCE(credit_amount, 0)) as credit_amt
FROM `finanzas-personales-457115.silver.stg_movimientos`
WHERE bank_code = '{bank}'
  AND (LOWER(description) LIKE '%inversion%'
    OR LOWER(description) LIKE '%reten%'
    OR LOWER(description) LIKE '%rescate%'
    OR LOWER(description) LIKE '%plazo%'
    OR LOWER(description) LIKE '%fondo%'
    OR LOWER(description) LIKE '%vto%letra%')
GROUP BY 1, 2
ORDER BY ym DESC
"""

print("\nInvestment/Retention Movements by Month:")
print("-" * 70)
results = list(client.query(q_all_investments).result())
if results:
    for row in results:
        print(
            f'{row["ym"]} | {row["description"][:40]:40} | {row["cnt"]:2} ops | D:${row["debit_amt"]:9,.2f} C:${row["credit_amt"]:9,.2f}'
        )
else:
    print("  (None found)")

# Now analyze cycle impact
# Find months where debit != credit (may represent partial cycles)
q_monthly_imbalance = f"""
SELECT
  FORMAT_DATE('%Y-%m', transaction_date) as ym,
  bank_code,
  COUNT(*) as transactions,
  SUM(COALESCE(debit_amount, 0)) as total_debit_all,
  SUM(COALESCE(credit_amount, 0)) as total_credit_all,
  SUM(CASE WHEN movement_type = 'expense' THEN COALESCE(debit_amount, 0) ELSE 0 END) as total_expense,
  SUM(CASE WHEN movement_type = 'income' THEN COALESCE(credit_amount, 0) ELSE 0 END) as total_income,
  ROUND(
    SUM(CASE WHEN movement_type = 'expense' THEN COALESCE(debit_amount, 0) ELSE 0 END)
    - SUM(CASE WHEN movement_type = 'income' THEN COALESCE(credit_amount, 0) ELSE 0 END),
    2
  ) as net_flow,
  SUM(CASE WHEN movement_type = 'internal_transfer' THEN COALESCE(debit_amount, 0) + COALESCE(credit_amount, 0) ELSE 0 END) as internal_transfer_amt
FROM `finanzas-personales-457115.silver.stg_movimientos`
WHERE bank_code = '{bank}'
GROUP BY 1, 2
ORDER BY 1 DESC
LIMIT 12
"""

print("\n\nMonthly Debit vs Credit (last 12 months):")
print("-" * 70)
print(
    f'{"Month":10} | {"Trans":7} | {"Expense":12} | {"Income":12} | {"Net":12} | {"Flag":10}'
)
print("-" * 70)
results = list(client.query(q_monthly_imbalance).result())
for row in results:
    imbalance = row["net_flow"]
    if imbalance > ALERT_THRESHOLD:
        status = "⚠️ DEBIT"
    elif imbalance < -ALERT_THRESHOLD:
        status = "⚠️ CREDIT"
    else:
        status = "✓ BALANCED"
    print(
        f'{row["ym"]:10} | {row["transactions"]:7} | ${row["total_expense"]:11,.2f} | ${row["total_income"]:11,.2f} | ${imbalance:11,.2f} | {status:10}'
    )

if focus_month:
    month_rows = [row for row in results if row["ym"] == focus_month]
    print("\n" + "=" * 70)
    print(f"FOCUS CHECK: {focus_month}")
    print("-" * 70)
    if month_rows:
        row = month_rows[0]
        print(f'  Transactions:            {row["transactions"]}')
        print(f'  Expense (movement_type): ${row["total_expense"]:,.2f}')
        print(f'  Income (movement_type):  ${row["total_income"]:,.2f}')
        print(f'  Net flow:                ${row["net_flow"]:,.2f}')
        print(
            f'  Internal transfer amt:   ${row["internal_transfer_amt"]:,.2f} (excluded from net flow)'
        )
        print(
            f'  Raw debit/credit (all):  D=${row["total_debit_all"]:,.2f} | C=${row["total_credit_all"]:,.2f}'
        )
    else:
        print("  No data for that month in the last-12-month output.")

print(f'\n{"="*70}')
print("INTERPRETATION:")
print("-" * 70)
print("If Net is positive (MORE DEBITS than CREDITS):")
print("  • It may be: an investment cycle that started, with return in a later month")
print("  • It may be: real spending without matching income")
print("  • It requires: individual movement analysis or bank-level verification")
print()
print("Data Shift = artificial monthly movement caused by multi-month financial cycles")
print("It does not necessarily indicate an ETL problem")
