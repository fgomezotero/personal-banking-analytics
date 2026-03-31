#!/usr/bin/env python3
# pylint: disable=import-error
"""
Deep Investment Analysis: searches for complete historical cycles (6+ months)
Includes tolerance for commissions and interest

PURPOSE:
  Deep investment-cycle analysis without strict time limits.
  Detects retention/recovery patterns even when cycles remain open or exceed 6 months.

USAGE:
  conda run -n meltano python3 quality/analysis/05_deep_investment_analysis.py

EXPECTED OUTPUT:
  Three analysis levels:

  1. UNIQUE DESCRIPTIONS: all investment-like concepts found
     Shows frequency and debit vs credit amounts by concept

  2. 6-MONTH CYCLES: strict cycles (180±30 days) with commission tolerance
    Example:
     ✓ 2024-05-01 → 2024-11-01 (184 days)
       Debit:  $100,000.00 | LETRA DE CAMBIO 6M
       Credit: $101,234.00 | RESCATE LETRA...
       Commission/diff: 1.23%

  3. ANY CLOSED CYCLES: flexible debit-credit cycles (no day restriction)
     Looks for any debit with a corresponding credit
     ✓ Matched: closed cycle (recovery received)
     ✗ Unmatched: debit without visible credit

HOW TO INTERPRET RESULTS:
  Commission/diff > 5%: probably not the real cycle (search for another match)
  Commission/diff 0-5%: normal interest or fees
  Commission/diff < 0%: recovery with loss (option exercise, etc.)

  Investment keywords high frequency: most common movement type

  If unmatched_count > 30%: there are still open cycles or no visible recovery yet

GUARDRAILS:
  • This is exploratory analysis (no strict time restrictions)
  • It does not infer data-integrity issues; it only shows patterns
  • A debit without credit ≠ error; it may be real spending, fee, or partial reimbursement
  • Run after 04 for full context

NEXT STEPS:
  If there are many unmatched entries:
  1. Run script 06 (monthly impact) for month-by-month global impact
  2. Manually verify cycles after 6 months (12M letters, term funds)
  3. Investigate manual vs automated retention management
"""
from google.cloud import bigquery
import json

with open("secrets/finanzas-personales.json", encoding="utf-8") as f:
    creds = json.load(f)

client = bigquery.Client.from_service_account_info(
    creds, project="finanzas-personales-457115"
)

print(f'\n{"="*80}')
print("DEEP INVESTMENT CYCLE ANALYSIS: ITAU")
print(f'{"="*80}')

# Step 1: all unique concepts
print("\n1. UNIQUE DESCRIPTIONS (searching for investment keywords):")
print("-" * 80)

q1 = """
SELECT DISTINCT 
  description,
  COUNT(*) as freq,
  SUM(COALESCE(debit_amount, 0)) as total_debits,
  SUM(COALESCE(credit_amount, 0)) as total_credits
FROM `finanzas-personales-457115.silver.stg_movimientos`
WHERE bank_code = 'itau'
GROUP BY description
HAVING LOWER(description) LIKE '%inversion%'
  OR LOWER(description) LIKE '%letra%'
  OR LOWER(description) LIKE '%fondo%'
  OR LOWER(description) LIKE '%plazo%'
  OR LOWER(description) LIKE '%renta%'
  OR LOWER(description) LIKE '%rescate%'
  OR LOWER(description) LIKE '%vto%'
  OR LOWER(description) LIKE '%vencimiento%'
ORDER BY freq DESC
"""

results1 = list(client.query(q1).result())
for row in results1:
    print(f'\n  {row["description"]}')
    print(
        f'    Freq: {row["freq"]:3d}  Debits: ${row["total_debits"]:>12,.2f}  Credits: ${row["total_credits"]:>12,.2f}'
    )

# Step 2: find 6-month cycles (including commission tolerance)
print("\n\n2. 6-MONTH CYCLES (180 ± 30 days, with commission tolerance):")
print("-" * 80)

q2 = """
WITH debits AS (
  SELECT
    transaction_date,
    description,
    COALESCE(debit_amount, 0) as amount,
    ROW_NUMBER() OVER (ORDER BY transaction_date) as debit_id
  FROM `finanzas-personales-457115.silver.stg_movimientos`
  WHERE bank_code = 'itau' AND COALESCE(debit_amount, 0) > 0
),
credits AS (
  SELECT
    transaction_date,
    description,
    COALESCE(credit_amount, 0) as amount
  FROM `finanzas-personales-457115.silver.stg_movimientos`
  WHERE bank_code = 'itau' AND COALESCE(credit_amount, 0) > 0
),
cycles AS (
  SELECT
    d.transaction_date as debit_date,
    d.description as debit_desc,
    d.amount as debit_amt,
    c.transaction_date as credit_date,
    c.description as credit_desc,
    c.amount as credit_amt,
    DATE_DIFF(c.transaction_date, d.transaction_date, DAY) as cycle_days,
    DATE_DIFF(c.transaction_date, d.transaction_date, DAY) as days,
    ROUND(100.0 * ABS(c.amount - d.amount) / d.amount, 2) as commission_pct
  FROM debits d
  LEFT JOIN credits c
    ON DATE_DIFF(c.transaction_date, d.transaction_date, DAY) BETWEEN 150 AND 210
    AND c.amount BETWEEN d.amount * 0.95 AND d.amount * 1.05
)
SELECT * FROM cycles
WHERE credit_date IS NOT NULL
ORDER BY debit_date DESC
"""

results2 = list(client.query(q2).result())
if results2:
    for row in results2:
        print(
            f'\n✓ {row["debit_date"].strftime("%Y-%m-%d")} → {row["credit_date"].strftime("%Y-%m-%d")} ({row["cycle_days"]} days)'
        )
        print(f'  Debit:  ${row["debit_amt"]:>10,.2f}  | {row["debit_desc"][:50]}')
        print(f'  Credit: ${row["credit_amt"]:>10,.2f}  | {row["credit_desc"][:50]}')
        if row["commission_pct"] > 0:
            print(f'  Commission/diff: {row["commission_pct"]:.2f}%')
else:
    print("  (No 6-month cycles found)")

# Step 3: find any closed debit-credit cycle (flexible)
print("\n\n3. ANY CLOSED CYCLES (flexible matching, all periods):")
print("-" * 80)

q3 = """
WITH debits AS (
  SELECT
    transaction_date,
    description,
    COALESCE(debit_amount, 0) as amount
  FROM `finanzas-personales-457115.silver.stg_movimientos`
  WHERE bank_code = 'itau' AND COALESCE(debit_amount, 0) > 0
),
credits AS (
  SELECT
    transaction_date,
    description,
    COALESCE(credit_amount, 0) as amount
  FROM `finanzas-personales-457115.silver.stg_movimientos`
  WHERE bank_code = 'itau' AND COALESCE(credit_amount, 0) > 0
)
SELECT
  d.transaction_date as debit_date,
  d.description as debit_desc,
  d.amount as debit_amt,
  c.transaction_date as credit_date,
  c.description as credit_desc,
  c.amount as credit_amt,
  DATE_DIFF(c.transaction_date, d.transaction_date, DAY) as cycle_days
FROM debits d
LEFT JOIN credits c
  ON c.amount BETWEEN d.amount * 0.90 AND d.amount * 1.10
  AND c.transaction_date >= d.transaction_date
WHERE (LOWER(d.description) LIKE '%inversion%' 
   OR LOWER(d.description) LIKE '%letra%'
   OR LOWER(d.description) LIKE '%fondo%'
   OR LOWER(d.description) LIKE '%plazo%')
ORDER BY d.transaction_date DESC
LIMIT 30
"""

results3 = list(client.query(q3).result())
matched_count = 0
unmatched_count = 0

for row in results3:
    if row["credit_date"]:
        matched_count += 1
        print(
            f'✓ {row["debit_date"].strftime("%Y-%m-%d")} → {row["credit_date"].strftime("%Y-%m-%d")} ({row["cycle_days"]:3d}d): ${row["debit_amt"]:>10,.2f}'
        )
    else:
        unmatched_count += 1
        print(
            f'✗ {row["debit_date"].strftime("%Y-%m-%d")} (no match):                        ${row["debit_amt"]:>10,.2f}'
        )

print(f'\n{"="*80}')
print("SUMMARY:")
print(f"  Investment keywords found: {len(results1)}")
print(f"  6-month completed cycles:  {len(results2)}")
print(
    f"  Investment debits total:   {matched_count} matched, {unmatched_count} unmatched"
)
print("\nNOTE: Incomplete cycles can impact specific months.")
print(
    '      Retentions without visible recovery are currently considered "normal data".'
)
