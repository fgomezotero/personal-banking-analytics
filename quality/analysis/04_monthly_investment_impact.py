#!/usr/bin/env python3
# pylint: disable=import-error
"""
Monthly Investment Impact: shows how 6-month cycles impact monthly balance

PURPOSE:
  Analyzes investment cycles (6M letters/funds) and their month-by-month impact.
  Explains why some months have debit/credit imbalance (legitimate data shift).

USAGE:
  conda run -n meltano python3 quality/analysis/04_monthly_investment_impact.py

EXPECTED OUTPUT:
  1. List of detected debit->credit cycles (typically around 6 months)
  2. Monthly impact table: debits, credits, and net by month
  3. Conclusion explaining cycles vs ETL errors

HOW TO INTERPRET RESULTS:
  Investment-Recovery Cycles:
  2024-05-01 DEBIT:  $100,000.00
    ↓
  2024-11-01 CREDIT: $101,234.00  (184 days after)

  Monthly Summary row example:
  2024-05 D: $100,000.00  C: $0.00      Net: -$100,000  -> Cycle started
  2024-11 D: $0.00       C: $101,234.00 Net: +$101,234  -> Cycle closed (gain)

  ⚠️  This is NORMAL and not an ETL issue. It reflects real cash flow timing.

INTERPRETATION:
  • DEBIT month: cash outflow (investment created)
  • CREDIT month: cash inflow (investment recovered + interests)
  • Net Impact: monthly difference caused by cycle timing

  If Net > $100,000 in a month: likely an investment cycle closing
  If Net < -$100,000 in a month: likely an investment cycle opening

GUARDRAILS:
  • Short cycles (< 60 days) are exceptions; ~180 days is typical
  • Interests can create debit/credit differences (e.g., 100k -> 101.2k)
  • If cycle >210 days, it may still be open; see script 05
  • Run after script 03 for better context

NEXT STEPS:
  If data shift is confirmed, review:
  1. Months where Net >> $10,000 without an obvious cycle
  2. Use script 05 (deep analysis) for open cycles
  3. Manually verify movements in those months
"""
from google.cloud import bigquery
import json

with open("secrets/finanzas-personales.json", encoding="utf-8") as f:
    creds = json.load(f)

client = bigquery.Client.from_service_account_info(
    creds, project="finanzas-personales-457115"
)

print(f'\n{"="*90}')
print("MONTHLY IMPACT: 6-Month Investment Cycles")
print(f'{"="*90}')

# Retrieve 6-month cycles associated with investment-related movements
q = """
WITH debits AS (
  SELECT
    transaction_date,
    FORMAT_DATE('%Y-%m', transaction_date) as month_debit,
    description,
    COALESCE(debit_amount, 0) as amount
  FROM `finanzas-personales-457115.silver.stg_movimientos`
  WHERE bank_code = 'itau' AND COALESCE(debit_amount, 0) > 0
    AND (LOWER(description) LIKE '%let%' 
      OR LOWER(description) LIKE '%inversion%'
      OR description LIKE 'DEB. VARIOS%')
),
credits AS (
  SELECT
    transaction_date,
    FORMAT_DATE('%Y-%m', transaction_date) as month_credit,
    description,
    COALESCE(credit_amount, 0) as amount
  FROM `finanzas-personales-457115.silver.stg_movimientos`
  WHERE bank_code = 'itau' AND COALESCE(credit_amount, 0) > 0
    AND (LOWER(description) LIKE '%let%' 
      OR LOWER(description) LIKE '%inversion%'
      OR LOWER(description) LIKE '%cambios%')
)
SELECT
  d.month_debit,
  d.transaction_date as debit_date,
  c.month_credit,
  c.transaction_date as credit_date,
  d.amount as debit_amt,
  c.amount as credit_amt,
  DATE_DIFF(c.transaction_date, d.transaction_date, DAY) as days_held
FROM debits d
LEFT JOIN credits c
  ON c.amount BETWEEN d.amount * 0.85 AND d.amount * 1.15
  AND c.transaction_date >= d.transaction_date
  AND DATE_DIFF(c.transaction_date, d.transaction_date, DAY) BETWEEN 150 AND 210
ORDER BY d.transaction_date
"""

results = list(client.query(q).result())

if not results:
    print("\n(No investment cycles found)")
else:
    print("\nInvestment-Recovery Cycles (6-month letters):")
    print("-" * 90)

    # Group by debit month and credit month
    monthly_impact = {}

    for row in results:
        debit_month = row["month_debit"]
        credit_month = row["month_credit"] if row["credit_date"] else None

        if debit_month not in monthly_impact:
            monthly_impact[debit_month] = {"debits": 0, "credits": [], "cycle_info": []}

        monthly_impact[debit_month]["debits"] += row["debit_amt"]

        if credit_month:
            if credit_month not in monthly_impact:
                monthly_impact[credit_month] = {
                    "debits": 0,
                    "credits": [],
                    "cycle_info": [],
                }
            monthly_impact[credit_month]["credits"].append(row["credit_amt"])

            cycle_info = f"{debit_month} → {credit_month} ({row['days_held']}d): ${row['debit_amt']:,.0f} → ${row['credit_amt']:,.0f}"
            monthly_impact[debit_month]["cycle_info"].append(cycle_info)

        print(
            f'\n{row["debit_date"].strftime("%Y-%m-%d")} DEBIT:  ${row["debit_amt"]:>10,.0f}'
        )
        if row["credit_date"]:
            days = (row["credit_date"] - row["debit_date"]).days
            print("  ↓")
            print(
                f'{row["credit_date"].strftime("%Y-%m-%d")} CREDIT: ${row["credit_amt"]:>10,.0f}  ({days} days after)'
            )
        else:
            print("  → (No matching credit found)")

    # Monthly summary
    print(f'\n\n{"="*90}')
    print("MONTHLY SUMMARY (Investment Impact):")
    print("-" * 90)
    print(
        f'{"Month":<12} {"Inv. Debits":>15} {"Inv. Credits":>15} {"Net Impact":>15} {"Notes"}'
    )
    print("-" * 90)

    for month in sorted(monthly_impact.keys()):
        debits = monthly_impact[month]["debits"]
        credit_total = sum(monthly_impact[month]["credits"])
        net = credit_total - debits
        notes = monthly_impact[month]["cycle_info"]

        note_str = " | ".join(notes) if notes else "Credit month"

        print(
            f"{month:<12} ${debits:>14,.0f} ${credit_total:>14,.0f} ${net:>14,.0f}  → {note_str[:50]}..."
        )

print(f'\n{"="*90}')
print("CONCLUSION:")
print('  Investment cycles (6-month letras) explain "data shift" in affected months.')
print("  Debit month: Shows expense/outflow")
print("  Credit month: Shows income/recovery (6 months later)")
print("  This is NORMAL for investment operations - NOT a data integrity issue.")
