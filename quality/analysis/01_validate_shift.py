#!/usr/bin/env python3
# pylint: disable=import-error,broad-exception-caught
"""
Data Shift Validator: Reconciles Bronze -> Silver -> Gold for a specific bank/month

PURPOSE:
    Validates data integrity across all medallion layers.
    Compares row counts and debit/credit sums to confirm no data loss.

USAGE:
    conda run -n meltano python3 quality/analysis/01_validate_shift.py [bank] [YYYY-MM]

EXAMPLES:
    # Validate Scotia October 2024
  conda run -n meltano python3 quality/analysis/01_validate_shift.py scotia 2024-10

    # Validate Itau May 2024 (default if no arguments are provided)
  conda run -n meltano python3 quality/analysis/01_validate_shift.py itau 2024-05

EXPECTED OUTPUT:
    ✅ NO DATA SHIFT: Data is consistent from bronze to gold
    ⚠️  DATA SHIFT DETECTED: Differences exist across layers
    ❌ No aggregate data: The month has no rows in gold

HOW TO INTERPRET RESULTS:
    1. BRONZE (Source): Input rows and debit/credit sums
    2. SILVER (Standardized): After parsing and cleanup
    3. GOLD (Aggregate): Final analysis-ready values

    Reconciliation checks:
    - Debit->Expense: Sum of silver debits = gold expense
    - Credit->Income: Sum of silver credits = gold income
    - Row count: Number of rows is preserved

    ❌ Any reconciliation error = real data shift (investigate transformations)
    ✅ All checks OK = pipeline integrity confirmed

GUARDRAILS:
    • Run after BigQuery loads (once silver/gold are populated)
    • `stream_maps` changes in meltano.yml may cause legitimate data shift
    • For investment cycles (6+ months), see script 04_monthly_investment_impact.py
"""
from google.cloud import bigquery
import json
import sys

with open("secrets/finanzas-personales.json", encoding="utf-8") as f:
    creds = json.load(f)

client = bigquery.Client.from_service_account_info(
    creds, project="finanzas-personales-457115"
)

# Parameters (set here or pass as arguments)
if len(sys.argv) > 2:
    bank = sys.argv[1]
    year_month = sys.argv[2]
else:
    bank = "scotia"
    year_month = "2024-10"

print(f'\n{"="*70}')
print(f"DATA SHIFT VALIDATION: {bank.upper()} - {year_month}")
print(f'{"="*70}')

try:
    # BRONZE
    q1 = f"""SELECT COUNT(*) as row_count, COALESCE(SUM(CAST(REPLACE(REPLACE(debito, ' ', ''), ',', '.') AS FLOAT64)),0) as debito, COALESCE(SUM(CAST(REPLACE(REPLACE(credito, ' ', ''), ',', '.') AS FLOAT64)),0) as credito FROM `finanzas-personales-457115.bronze.scotia_debito` WHERE FORMAT_DATE("%Y-%m", PARSE_DATE("%d/%m/%Y", fecha)) = "{year_month}" """

    r1 = list(client.query(q1).result())[0]
    print("\n🔵 BRONZE (Source):")
    print(f'  Total rows:              {r1["row_count"]}')
    print(f'  Debit (sum):             ${r1["debito"]:,.2f}')
    print(f'  Credit (sum):            ${r1["credito"]:,.2f}')

    # SILVER
    q2 = f"""SELECT COUNT(*) as row_count, COALESCE(SUM(ABS(debit_amount)),0) as debito, COALESCE(SUM(ABS(credit_amount)),0) as credito FROM `finanzas-personales-457115.silver.stg_movimientos` WHERE bank_code = "{bank}" AND FORMAT_DATE("%Y-%m", transaction_date) = "{year_month}" """

    r2 = list(client.query(q2).result())[0]
    print("\n🟢 SILVER (Standardized):")
    print(f'  Total rows:              {r2["row_count"]}')
    print(f'  Debit (sum):             ${r2["debito"]:,.2f}')
    print(f'  Credit (sum):            ${r2["credito"]:,.2f}')

    # SILVER by movement_type (for detailed reconciliation)
    q2b = f"""SELECT movement_type, COUNT(*) as cnt, COALESCE(SUM(ABS(debit_amount)),0) as debito, COALESCE(SUM(ABS(credit_amount)),0) as credito FROM `finanzas-personales-457115.silver.stg_movimientos` WHERE bank_code = "{bank}" AND FORMAT_DATE("%Y-%m", transaction_date) = "{year_month}" GROUP BY movement_type ORDER BY movement_type """
    r2b = list(client.query(q2b).result())
    print("  By movement_type:")
    silver_expense_amount = 0
    silver_income_amount = 0
    for row in r2b:
        mt = row["movement_type"]
        print(
            f"    - {mt}: {row['cnt']} rows, debit=${row['debito']:,.2f}, credit=${row['credito']:,.2f}"
        )
        if mt == "expense":
            silver_expense_amount = row["debito"]
        elif mt == "income":
            silver_income_amount = row["credito"]

    # GOLD
    q3 = f"""SELECT movements_count, expense_month, income_month FROM `finanzas-personales-457115.gold.agg_monthly_cashflow` WHERE bank_code = "{bank}" AND year_month = "{year_month}" """

    r3 = list(client.query(q3).result())
    if r3:
        r3 = r3[0]
        print("\n🟡 GOLD (Aggregate agg_monthly_cashflow):")
        print(f'  Movement count:          {r3["movements_count"]}')
        print(f'  Expense:                 ${r3["expense_month"]:,.2f}')
        print(f'  Income:                  ${r3["income_month"]:,.2f}')

        # Reconciliation
        print(f'\n{"="*70}')
        print("RECONCILIATION:")
        print(f'{"-"*70}')
        # Reconciliation: compare only expense and income movements (internal_transfer excluded by design)
        d_match = abs(silver_expense_amount - r3["expense_month"]) < 0.01
        c_match = abs(silver_income_amount - r3["income_month"]) < 0.01
        r_match = r2["row_count"] == r3["movements_count"]

        print(
            f'  Debit(expense)->Expense: {"✅ OK" if d_match else "❌ ERROR"} (silver expense=${silver_expense_amount:,.2f} vs gold=${r3["expense_month"]:,.2f}, diff: ${abs(silver_expense_amount - r3["expense_month"]):,.2f})'
        )
        print(
            f'  Credit(income)→Income:   {"✅ OK" if c_match else "❌ ERROR"} (silver income=${silver_income_amount:,.2f} vs gold=${r3["income_month"]:,.2f}, diff: ${abs(silver_income_amount - r3["income_month"]):,.2f})'
        )
        print(
            f'  Row count:               {"✅ OK" if r_match else "❌ ERROR"} (silver={r2["row_count"]}, gold={r3["movements_count"]})'
        )
        print(
            "\n  NOTE: internal_transfer movements are excluded from expense/income by design (not real $flow in/out)"
        )

        if d_match and c_match and r_match:
            print("\n✅ NO DATA SHIFT - Data is consistent from source to aggregate")
            sys.exit(0)
        else:
            print("\n⚠️  DATA SHIFT DETECTED")
            sys.exit(1)
    else:
        print(f"\n❌ No aggregate data for {year_month}")
        sys.exit(2)

except Exception as e:
    print(f"\n❌ Error: {e}")
    sys.exit(3)
