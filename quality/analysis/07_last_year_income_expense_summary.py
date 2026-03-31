#!/usr/bin/env python3
# pylint: disable=import-error,broad-exception-caught
"""
Last Year Income vs Expense Summary: consolidated yearly view plus monthly detail

PURPOSE:
    Shows income vs expense for the last 12 months using the gold model.
    Returns a consolidated summary and a month-by-month detail section.

USAGE:
    conda run -n meltano python3 quality/analysis/07_last_year_income_expense_summary.py

EXAMPLES:
  conda run -n meltano python3 quality/analysis/07_last_year_income_expense_summary.py
"""
from google.cloud import bigquery
import json
import sys

with open("secrets/finanzas-personales.json", encoding="utf-8") as file_handle:
    creds = json.load(file_handle)

client = bigquery.Client.from_service_account_info(
    creds, project="finanzas-personales-457115"
)

print(f'\n{"="*86}')
print("LAST YEAR INCOME VS EXPENSE")
print(f'{"="*86}')

summary_query = """
WITH last_12_months AS (
  SELECT
    year_month,
    income_month,
    expense_month,
    movements_count,
    internal_transfer_count
  FROM `finanzas-personales-457115.gold.agg_monthly_cashflow_total`
  ORDER BY year_month DESC
  LIMIT 12
)
SELECT
  COUNT(*) AS months_included,
  MIN(year_month) AS start_month,
  MAX(year_month) AS end_month,
  SUM(income_month) AS total_income,
  SUM(expense_month) AS total_expense,
  SUM(income_month) - SUM(expense_month) AS net_amount,
  SUM(movements_count) AS total_movements,
  SUM(internal_transfer_count) AS total_transfers
FROM last_12_months
"""

monthly_detail_query = """
SELECT
    year_month,
    income_month,
    expense_month,
    income_month - expense_month AS net_amount,
    movements_count,
    internal_transfer_count
FROM `finanzas-personales-457115.gold.agg_monthly_cashflow_total`
ORDER BY year_month DESC
LIMIT 12
"""

try:
    rows = list(client.query(summary_query).result())

    if not rows:
        print("\n(No data found for the last year)")
        sys.exit(2)

    row = rows[0]
    total_income = float(row["total_income"] or 0)
    total_expense = float(row["total_expense"] or 0)
    net_amount = float(row["net_amount"] or 0)
    total_movements = int(row["total_movements"] or 0)
    total_transfers = int(row["total_transfers"] or 0)

    if net_amount > 1000:
        status = "Surplus"
    elif net_amount < -1000:
        status = "Deficit"
    else:
        status = "Balanced"

    print(f'\nPeriod:              {row["start_month"]} -> {row["end_month"]}')
    print(f'Months included:     {row["months_included"]}')
    print(f"Total income:        ${total_income:,.2f}")
    print(f"Total expense:       ${total_expense:,.2f}")
    print(f"Net:                 ${net_amount:,.2f}")
    print(f"Status:              {status}")
    print(f"Total movements:     {total_movements}")
    print(f"Internal transfers:  {total_transfers}")

    monthly_rows = list(client.query(monthly_detail_query).result())

    if monthly_rows:
        print(f'\n{"="*86}')
        print("LAST 12 MONTHS SUMMARY")
        print(f'{"="*86}')
        print(
            "Month      | Income       | Expense      | Net          | Status     | Trans | Xfers"
        )
        print("-" * 86)

        for month_row in monthly_rows:
            month_income = float(month_row["income_month"] or 0)
            month_expense = float(month_row["expense_month"] or 0)
            month_net = float(month_row["net_amount"] or 0)
            month_movements = int(month_row["movements_count"] or 0)
            month_transfers = int(month_row["internal_transfer_count"] or 0)

            if month_net > 1000:
                month_status = "Surplus"
            elif month_net < -1000:
                month_status = "Deficit"
            else:
                month_status = "Balanced"

            print(
                f'{month_row["year_month"]:10} | '
                f"${month_income:11,.2f} | "
                f"${month_expense:11,.2f} | "
                f"${month_net:11,.2f} | "
                f"{month_status:10} | "
                f"{month_movements:5} | "
                f"{month_transfers:5}"
            )

    print(f'\n{"="*86}')
    print("NOTE:")
    print("  • Amounts come from gold.agg_monthly_cashflow_total")
    print(
        "  • internal_transfer_count is reported for context, but does not impact income/expense"
    )
    print("  • Net = income - expense")
    sys.exit(0)

except Exception as error:
    print(f"\n❌ Error: {error}")
    sys.exit(3)
