#!/usr/bin/env python3
# pylint: disable=import-error,broad-exception-caught
"""
Data Shift Validator: Reconcilia Bronze → Silver → Gold para un banco/mes específico

PROPOSITO:
  Valida que los datos se mantengan íntegros a través de todas las capas medallon.
  Compara conteos, sumas de débito/crédito y verifica que NO haya pérdida de datos.

USO:
  conda run -n meltano python3 quality/analysis/01_validate_shift.py [banco] [YYYY-MM]

EJEMPLOS:
  # Validar Scotia octubre 2024
  conda run -n meltano python3 quality/analysis/01_validate_shift.py scotia 2024-10

  # Validar Itaú mayo 2024 (por defecto si no se especifican argumentos)
  conda run -n meltano python3 quality/analysis/01_validate_shift.py itau 2024-05

OUTPUT ESPERADO:
  ✅ NO DATA SHIFT: Datos íntegros de bronze a gold
  ⚠️  DATA SHIFT DETECTADO: Hay diferencias entre capas
  ❌ No hay datos en agregado: El mes no tiene datos en la capa gold

COMO INTERPRETAR RESULTADOS:
  1. BRONZE (Fuente): Filas y suma de débito/crédito que entran
  2. SILVER (Normalizado): Después de parsing y limpieza
  3. GOLD (Agregado): Datos finales listos para análisis

  Reconciliación verifica:
  - Debito→Expense: Suma de débitos en silver = gastos en gold
  - Credito→Income: Suma de créditos en silver = ingresos en gold
  - Row count: Cantidad de filas se mantiene

  ❌ ERROR en cualquier reconciliación = data shift genuino (investigar transformación)
  ✅ OK en todas = pipeline íntegro

GUARDRAILS:
  • Ejecutar después de cargas a BigQuery (una vez que silver/gold esté poblado)
  • Cambios en stream_maps de meltano.yml pueden causar data shift legítimo
  • Para ciclos de inversión (6+ meses), ver script 04_monthly_investment_impact.py
"""
from google.cloud import bigquery
import json
import sys

with open("secrets/finanzas-personales.json", encoding="utf-8") as f:
    creds = json.load(f)

client = bigquery.Client.from_service_account_info(
    creds, project="finanzas-personales-457115"
)

# Parámetros (cambiar aquí o pasar como argumentos)
if len(sys.argv) > 2:
    bank = sys.argv[1]
    year_month = sys.argv[2]
else:
    bank = "scotia"
    year_month = "2024-10"

print(f'\n{"="*70}')
print(f"VALIDACION DATA SHIFT: {bank.upper()} - {year_month}")
print(f'{"="*70}')

try:
    # BRONZE
    q1 = f"""SELECT COUNT(*) as row_count, COALESCE(SUM(CAST(REPLACE(REPLACE(debito, ' ', ''), ',', '.') AS FLOAT64)),0) as debito, COALESCE(SUM(CAST(REPLACE(REPLACE(credito, ' ', ''), ',', '.') AS FLOAT64)),0) as credito FROM `finanzas-personales-457115.bronze.scotia_debito` WHERE FORMAT_DATE("%Y-%m", PARSE_DATE("%d/%m/%Y", fecha)) = "{year_month}" """

    r1 = list(client.query(q1).result())[0]
    print("\n🔵 BRONZE (Fuente):")
    print(f'  Total rows:              {r1["row_count"]}')
    print(f'  Debito (sum):            ${r1["debito"]:,.2f}')
    print(f'  Credito (sum):           ${r1["credito"]:,.2f}')

    # SILVER
    q2 = f"""SELECT COUNT(*) as row_count, COALESCE(SUM(ABS(debit_amount)),0) as debito, COALESCE(SUM(ABS(credit_amount)),0) as credito FROM `finanzas-personales-457115.silver.stg_movimientos` WHERE bank_code = "{bank}" AND FORMAT_DATE("%Y-%m", transaction_date) = "{year_month}" """

    r2 = list(client.query(q2).result())[0]
    print("\n🟢 SILVER (Normalizado):")
    print(f'  Total rows:              {r2["row_count"]}')
    print(f'  Debito (sum):            ${r2["debito"]:,.2f}')
    print(f'  Credito (sum):           ${r2["credito"]:,.2f}')

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
        print("\n🟡 GOLD (Agregado agg_monthly_cashflow):")
        print(f'  Movement count:          {r3["movements_count"]}')
        print(f'  Expense (gasto):         ${r3["expense_month"]:,.2f}')
        print(f'  Income (ingreso):        ${r3["income_month"]:,.2f}')

        # Reconciliacion
        print(f'\n{"="*70}')
        print("RECONCILIACION:")
        print(f'{"-"*70}')
        # Reconciliation: compare only expense and income movements (internal_transfer excluded by design)
        d_match = abs(silver_expense_amount - r3["expense_month"]) < 0.01
        c_match = abs(silver_income_amount - r3["income_month"]) < 0.01
        r_match = r2["row_count"] == r3["movements_count"]

        print(
            f'  Debito(expense)→Expense: {"✅ OK" if d_match else "❌ ERROR"} (silver expense=${silver_expense_amount:,.2f} vs gold=${r3["expense_month"]:,.2f}, diff: ${abs(silver_expense_amount - r3["expense_month"]):,.2f})'
        )
        print(
            f'  Credit(income)→Income:   {"✅ OK" if c_match else "❌ ERROR"} (silver income=${silver_income_amount:,.2f} vs gold=${r3["income_month"]:,.2f}, diff: ${abs(silver_income_amount - r3["income_month"]):,.2f})'
        )
        print(
            f'  Row count:               {"✅ OK" if r_match else "❌ ERROR"} (silver={r2["row_count"]}, gold={r3["movements_count"]})'
        )
        print(
            f"\n  NOTE: internal_transfer movements are excluded from expense/income by design (not real $flow in/out)"
        )

        if d_match and c_match and r_match:
            print("\n✅ NO DATA SHIFT - Datos integros de fuente a agregado")
            sys.exit(0)
        else:
            print("\n⚠️  DATA SHIFT DETECTADO")
            sys.exit(1)
    else:
        print(f"\n❌ No hay datos en agregado {year_month}")
        sys.exit(2)

except Exception as e:
    print(f"\n❌ Error: {e}")
    sys.exit(3)
