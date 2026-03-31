#!/usr/bin/env python3
# pylint: disable=import-error
"""
Monthly Impact Analysis: Valida data shift considerando ciclos de inversión

PROPOSITO:
  Análisis de impacto mensual global. Muestra qué meses tienen debit/Credit
  desbalanceado y si se explica por ciclos de inversión.

USO:
  conda run -n meltano python3 quality/analysis/06_monthly_impact_analysis.py

OUTPUT ESPERADO:
  Dos secciones:

  1. Movimientos de Inversión/Retención por Mes
     Mostratomía tipo inversión filtrada por mes:
     2024-05 | LETRA DE CAMBIO... | 1 ops | D:$100,000.00 C:$0.00

  2. Monthly Debit vs Credit (últimos 12 meses)
     Tabla comparativa mensual:
     2024-05 | 25 trans | D:$250,000 | C:$200,000 | Net:$50,000 ⚠️ DEBIT
     2024-06 | 22 trans | D:$180,000 | C:$280,000 | Net:-$100,000 ✓

  Interpretación del Net:
  • Positivo (MAS DEBITS): Mes con más gastos/inversiones que ingresos
  • Negativo (MAS CREDITS): Mes con más ingresos que gastos
  • Alto (±$100k+): Probablemente ciclo de inversión inicio/cierre

COMO INTERPRETAR RESULTADOS:
  ⚠️ DEBIT (Net > $100): Mes con desequilibrio de gastos
    Casos normales:
    a) Ciclo de inversión 6M iniciado (débito de letra)
    b) Retención de impuestos o comisión
    c) Gasto real de efectivo (no inversión)

    Investigar:
    - ¿Hay movimiento tipo inversión ese mes?
    - ¿El débito tiene crédito en mes+6?
    - ¿Es gasto operativo o retención?

  ✓ Balanceado: Mes sin desequilibrio significativo

DATA SHIFT explicado:
  Si Net > $100k en meses A y Net < -$100k en mes A+6:
  → ES data shift LEGÍTIMO por ciclos de inversión
  → NO es error en ETL; es flujo de efectivo normal

GUARDRAILS:
  • Ejecutar después de scripts 01-05 para contexto completo
  • Último mes puede parecer incompleto si ciclos aún abiertos
  • Comparar Net vs movimientos inversión para context
  • Si Net erráticoo sin explicación: posible error en transformación silver

DECISIÓN FINAL sobre Data Shift:
  ✅ ACEPTADO: Si Net explica ciclos de inversión documentados (ver scripts 03-05)
  ❌ INVESTIGAR: Si Net > $100k sin ciclo obvioo o sin movimientos inversión

  Próximo paso: Revisar manual en BigQuery los movimientos de ese mes
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

# Primero, identifica movimientos de inversión/retenidos históricos en el banco objetivo
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

print("\nMovimientos de Inversión/Retención por Mes:")
print("-" * 70)
results = list(client.query(q_all_investments).result())
if results:
    for row in results:
        print(
            f'{row["ym"]} | {row["description"][:40]:40} | {row["cnt"]:2} ops | D:${row["debit_amt"]:9,.2f} C:${row["credit_amt"]:9,.2f}'
        )
else:
    print("  (Ninguno encontrado)")

# Ahora analiza: ¿hay impacto por ciclos?
# Busca meses donde debit != credit (pueden ser ciclos parciales)
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

print("\n\nMonthly Debit vs Credit (últimos 12 meses):")
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
print("Si Net es positivo (MAS DEBITS que CREDITS):")
print("  • Puede ser: ciclo de inversión iniciado pero devolución en mes siguiente")
print("  • Puede ser: gasto real sin correspondencia")
print("  • Necesita: análisis individual o verificación bancaria")
print()
print("Data Shift = Cambio artificial en cifras por ciclos multi-mes")
print("NO necesariamente indica un problema en el ETL")
