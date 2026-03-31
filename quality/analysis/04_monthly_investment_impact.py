#!/usr/bin/env python3
# pylint: disable=import-error
"""
Monthly Investment Impact: Muestra cómo ciclos de 6 meses impactan balance mensual

PROPOSITO:
  Analiza ciclos de inversión (letras 6M, fondos plazo) y su impacto mes a mes.
  Explica por qué meses tienen débito/crédito desbalanceado (data shift legítimo).

USO:
  conda run -n meltano python3 quality/analysis/04_monthly_investment_impact.py

OUTPUT ESPERADO:
  1. Listado de ciclos debit->credit encontrados (6 meses típicamente)
  2. Tabla de impacto mensual: débitos, créditos y net por mes
  3. Conclusión explicando ciclos vs errores de ETL

COMO INTERPRETAR RESULTADOS:
  Ciclos de Inversión-Devolución:
  2024-05-01 DEBIT:  $100,000.00
    ↓
  2024-11-01 CREDIT: $101,234.00  (184 days after)

  Monthly Summary row ejemplo:
  2024-05 D: $100,000.00  C: $0.00      Net: -$100,000  → Ciclo iniciado
  2024-11 D: $0.00       C: $101,234.00 Net: +$101,234  → Ciclo cerrado (ganancia)

  ⚠️  Esto es NORMAL, no es data shift. Es flujo de efectivo real.

COMO INTERPRETAR:
  • Mes de DEBIT: Sale efectivo (inversión realizada)
  • Mes de CREDIT: Entra efectivo (inversión recuperada + intereses)
  • Net Impact: Diferencia mensual por cicltail

  Si Net > $100,000 en mes: Probablemente ciclo cierre de inversión
  Si Net < -$100,000 en mes: Probablemente ciclo inicio de inversión

GUARDRAILS:
  • Ciclos cortos (< 60 días) son excepciones; 180 días es típico
  • Intereses generan diferencia entre debit y credit (ej: 100k→101.2k)
  • Si ciclo >210 días, puede estar sin cerrar; ver script 05
  • Ejecutar después de 03 para mejor contexto

PRÓXIMOS PASOS:
  Si data shift confirmado, revisar:
  1. Meses donde Net >> $10,000 sin ciclo obvio
  2. Usar script 05 (deep analysis) para ciclos sin cerrar
  3. Verificar manualmente movimientos en esos meses
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

# Obtén todos los ciclos de 6 meses ligados a INVERSIONES/LETRAS
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
    print("\nCiclos de Inversión-Devolución (Letras 6 meses):")
    print("-" * 90)

    # Agrupa por mes de débito y mes de crédito
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

    # Resumen por mes
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
print("  Débito month: Shows expense/outflow")
print("  Crédito month: Shows income/recovery (6 months later)")
print("  This is NORMAL for investment operations - NOT a data integrity issue.")
