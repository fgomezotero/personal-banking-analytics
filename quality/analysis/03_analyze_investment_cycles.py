#!/usr/bin/env python3
# pylint: disable=import-error
"""
Investment Cycle Analyzer: Detecta ciclos de retención-devolución en Itau

PROPOSITO:
  Identifica movimientos de inversión/retención y su correspondiente devolución.
  Entender ciclos completos (debit→credit) ayuda a explicar data shift.

USO:
  conda run -n meltano python3 quality/analysis/03_analyze_investment_cycles.py

OUTPUT ESPERADO:
  1. Descripción de movimientos tipo inversión encontrados
  2. Listado de ciclos cerrados (débito + crédito correspondiente)
  3. Débitos sin devolución aún visible

  Ejemplo:
  ✅ 2024-05-01 → 2024-11-01 (184 días): $100,000.00
     Débito:  LETRA DE CAMBIO 6M
     Crédito: RESCATE LETRA...

  ❌ 2024-06-15 (sin devolución): $50,000.00
     Débito:  RETENCIÓN RETORNO

COMO INTERPRETAR RESULTADOS:
  ✅ Ciclos completos: Normales. Explican por qué hay diferencia débito/crédito mes a mes
  ❌ Débitos sin devolución: Pueden ser:
     a) Ciclos aún abiertos (devolución en futuro próximo)
     b) Movimientos de gasto real (no retención)
     c) Comisiones o ajustes

  Si >50% sin devolución = investigar movimientos manuales o ajustes

GUARDRAILS:
  • Ejecutar después de 01 y 02 para contexto
  • Ciclos > 180 días se truncan (ver script 05 para ciclos flexibles)
  • Solo analiza Itaú (otros bancos pueden no tener inversiones)
  • Si devoluciones están en futuro, ver 04_monthly_investment_impact.py
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

# Busca movimientos tipo inversión/retención
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

print("\nDescripciones encontradas en Itau (potenciales inversiones):")
print("-" * 70)
results = list(client.query(q_concepts).result())
if results:
    for row in results:
        print(f'  • {row["description"]}')
else:
    print("  (Ninguna encontrada)")

# Análisis de ciclos completos: busca débito y crédito del mismo monto
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

print("\n\nCiclos Inversión-Devolución (últimos 20):")
print("-" * 70)
results = list(client.query(q_cycles).result())

matched = 0
unmatched = 0

for row in results:
    if row["credit_date"]:
        matched += 1
        print(
            f'✅ {row["debit_date"].strftime("%Y-%m-%d")} → {row["credit_date"].strftime("%Y-%m-%d")} ({row["cycle_days"]:3d} días): ${row["amount"]:,.2f}'
        )
        print(f'   Débito:  {row["debit_desc"][:50]}')
        print(f'   Crédito: {row["credit_desc"][:50]}')
    else:
        unmatched += 1
        print(
            f'❌ {row["debit_date"].strftime("%Y-%m-%d")} (sin devolución):             ${row["amount"]:,.2f}'
        )
        print(f'   Débito:  {row["debit_desc"][:50]}')

print(f'\n{"="*70}')
print("SUMMARY:")
print(f"  Ciclos completos (debit→credit): {matched}")
print(f"  Débitos sin devolución:          {unmatched}")
print("\nNOTA: Este análisis explica retenciones legítimas que impactan mes a mes.")
print('      Si tienes ciclos largos (>90 días), verás meses "desbalanceados".')
