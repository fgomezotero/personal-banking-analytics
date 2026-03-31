#!/usr/bin/env python3
# pylint: disable=import-error
"""
Deep Investment Analysis: Busca ciclos históricos completos (6+ meses)
Incluye tolerancia por comisiones e intereses

PROPOSITO:
  Análisis profundo de ciclos de inversión sin restricciones de tiempo.
  Detecta patrones de retención/recuperación incluso si están abiertos o tras 6+ meses.

USO:
  conda run -n meltano python3 quality/analysis/05_deep_investment_analysis.py

OUTPUT ESPERADO:
  Tres niveles de análisis:

  1. UNIQUE DESCRIPTIONS: Todos los conceptos tipo inversión encontrados
     Muestra frecuencia y montos débito vs crédito por concepto

  2. 6-MONTH CYCLES: Ciclos estrictos (180±30 días) con tolerancia por comisiones
     Ej:
     ✓ 2024-05-01 → 2024-11-01 (184 days)
       Debit:  $100,000.00 | LETRA DE CAMBIO 6M
       Credit: $101,234.00 | RESCATE LETRA...
       Commission/diff: 1.23%

  3. ANY CLOSED CYCLES: Ciclos débito-crédito flexibles (sin restricción de días)
     Busca cualquier debit que tenga credit correspondiente
     ✓ Matched: Ciclo cerrado (devolución recibida)
     ✗ Unmatched: Debit sin crédito visible

COMO INTERPRETAR RESULTADOS:
  Commission/diff > 5%: Probablemente no es el ciclo real (buscar diferente)
  Commission/diff 0-5%: Intereses o comisiones normales
  Commission/diff < 0%: Recuperación con pérdida (ejercicio de opción, etc)

  Investment keywords high frequency: Tipo de movimiento más común

  If unmatched_count > 30%: Hay ciclos aún abiertos o sin devolución visible

GUARDRAILS:
  • Este es análisis exploratorio (sin restricciones de tiempo)
  • No prejuzga sobre data integrity; solo muestra patrones
  • Un debit sin credit ≠ error; puede ser gasto real, comisión, reembolso parcial
  • Ejecutar después de 04 para contexto completo

PRÓXIMOS PASOS:
  Si muchos unmatched:
  1. Ver script 06 (monthly impact) para impacto global mes a mes
  2. Verificar manualmente ciclos tras 6 meses (letras 12M, fondos plazo)
  3. Investigar gestión manual de retenciones vs automatizadas
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

# Paso 1: Todos los conceptos únicos
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

# Paso 2: Busca ciclos de 6 meses (incluyendo comisiones)
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

# Paso 3: Busca cualquier ciclo debit-credit que se cierre (flexible)
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
print("\nNOTA: Si hay ciclos incompletos, pueden impactar meses específicos.")
print('      Retenciones sin devolución aún visible son "datos normales" por ahora.')
