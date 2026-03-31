# Quality: Scripts de Analisis y Validacion

Conjunto de herramientas para validar integridad de datos en el pipeline medallion Bronze->Silver->Gold.

## Orden de Ejecucion Recomendado

Lo ideal es ejecutar los scripts en orden: **01 -> 02 -> 03 -> 04 -> 05 -> 06** (validacion y auditoria) + **07** (vista financiera).

Cada script aporta contexto para el siguiente. Si solo necesitas una validacion rapida, ejecuta **01**. Para una vista financiera rapida, ejecuta **07**.

```text
01_validate_shift                 (rapido: 5-10 seg)
    |
02_audit_flow                     (rapido: 5-10 seg)
    |
03_analyze_investment_cycles      (medio: 15-30 seg)
    |
04_monthly_investment_impact      (medio: 20-40 seg)
    |
05_deep_investment_analysis       (medio: 30-60 seg)
    |
06_monthly_impact_analysis        (medio: 30-60 seg)
    |
07_last_year_income_expense_summary (rapido: 10-15 seg)
    |
08_income_analysis_by_month       (rapido: 10-15 seg)

Total sin cache: ~2-4 minutos
Alternativa rapida (01 + 07): ~15-25 seg
```

## Scripts Disponibles

| # | Script | Proposito | Tiempo | Parametros | Cuando usar |
| --- | --- | --- | --- | --- | --- |
| 01 | `validate_shift.py` | Reconciliacion completa Bronze->Silver->Gold | 5-10s | `[banco] [YYYY-MM]` | Despues de cada carga a BigQuery |
| 02 | `audit_flow.py` | Audita filtrado en cada transformacion | 5-10s | `[banco] [YYYY-MM]` | Entender perdida de filas |
| 03 | `analyze_investment_cycles.py` | Detecta ciclos debito->credito cerrados | 15-30s | (ninguno) | Exploracion de retenciones |
| 04 | `monthly_investment_impact.py` | Impacto mensual de ciclos 6M | 20-40s | (ninguno) | Explicar desbalance mensual |
| 05 | `deep_investment_analysis.py` | Analisis profundo sin restriccion temporal | 30-60s | (ninguno) | Ciclos >6M o sin cerrar |
| 06 | `monthly_impact_analysis.py` | Impacto global mes a mes y decision final | 30-60s | (ninguno) | Concluir sobre data shift |
| 07 | `last_year_income_expense_summary.py` | Resumen anual consolidado + detalle mensual de los ultimos 12 meses | 10-15s | (ninguno) | Vista financiera rapida |
| 08 | `income_analysis_by_month.py` | Analisis detallado de incomes para un YYYY-MM | 10-15s | `[YYYY-MM]` | Investigar composicion de ingresos |

## Guia por Caso de Uso

### Caso 1: Validacion rapida despues de carga

```bash
conda run -n meltano python3 quality/analysis/01_validate_shift.py scotia 2024-10
```

Salida esperada: `NO DATA SHIFT`.

### Caso 2: Investigar data shift detectado

```bash
conda run -n meltano python3 quality/analysis/02_audit_flow.py scotia 2024-10
conda run -n meltano python3 quality/analysis/03_analyze_investment_cycles.py
conda run -n meltano python3 quality/analysis/04_monthly_investment_impact.py
```

### Caso 3: Auditoria completa post-mes

```bash
for script in 01_validate_shift.py 02_audit_flow.py 03_analyze_investment_cycles.py \
              04_monthly_investment_impact.py 05_deep_investment_analysis.py 06_monthly_impact_analysis.py; do
  echo "=== Ejecutando $script ==="
  conda run -n meltano python3 quality/analysis/$script
done
```

### Caso 4: Vista financiera rapida

```bash
conda run -n meltano python3 quality/analysis/07_last_year_income_expense_summary.py
```

### Caso 5: Analizar incomes de un mes especifico

```bash
conda run -n meltano python3 quality/analysis/08_income_analysis_by_month.py 2025-04
```

## Interpretacion de Resultados

### Script 01: Validate Shift

```text
NO DATA SHIFT          -> Pipeline integro
DATA SHIFT DETECTADO   -> Ejecutar scripts 02-06
No hay datos agregado  -> Revisar cargas/transformaciones
```

### Script 02: Audit Flow

```text
[OK]   Sin perdida relevante
[LOSE] Filtrado detectado, revisar causa
```

### Scripts 03-05: Ciclos de inversion

```text
Ciclos completos altos -> Data shift puede ser legitimo
Muchos sin devolucion  -> Puede haber ciclos abiertos
```

### Script 06: Monthly Impact

```text
Net extremo (positivo o negativo) + ciclos detectados -> Shift legitimo
Net extremo sin ciclos detectados                     -> Investigar
```

### Script 07: Last Year Income vs Expense Summary

```text
Period:              2025-01 -> 2025-12
Total income:        $1,240,000.00
Total expense:       $1,180,000.00
Net:                 $60,000.00
```

| Status | Significado | Accion |
| --- | --- | --- |
| Surplus | Net > 0: ingresos > egresos | Normal |
| Deficit | Net < 0: egresos > ingresos | Verificar si hay ciclos |
| Balanced | \|Net\| < $1k | Balanceado |

## Troubleshooting

### Error: No data in aggregate

1. Verificar ejecucion de transformaciones dbt.
2. Verificar que la carga a BigQuery finalizo.
3. Revisar logs en `.meltano/logs/`.

### Error: Cannot find secret file

```bash
pwd
ls secrets/finanzas-personales.json
```

### Resultado: many unmatched cycles

1. Ejecutar script 05 para analisis sin limite temporal.
2. Verificar manualmente movimientos de Itaú.
3. Revisar comisiones y retenciones.

## Guardrails

- No editar scripts sin necesidad funcional.
- Ejecutar desde la raiz del proyecto.
- Usar siempre `conda run -n meltano`.
- Documentar conclusiones cuando se confirme data shift.

## Comandos Rapidos

```bash
conda run -n meltano python3 quality/analysis/01_validate_shift.py scotia 2024-10
conda run -n meltano python3 quality/analysis/02_audit_flow.py itau 2024-05
conda run -n meltano python3 quality/analysis/03_analyze_investment_cycles.py
conda run -n meltano python3 quality/analysis/06_monthly_impact_analysis.py
```

Ultima actualizacion: 2026-03-30
Version: 1.1
