# Quality: Analysis and Validation Scripts

Toolkit to validate data integrity in the Bronze->Silver->Gold medallion pipeline.

## Recommended Execution Order

The recommended order is **01 -> 02 -> 03 -> 04 -> 05 -> 06** (validation and auditing) + **07** (financial summary) + **08** (month-specific income analysis).

Each script provides context for the next one. If you only need a quick validation, run **01**. For a quick financial snapshot, run **07**.

```text
01_validate_shift                     (fast: 5-10 sec)
    |
02_audit_flow                         (fast: 5-10 sec)
    |
03_analyze_investment_cycles          (medium: 15-30 sec)
    |
04_monthly_investment_impact          (medium: 20-40 sec)
    |
05_deep_investment_analysis           (medium: 30-60 sec)
    |
06_monthly_impact_analysis            (medium: 30-60 sec)
    |
07_last_year_income_expense_summary   (fast: 10-15 sec)
    |
08_income_analysis_by_month           (fast: 10-15 sec)
```

Total without cache: ~2-4 minutes
Quick path (01 + 07): ~15-25 sec

## Available Scripts

| # | Script | Purpose | Time | Parameters | When to use |
| --- | --- | --- | --- | --- | --- |
| 01 | `01_validate_shift.py` | Full Bronze->Silver->Gold reconciliation | 5-10s | `[bank] [YYYY-MM]` | After every BigQuery load |
| 02 | `02_audit_flow.py` | Audits filtering at each transformation stage | 5-10s | `[bank] [YYYY-MM]` | Understand row-loss points |
| 03 | `03_analyze_investment_cycles.py` | Detects closed debit->credit cycles | 15-30s | none | Explore retention/investment patterns |
| 04 | `04_monthly_investment_impact.py` | Monthly impact of 6M cycles | 20-40s | none | Explain monthly imbalance |
| 05 | `05_deep_investment_analysis.py` | Deep analysis without strict time window | 30-60s | none | Cycles >6M or open cycles |
| 06 | `06_monthly_impact_analysis.py` | Global month-by-month impact and final shift decision | 30-60s | none | Final data-shift interpretation |
| 07 | `07_last_year_income_expense_summary.py` | Last-year consolidated summary + last 12 months detail | 10-15s | none | Quick financial view |
| 08 | `08_income_analysis_by_month.py` | Detailed income analysis for a specific YYYY-MM | 10-15s | `[YYYY-MM]` | Investigate income composition |

## Use Cases

### Use Case 1: Quick post-load validation

```bash
conda run -n meltano python3 quality/analysis/01_validate_shift.py scotia 2024-10
```

Expected output: `NO DATA SHIFT`.

### Use Case 2: Investigate detected data shift

```bash
conda run -n meltano python3 quality/analysis/02_audit_flow.py scotia 2024-10
conda run -n meltano python3 quality/analysis/03_analyze_investment_cycles.py
conda run -n meltano python3 quality/analysis/04_monthly_investment_impact.py
```

### Use Case 3: Full post-period audit

```bash
for script in 01_validate_shift.py 02_audit_flow.py 03_analyze_investment_cycles.py \
              04_monthly_investment_impact.py 05_deep_investment_analysis.py 06_monthly_impact_analysis.py; do
  echo "=== Running $script ==="
  conda run -n meltano python3 quality/analysis/$script
done
```

### Use Case 4: Quick financial summary

```bash
conda run -n meltano python3 quality/analysis/07_last_year_income_expense_summary.py
```

### Use Case 5: Income analysis for a specific month

```bash
conda run -n meltano python3 quality/analysis/08_income_analysis_by_month.py 2025-04
```

## Results Interpretation

### Script 01: Validate Shift

```text
NO DATA SHIFT        -> Pipeline integrity is OK
DATA SHIFT DETECTED  -> Run scripts 02-06
No aggregate data    -> Check loads/transformations
```

### Script 02: Audit Flow

```text
[OK]   No significant row loss
[LOSE] Filtering detected (inspect cause)
```

### Scripts 03-05: Investment cycles

```text
Many complete cycles -> Shift can be legitimate
Many open cycles     -> Cycles may still be active
```

### Script 06: Monthly Impact

```text
Extreme net + cycles detected -> Likely legitimate shift
Extreme net without cycles    -> Investigate
```

### Script 07: Last-Year Income vs Expense Summary

```text
Period:              2025-01 -> 2025-12
Total income:        $1,240,000.00
Total expense:       $1,180,000.00
Net:                 $60,000.00
```

| Status | Meaning | Action |
| --- | --- | --- |
| Surplus | Net > 0: income > expense | Normal |
| Deficit | Net < 0: expense > income | Check whether cycles explain it |
| Balanced | \|Net\| < $1k | Balanced |

## Troubleshooting

### Error: No data in aggregate

1. Verify dbt transformations were executed.
2. Verify BigQuery load completed.
3. Check logs in `.meltano/logs/`.

### Error: Cannot find secret file

```bash
pwd
ls secrets/finanzas-personales.json
```

### Result: many unmatched cycles

1. Run script 05 for no-time-limit analysis.
2. Manually inspect Itau movements.
3. Review commissions and retentions.

## Guardrails

- Do not edit scripts unless functionally necessary.
- Execute from project root.
- Always use `conda run -n meltano`.
- Document conclusions when data shift is confirmed.

## Quick Commands

```bash
conda run -n meltano python3 quality/analysis/01_validate_shift.py scotia 2024-10
conda run -n meltano python3 quality/analysis/02_audit_flow.py itau 2024-05
conda run -n meltano python3 quality/analysis/03_analyze_investment_cycles.py
conda run -n meltano python3 quality/analysis/06_monthly_impact_analysis.py
```

Last update: 2026-03-30
Version: 1.2
