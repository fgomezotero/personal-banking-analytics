# Quality & Validation

Subfolders organized to validate data integrity in the medallion pipeline.

## Structure

```text
quality/
├── analysis/              # Analysis and validation scripts
│   ├── README.md         # 👈 START HERE: execution guide
│   ├── 01_validate_shift.py
│   ├── 02_audit_flow.py
│   ├── 03_analyze_investment_cycles.py
│   ├── 04_monthly_investment_impact.py
│   ├── 05_deep_investment_analysis.py
│   └── 06_monthly_impact_analysis.py
└── reconciliation/        # (Reserved for future)
```

## Quick Access

### Validate data load into BigQuery

```bash
conda run -n meltano python3 quality/analysis/01_validate_shift.py [bank] [YYYY-MM]
```

### Audit row loss across transformations

```bash
conda run -n meltano python3 quality/analysis/02_audit_flow.py [bank] [YYYY-MM]
```

### Full data-shift investigation

See [quality/analysis/README.md](analysis/README.md) -> "Use Case 2: Investigate Data Shift"

## Philosophy

All scripts in `quality/` should:

- Be **readable and documented** (docstring with PURPOSE, USAGE, OUTPUT, INTERPRETATION)
- Use **conda run -n meltano** (consistent runtime environment)
- **Not modify data** (read-only analysis and reporting)
- Be **sequentially executable** (run in order without conflict)

## Next Phases

- **reconciliation/**: Silver↔Gold reconciliation scripts (when dbt transformations are active)
- **dbt_tests/**: Integrated dbt tests (~future, depends on dbt setup)
- **metrics/**: Data quality dashboards (~future, BigQuery monitoring)
