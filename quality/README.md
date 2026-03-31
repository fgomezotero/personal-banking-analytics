# Quality & Validation

Subcarpetas organizadas para validar integridad de datos en pipeline medallion.

## Estructura

```text
quality/
├── analysis/              # Scripts de análisis y validación
│   ├── README.md         # 👈 COMIENZA AQUÍ: guía de ejecución
│   ├── 01_validate_shift.py
│   ├── 02_audit_flow.py
│   ├── 03_analyze_investment_cycles.py
│   ├── 04_monthly_investment_impact.py
│   ├── 05_deep_investment_analysis.py
│   └── 06_monthly_impact_analysis.py
└── reconciliation/        # (Reservado para futuro)
```

## Acceso Rápido

### Para validar que datos llegaron bien a BigQuery

```bash
conda run -n meltano python3 quality/analysis/01_validate_shift.py [banco] [YYYY-MM]
```

### Para auditar pérdida de filas en transformación

```bash
conda run -n meltano python3 quality/analysis/02_audit_flow.py [banco] [YYYY-MM]
```

### Para investigar "Data Shift" completo

Ver [quality/analysis/README.md](analysis/README.md) → "Caso 2: Investigar Data Shift"

## Filosofía

todos los scripts en `quality/` deben:

- Ser **leíbles y documentados** (docstring con PURPOSE, USO, OUTPUT, INTERPRETACIÓN)
- Usar **conda run -n meltano** (ambiente consistente)
- **No modificar datos** (solo lectura, análisis, reportes)
- Ser **secuenciables** (pueden ejecutarse en orden sin conflicto)

## Próximg Fases

- **reconciliation/**: Scripts para reconciliación Silver↔Gold (cuando hay transformaciones dbt)
- **dbt_tests/**: Tests dbt integrados (~future, dependencia de dbt setup)
- **metrics/**: Data quality dashboards (~future, BigQuery Monitoring)
