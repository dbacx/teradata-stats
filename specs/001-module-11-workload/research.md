# Research — Module 11: Workload Management

## Decisiones Técnicas

### Vistas de Teradata a consultar

| Vista | Propósito | Disponibilidad |
|-------|-----------|---------------|
| `DBC.QryLogV` / `PDCRINFO.DBQLogTbl` | Distribución de queries por workload | TD 17.x+ |
| `TDWM.RuleSetsV` | Reglas de clasificación activas | Requiere permisos TDWM |
| `DBC.ResUsageSpma` | CPU por AMP/período | TD 17.x+ |

### Patrón a seguir

Replicar el patrón exacto de `mod3_performance_collector.py`:
- Lista de archivos SQL en `__init__`
- Loop con `try...except` por componente en `collect()`
- Retorno `Dict[str, pd.DataFrame]`

### Dependencias

No se requieren nuevas dependencias. Se usan las existentes:
- `pandas` para procesamiento
- `streamlit` + `plotly` para visualización
- `core.base_collector.BaseCollector` y `core.base_analyzer.BaseAnalyzer`
