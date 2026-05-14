# Tasks — Module 11: Workload Management

> Cada tarea es atómica: un commit verificable de forma independiente.

## Tareas

- [ ] **T-01:** Crear `sql/module_11_workload/01_classification_rules.sql`
  - Query contra TDWM.RuleSetsV para extraer reglas activas
  - Verificar con `SELECT` manual antes de commitear

- [ ] **T-02:** Crear `sql/module_11_workload/02_workload_distribution.sql`
  - Query contra PDCRINFO.DBQLogTbl agrupando por WDName
  - Parámetro `{days_threshold}` para filtro de fecha

- [ ] **T-03:** Crear `sql/module_11_workload/03_cpu_by_workload.sql`
  - Query contra PDCRINFO.DBQLogTbl con SUM(AMPCPUTime) por WDName
  - Excluir bases de sistema usando `NOT IN ({system_databases})`

- [ ] **T-04:** Crear `collectors/mod11_workload_collector.py`
  - Clase `WorkloadCollector(BaseCollector)`
  - `module_name='module_11_workload'`
  - `collect()` retorna `Dict[str, pd.DataFrame]`
  - `try...except` por componente

- [ ] **T-05:** Crear `analyzers/mod11_workload_analyzer.py`
  - Clase `WorkloadAnalyzer(BaseAnalyzer)`
  - `run()` recibe `Dict[str, pd.DataFrame]`
  - Regla: workloads con 0 queries → Severity.HIGH
  - Regla: workload con >50% CPU → Severity.MEDIUM

- [ ] **T-06:** Crear `ui/pages/10_Module_11_Workload.py`
  - Patrón: inject_css → initialize_state → button → collect → analyze → render
  - KPI cards: Total Workloads, Activos, Sin Uso, % CPU Top
  - Tabs por componente con `isinstance()` + `st.tabs`
  - CSS `#FD6724` para botón

- [ ] **T-07:** Registrar en `ui/main.py`
  - Agregar `pg_workload = st.Page("pages/10_Module_11_Workload.py", title="Workload Management")`
  - Agregar a la sección "Módulos" en `st.navigation()`

- [ ] **T-08:** Verificar AC-01 a AC-05
  - Prueba manual contra entorno Teradata de desarrollo
  - Verificar que fallo de un componente no bloquea los demás
