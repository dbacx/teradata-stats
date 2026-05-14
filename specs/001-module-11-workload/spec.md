# Feature: Module 11 — Workload Management Analysis

> **Spec ID:** 001-module-11-workload
> **Autor:** Ricardo
> **Fecha:** 2026-05-14
> **Estado:** Draft

---

## Contexto

Teradata Workload Management (TDWM/TASM) controla la priorización y distribución
de recursos entre workloads. Actualmente el proyecto no tiene un módulo para
analizar la eficiencia de las reglas de clasificación, identificar workloads
infrautilizados o detectar desbalances en la asignación de recursos.

Este módulo proporcionará visibilidad sobre el estado de TDWM para que los DBAs
puedan optimizar la configuración de workloads.

---

## Requisitos Funcionales

1. **RF-01:** Extraer las reglas de clasificación activas de TDWM.
2. **RF-02:** Analizar la distribución de queries por workload en los últimos 30 días.
3. **RF-03:** Identificar workloads sin actividad en los últimos 30 días.
4. **RF-04:** Calcular el consumo de CPU por workload para detectar desbalances.
5. **RF-05:** Mostrar resultados en UI con tabs por componente y KPI cards.

---

## Criterios de Aceptación

### AC-01: Extracción exitosa de datos
- **Given** conexión activa a Teradata con acceso a vistas de TDWM/DBQL
- **When** el usuario presiona "Ejecutar Analisis" en la página de Workload
- **Then** el Collector retorna un `Dict[str, pd.DataFrame]` con al menos
  3 componentes: `classification_rules`, `workload_distribution`, `cpu_by_workload`

### AC-02: Tolerancia a fallos por componente
- **Given** un componente SQL que falla (ej: sin permisos sobre TDWM views)
- **When** el Collector ejecuta la query de ese componente
- **Then** registra el error con `logger.error()`, retorna `pd.DataFrame()` vacío
  para ese componente, y continúa con los demás

### AC-03: Detección de workloads sin uso
- **Given** datos de DBQL con distribución de queries por workload
- **When** el Analyzer ejecuta la regla de "workloads sin uso"
- **Then** identifica workloads con 0 queries en los últimos 30 días y asigna
  severidad HIGH

### AC-04: Renderizado con tabs
- **Given** el Analyzer retorna `Dict[str, pd.DataFrame]`
- **When** la UI renderiza los resultados
- **Then** usa `st.tabs` iterando sobre las claves del diccionario,
  verifica con `isinstance()` antes de llamar `.empty`

### AC-05: KPI cards en la parte superior
- **Given** resultados del análisis disponibles
- **When** la página renderiza
- **Then** muestra métricas clave: Total Workloads, Workloads Activos,
  Workloads Sin Uso, % CPU Top Workload

---

## Restricciones Arquitectónicas

- [x] SQL puro en `sql/module_11_workload/` (sin SQL inline en Python)
- [x] `collectors/mod11_workload_collector.py` hereda de `BaseCollector`
- [x] `analyzers/mod11_workload_analyzer.py` hereda de `BaseAnalyzer`
- [x] `ui/pages/10_Module_11_Workload.py` con botón "Ejecutar Analisis" + CSS `#FD6724`
- [x] `try...except` en cada query del Collector
- [x] Verificación de DataFrame vacío y columnas requeridas en el Analyzer
- [x] Renderizado con `isinstance()` + `st.tabs`
- [x] Registrar página en `ui/main.py` → `st.navigation()`

---

## Edge Cases

1. **Query retorna 0 filas:** Mostrar `st.info("Sin datos para este componente")`.
2. **Columna esperada no existe:** `logger.warning()` y retornar DataFrame sin modificar.
3. **Conexión falla:** `st.error()` con mensaje descriptivo, no propagar excepción.
4. **Sin permisos TDWM:** El Collector retorna DataFrame vacío para ese componente;
   la UI muestra "Sin datos" en el tab correspondiente.
5. **TDWM no configurado:** Todos los componentes retornan vacío; la UI muestra
   mensaje general "TDWM no está configurado en este sistema".

---

## Fuera de Alcance

- Modificación automática de reglas TDWM
- Integración con Viewpoint
- Análisis histórico de cambios en configuración TDWM

---

## Archivos a Crear/Modificar

| Acción | Archivo | Descripción |
|--------|---------|-------------|
| Crear  | `sql/module_11_workload/01_classification_rules.sql` | Reglas de clasificación |
| Crear  | `sql/module_11_workload/02_workload_distribution.sql` | Distribución de queries |
| Crear  | `sql/module_11_workload/03_cpu_by_workload.sql` | CPU por workload |
| Crear  | `collectors/mod11_workload_collector.py` | WorkloadCollector |
| Crear  | `analyzers/mod11_workload_analyzer.py` | WorkloadAnalyzer |
| Crear  | `ui/pages/10_Module_11_Workload.py` | Página Workload |
| Modificar | `ui/main.py` | Agregar `pg_workload` a navegación |

---

## Notas Adicionales

- Las vistas relevantes de TDWM varían entre versiones de Teradata (17.x vs 20.x).
  Verificar disponibilidad antes de implementar.
- Referencia: [Teradata Workload Management Guide](https://docs.teradata.com/)
