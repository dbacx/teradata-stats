# Feature: Módulo 2 — Ajuste de severidad de KPIs y Limpieza de UI

> **Spec ID:** 002-mod2-kpi-severity-cleanup
> **Autor:** Ricardo
> **Fecha:** 2026-05-14
> **Estado:** Draft

---

## Contexto

El Módulo 2 actualmente calcula una categoría "INFO" en las tarjetas de KPI y expone
datos crudos en la interfaz. Se requiere restringir los niveles de severidad gerencial
y limpiar el renderizado de la UI.

---

## Requisitos Funcionales

1. **RF-01:** Eliminar el cálculo, conteo y visualización de la severidad "INFO"
   en el Analyzer y la UI.
2. **RF-02:** Mantener exclusivamente 4 tarjetas de KPI de severidad:
   CRITICAL, HIGH, MEDIUM, LOW.
3. **RF-03:** Eliminar la pestaña "Datos Analizados" de la UI.

---

## Contrato de Datos (I/O)

- **Output (Analyzer → UI):** El diccionario resultante que alimenta los KPIs debe
  contener ESTRICTAMENTE las claves de severidad: `['CRITICAL', 'HIGH', 'MEDIUM', 'LOW']`.
  Si el Analyzer inyecta la clave 'INFO', se considera un fallo de contrato.

---

## Criterios de Aceptación

### AC-01: Renderizado estricto de KPIs
- **Given** el diccionario de resultados del Analyzer.
- **When** la UI procesa las métricas de severidad.
- **Then** renderiza exactamente 4 columnas (`st.columns(4)`) distribuyendo
  CRITICAL, HIGH, MEDIUM, LOW.

### AC-02: Control de Pestañas (Tabs)
- **Given** la sección "Resultados del Análisis".
- **When** se renderizan las pestañas.
- **Then** el código usa ESTRICTAMENTE `st.tabs(["Hallazgos", "Scripts de Remediación"])`.
  No debe existir ninguna referencia a "Datos Analizados".

---

## Restricciones Arquitectónicas

- [ ] Cambios en `analyzers/mod2_stats_analyzer.py`: eliminar asignación de Severity.INFO
- [ ] Cambios en `ui/pages/2_Module_2_Statistics.py`: 4 columnas de KPI, eliminar tab "Datos Analizados"
- [ ] No modificar la estructura de `collectors/mod2_stats_collector.py`
- [ ] No modificar archivos SQL

---

## Edge Cases

1. ¿Qué pasa si un hallazgo no tiene severidad asignada? → Debe asignarse LOW por defecto, nunca INFO.
2. ¿Qué pasa si todos los hallazgos son de una sola severidad? → Los 3 KPIs restantes muestran 0.

---

## Fuera de Alcance

- Modificación del motor de 16 reglas (`analyzers/rules/`)
- Cambios en la Home Page (`ui/app.py`)
- Cambios en otros módulos (3-10)

---

## Archivos a Crear/Modificar

| Acción | Archivo | Descripción |
|--------|---------|-------------|
| Modificar | `analyzers/mod2_stats_analyzer.py` | Eliminar Severity.INFO |
| Modificar | `ui/pages/2_Module_2_Statistics.py` | 4 KPIs, eliminar tab "Datos Analizados" |
