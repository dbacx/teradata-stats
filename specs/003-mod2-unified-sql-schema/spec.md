# Feature: Módulo 2 — Estandarización de Esquema SQL (Unified Output)

> **Spec ID:** 003-mod2-unified-sql-schema
> **Autor:** Ricardo
> **Fecha:** 2026-05-14
> **Estado:** Draft

---

## Contexto

Los 10 queries del Módulo 2 retornan diferentes estructuras de columnas. Al
concatenarlos en Python, la tabla de "Hallazgos" en la UI colapsa con columnas
innecesarias y valores nulos. Se requiere un esquema de salida estrictamente
unificado a nivel SQL.

---

## Requisitos Funcionales

1. **RF-01:** Refactorizar los 10 archivos `.sql` en `sql/module_2_stats/` para
   que retornen EXACTAMENTE la misma estructura de columnas.
2. **RF-02:** Si un query no posee la información natural para una columna
   (ej. `LastCollectTimeStamp` en un query de tablas sin estadísticas), debe
   inyectar un casteo nulo explícito (ej. `CAST(NULL AS TIMESTAMP) AS LastCollectTimeStamp`).
3. **RF-03:** Cada query debe inyectar una columna estática llamada `FindingCategory`
   con el nombre de la regla (ej. `'Stale Statistics'`).

---

## Contrato de Datos (I/O — Esquema Obligatorio)

Todos los queries SQL DEBEN retornar estrictamente este DDL de salida
(en este orden exacto y con estos alias):

| # | Columna | Tipo | Descripción |
|---|---------|------|-------------|
| 1 | `DatabaseName` | VARCHAR | Nombre de la base de datos |
| 2 | `TableName` | VARCHAR | Nombre de la tabla |
| 3 | `ObjectName` | VARCHAR | ColumnName, IndexName o StatsName según aplique. Si aplica a toda la tabla: `'TABLE LEVEL'` |
| 4 | `FindingCategory` | VARCHAR | Nombre de la regla (ej. `'Missing Partition Stats'`) |
| 5 | `LastCollectTimeStamp` | TIMESTAMP | Última recolección. `CAST(NULL AS TIMESTAMP)` si no aplica |
| 6 | `RemediationDDL` | VARCHAR | Sentencia `COLLECT STATISTICS` lista para ejecutar |

---

## Criterios de Aceptación

### AC-01: Concatenación Limpia
- **Given** la ejecución de los 10 queries.
- **When** el Collector/Analyzer los unifica.
- **Then** el DataFrame final tiene exactamente 6 columnas, eliminando la explosión de campos.

### AC-02: FindingCategory presente en cada query
- **Given** cualquiera de los 10 archivos SQL.
- **When** se ejecuta individualmente.
- **Then** el resultado incluye una columna `FindingCategory` con valor estático
  correspondiente al nombre de la regla.

### AC-03: Casteo nulo explícito
- **Given** un query que no tiene información natural para `LastCollectTimeStamp`
  (ej. `04_missing_table.sql`).
- **When** se ejecuta.
- **Then** la columna `LastCollectTimeStamp` existe en el resultado con valor `NULL`
  (tipo TIMESTAMP), no con la columna ausente.

### AC-04: RemediationDDL generado en SQL
- **Given** cualquiera de los 10 archivos SQL.
- **When** se ejecuta.
- **Then** la columna `RemediationDDL` contiene una sentencia `COLLECT STATISTICS`
  válida construida con `||` (concatenación SQL), no generada en Python.

---

## Restricciones Arquitectónicas

- [ ] Modificar los 10 archivos `.sql` en `sql/module_2_stats/`
- [ ] Mantener compatibilidad con `BaseCollector.execute_query()` (retorna DataFrame)
- [ ] No agregar lógica de concatenación/renombrado en Python — todo se resuelve en SQL
- [ ] Preservar los placeholders `{placeholder}` existentes para filtros dinámicos
- [ ] SQL puro, sin SQL inline en Python

---

## Edge Cases

1. ¿Qué pasa si una tabla no tiene ColumnName? → `ObjectName = 'TABLE LEVEL'`.
2. ¿Qué pasa si el query retorna 0 filas? → DataFrame vacío con las 6 columnas del esquema.
3. ¿Qué pasa si `RemediationDDL` excede el límite de VARCHAR? → Truncar a 10000 chars.
4. ¿Qué pasa si `DatabaseName` o `TableName` contienen caracteres especiales? →
   Escapar con comillas dobles en `RemediationDDL`.

---

## Fuera de Alcance

- Cambios en el motor de 16 reglas (`analyzers/rules/`)
- Cambios en la Home Page (`ui/app.py`)
- Refactorización del Collector o Analyzer (los cambios son exclusivamente SQL)

---

## Archivos a Crear/Modificar

| Acción | Archivo | Descripción |
|--------|---------|-------------|
| Modificar | `sql/module_2_stats/01_unused_objects.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/02_sample_candidates.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/03_missing_partition.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/04_missing_table.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/05_missing_index.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/06_stale_stats.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/07_zero_stats.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/08_multicolumn.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/09_skipped_sample.sql` | Adaptar al esquema unificado de 6 columnas |
| Modificar | `sql/module_2_stats/10_dbc_recommendations.sql` | Adaptar al esquema unificado de 6 columnas |

---

## Notas Adicionales

- El esquema unificado permite que el Collector haga `pd.concat()` directamente
  sin explosión de columnas.
- La columna `FindingCategory` permite filtrar y agrupar hallazgos en la UI sin
  necesidad de mapeos adicionales en Python.
- La columna `RemediationDDL` generada en SQL elimina la necesidad de que el
  Analyzer o `DDLRecommender` construya las sentencias en Python.
