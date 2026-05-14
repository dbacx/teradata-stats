# Feature: Módulo 2 - Refactorización Integral (Schema, Tipos, KPIs y UI)

## Contexto
El Módulo 2 presenta fallas estructurales: colapsa en tiempo de ejecución al validar estructuras de datos (`AttributeError`), sufre de explosión de columnas al concatenar diferentes queries, renderiza categorías de menor valor (INFO) y no prioriza los hallazgos según el impacto real en el optimizador de Teradata. Se requiere una reestructuración de extremo a extremo.

## Requisitos Funcionales
1. RF-01: Refactorizar los 10 archivos `.sql` en `sql/module_2_stats/` para que retornen un esquema de columnas idéntico.
2. RF-02: Modificar el Analyzer y la UI para que iteren de forma segura sobre diccionarios (`Dict[str, pd.DataFrame]`), eliminando el uso de `.empty` sobre estructuras que no son DataFrames.
3. RF-03: Eliminar por completo el cálculo y renderizado de la severidad "INFO".
4. RF-04: Eliminar la pestaña "Datos Analizados" de la interfaz.
5. RF-05: Forzar el orden de visualización de los hallazgos basándose en la criticidad técnica de Teradata.

## Contratos Estrictos

### Contrato 1: Datos SQL (Esquema Obligatorio)
Los 10 queries SQL DEBEN retornar estrictamente este DDL de salida (en este orden y con estos alias). Si el query no tiene el dato, usar `CAST(NULL AS [TIPO])` o strings estáticos:
1. `DatabaseName` (VARCHAR)
2. `TableName` (VARCHAR)
3. `ObjectName` (VARCHAR)
4. `FindingCategory` (VARCHAR) -> Nombre de la regla.
5. `LastCollectTimeStamp` (TIMESTAMP)
6. `RemediationDDL` (VARCHAR)

### Contrato 2: Presentación (Orden Obligatorio)
Tanto las tarjetas de KPI como la iteración de tablas en la UI DEBEN seguir este orden exacto:
1. 'Missing PARTITION'
2. 'Missing Table Stats'
3. 'Missing Index Stats'
4. 'Zero Statistics'
5. 'Stale Statistics'
6. 'Multicolumn Issues'
7. 'Sample Candidates'
8. 'Skipped/Sample'
9. 'Unused Objects'
10. 'DBC Recommendations'

## Criterios de Aceptación

### AC-01: Concatenación y Tipos Seguros
- **Given** la ejecución de los queries.
- **When** el Collector y Analyzer procesan los datos.
- **Then** el DataFrame final tiene exactamente 6 columnas y la validación en la UI usa `isinstance(findings, dict)` antes de iterar, evitando cualquier `AttributeError`.

### AC-02: Limpieza de Tabs y Severidad
- **Given** el renderizado de la interfaz.
- **When** se construyen los componentes visuales.
- **Then** solo existen las pestañas `["Hallazgos", "Scripts de Remediación"]` y las tarjetas de KPI se limitan a 4 columnas: CRITICAL, HIGH, MEDIUM, LOW.

### AC-03: Iteración por Prioridad
- **Given** el diccionario de resultados.
- **When** la UI renderiza los hallazgos.
- **Then** el código itera sobre la lista definida en el "Contrato 2", renderizando en ese orden específico. Si una categoría está vacía, no rompe el flujo, simplemente muestra cero o un mensaje de "Sin hallazgos".