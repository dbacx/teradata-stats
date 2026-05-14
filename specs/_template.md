# Feature: [NOMBRE DEL FEATURE]

> **Spec ID:** NNN-nombre-corto
> **Autor:** [Nombre]
> **Fecha:** YYYY-MM-DD
> **Estado:** Draft | In Review | Approved | Implemented

---

## Contexto

Descripción breve del problema o necesidad que motiva este feature.

---

## Requisitos Funcionales

1. **RF-01:** [Descripción del requisito]
2. **RF-02:** [Descripción del requisito]
3. **RF-03:** [Descripción del requisito]

---

## Criterios de Aceptación

### AC-01: [Nombre del criterio]
- **Given** [estado inicial del sistema]
- **When** [acción del usuario o evento]
- **Then** [resultado esperado verificable]

### AC-02: [Nombre del criterio]
- **Given** [estado inicial del sistema]
- **When** [acción del usuario o evento]
- **Then** [resultado esperado verificable]

### AC-03: [Nombre del criterio]
- **Given** [estado inicial del sistema]
- **When** [acción del usuario o evento]
- **Then** [resultado esperado verificable]

---

## Restricciones Arquitectónicas

Verificar contra `.windsurfrules` antes de implementar:

- [ ] SQL puro en `sql/module_XX_nombre/` (sin SQL inline en Python)
- [ ] Collector hereda de `core.base_collector.BaseCollector`
- [ ] Analyzer hereda de `core.base_analyzer.BaseAnalyzer`
- [ ] UI Page en `ui/pages/` con botón "Ejecutar Analisis" + CSS `#FD6724`
- [ ] `try...except` en cada query del Collector (tolerancia a fallos)
- [ ] Verificación de DataFrame vacío y columnas requeridas en el Analyzer
- [ ] Renderizado con `isinstance()` + `st.tabs` si el resultado es `Dict[str, pd.DataFrame]`
- [ ] Registrar página en `ui/main.py` → `st.navigation()`

---

## Edge Cases

1. ¿Qué pasa si la query retorna 0 filas?
2. ¿Qué pasa si una columna esperada no existe en el resultado?
3. ¿Qué pasa si la conexión a Teradata falla a mitad del proceso?
4. ¿Qué pasa si el usuario no tiene permisos sobre las vistas requeridas?

---

## Fuera de Alcance

- [Lo que explícitamente NO se incluye en este feature]

---

## Archivos a Crear/Modificar

| Acción | Archivo | Descripción |
|--------|---------|-------------|
| Crear  | `sql/module_XX_nombre/*.sql` | Queries SQL |
| Crear  | `collectors/modXX_nombre_collector.py` | Collector |
| Crear  | `analyzers/modXX_nombre_analyzer.py` | Analyzer |
| Crear  | `ui/pages/XX_Nombre.py` | Página UI |
| Modificar | `ui/main.py` | Registrar nueva página |

---

## Notas Adicionales

[Cualquier contexto adicional, referencias a documentación de Teradata, etc.]
