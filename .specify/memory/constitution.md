# Constitution — TD-Stats-Optimizer

> Este archivo es el enlace canónico entre Spec-Driven Development y las reglas
> del proyecto. La fuente de verdad para principios arquitectónicos,
> estándares de código y restricciones de desarrollo es:
>
> **[`.windsurfrules`](../../.windsurfrules)**

## Principios Inmutables

1. **Separación MVC estricta**: SQL en `sql/`, extracción en `collectors/`,
   lógica en `analyzers/`, visualización en `ui/pages/`.

2. **Herencia obligatoria**: Collectors heredan de `BaseCollector`,
   Analyzers heredan de `BaseAnalyzer`, Reglas heredan de `BaseStatsRule`.

3. **Tolerancia a fallos**: `try...except` obligatorio en Collectors y Analyzers.
   Un componente que falla no debe detener a los demás.

4. **Performance Teradata**: Toda lógica analítica en memoria con Pandas.
   Prohibido iterar fila por fila para ejecutar DDL contra Teradata.

5. **Seguridad**: Credenciales exclusivamente vía `.env` + `python-dotenv`.
   Prohibido hardcodear credenciales. Query Band inyectado en cada sesión.

6. **UI corporativa**: Botón "Ejecutar Analisis" con CSS `#FD6724`.
   Renderizado con `st.tabs` + `isinstance()` para comprobación de tipos.

7. **Stack**: Python 3.10+, teradatasql, pandas, streamlit, plotly,
   python-pptx, openpyxl.

## Referencia Completa

Para el catálogo completo de reglas (10 módulos, 16 reglas del motor,
convenciones de código, navegación UI), consultar `.windsurfrules`.
