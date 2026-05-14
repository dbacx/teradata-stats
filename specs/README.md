# Spec-Driven Development (SDD)

Este directorio contiene las especificaciones del proyecto siguiendo la metodología
**Spec-Driven Development**.

## Flujo de Trabajo

```
1. SPECIFY  →  Crear specs/NNN-feature/spec.md usando _template.md
2. CLARIFY  →  Iterar sobre la spec hasta eliminar toda ambigüedad
3. PLAN     →  Crear research.md + tasks.md con tareas atómicas
4. IMPLEMENT → Codificar siguiendo la spec y las restricciones de .windsurfrules
5. REVIEW   →  Verificar el PR contra los criterios de aceptación de la spec
```

## Estructura

```
specs/
├── README.md                          # Este archivo
├── _template.md                       # Template para nuevas specs
└── NNN-feature-name/                  # Una carpeta por feature
    ├── spec.md                        # PRD con criterios de aceptación
    ├── research.md                    # Decisiones técnicas
    └── tasks.md                       # Tareas atómicas implementables
```

## Crear una nueva spec

1. Copiar `_template.md` a `specs/NNN-nombre/spec.md`
2. Completar todos los campos, especialmente:
   - Criterios de aceptación con Given/When/Then
   - Checklist de restricciones arquitectónicas
   - Edge cases
3. Iterar hasta que no quede ningún `[NEEDS CLARIFICATION]`
4. Crear `research.md` y `tasks.md`
5. Implementar siguiendo las tareas

## Constitution

Los principios inmutables del proyecto están en `.specify/memory/constitution.md`,
que referencia a `.windsurfrules` como fuente de verdad.
