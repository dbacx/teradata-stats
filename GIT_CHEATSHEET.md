# 📘 Git Workflow & Commands Cheatsheet
**Proyecto:** Teradata Stats Optimizer
**Rol:** DBA / Cloud Ops Engineer

Esta guía documenta los comandos esenciales para el flujo de trabajo (workflow) utilizado en el desarrollo modular de esta herramienta.

---

## 🌳 1. Gestión de Ramas (Branches)

Las ramas permiten desarrollar funcionalidades de forma aislada sin afectar la versión estable en producción (`main`).

*   `git branch`
    *   **Qué hace:** Lista todas las ramas locales. La rama actual tiene un asterisco (`*`).
*   `git branch -a`
    *   **Qué hace:** Lista todas las ramas (locales y las remotas en GitHub).
*   `git checkout -b feature/nombre-del-modulo`
    *   **Qué hace:** Crea una nueva rama y cambia tu entorno de trabajo a ella inmediatamente. Todo el código nuevo debe desarrollarse aquí.
*   `git checkout main`
    *   **Qué hace:** Cambia tu entorno de trabajo de vuelta a la rama principal. (Útil para preparar integraciones).

## 💾 2. Guardar el Trabajo (Commits)

Un commit es una "fotografía" o punto de control de tu código en un momento específico.

*   `git status`
    *   **Qué hace:** Muestra el estado actual: qué archivos fueron modificados, cuáles son nuevos (untracked) y en qué rama estás. **(¡Úsalo siempre antes de hacer un commit!)**
*   `git add .`
    *   **Qué hace:** Prepara (stage) **todos** los archivos modificados y nuevos para ser guardados en el próximo commit.
*   `git commit -m "feat: descripcion clara del cambio"`
    *   **Qué hace:** Guarda los cambios permanentemente en el historial local de la rama con un mensaje descriptivo.
    *   *Tip:* Usa prefijos estándar como `feat:` (nueva función), `fix:` (corrección de bug), o `docs:` (documentación).

## 🔀 3. Integración (Merge)

Proceso para unir el código de tu rama de desarrollo a la línea principal.

1.  `git checkout main` (Asegúrate de estar en main).
2.  `git merge feature/nombre-del-modulo`
    *   **Qué hace:** Trae todos los commits de la rama `feature` y los aplica sobre `main`.

## ☁️ 4. Sincronización con la Nube (GitHub)

Comandos para enviar o traer código entre tu computadora local y el repositorio remoto (origin).

*   `git push origin main`
    *   **Qué hace:** Sube los commits de tu rama local `main` a la rama `main` en GitHub.
*   `git push -u origin feature/nombre-del-modulo`
    *   **Qué hace:** Sube una rama nueva a GitHub por primera vez y las vincula (upstream).
*   `git pull origin main`
    *   **Qué hace:** Descarga los cambios que existen en GitHub y los aplica a tu computadora local. (Vital si trabajas desde diferentes computadoras o si alguien más modificó el código en la web).

## 🚨 5. Auditoría y Solución de Problemas

*   `git log --oneline --graph --all`
    *   **Qué hace:** Muestra un "árbol" visual y compacto de todo el historial de commits y cómo se cruzan las ramas.
*   `git merge --abort`
    *   **Qué hace:** Si ocurre un conflicto durante un merge o la terminal se bloquea (ej. atrapado en Vim), este comando cancela el proceso y te devuelve al estado anterior.