#!/bin/bash

# Obtener el nombre de la rama actual
CURRENT_BRANCH=$(git branch --show-current)

# Validar que no estemos en main
if [ "$CURRENT_BRANCH" == "main" ]; then
    echo "Error: Ya estás en la rama main. Ejecuta este script desde tu rama de trabajo (feature/X)."
    exit 1
fi

# Validar que se haya enviado un mensaje de commit
if [ -z "$1" ]; then
    echo "Error: Debes proporcionar un mensaje de commit."
    echo "Uso: ./git_save.sh \"tu mensaje de commit aquí\""
    exit 1
fi

MESSAGE=$1

echo "1. Guardando cambios en la rama local ($CURRENT_BRANCH)..."
git add .
git commit -m "$MESSAGE"

echo "2. Cambiando a main y fusionando..."
git checkout main
git merge $CURRENT_BRANCH

echo "3. Subiendo ramas a GitHub..."
git push origin main
git push origin $CURRENT_BRANCH

echo "========================================="
echo "¡Proceso finalizado con éxito!"
echo "========================================="

#--comando: /git_save.sh "feat: descripcion de los cambios que hiciste"