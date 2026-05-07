import os
import re
import sqlparse
from pathlib import Path

# Directorio de archivos SQL
SQL_DIR = Path("sql/module_10_monthly_report")

# Encabezado corporativo estándar
CORPORATE_HEADER = """-- =============================================================================
-- Component   : Monthly Report Query
-- =============================================================================
-- Description : Monthly report query for Teradata DBA Services Framework
-- 
-- Version     : 1.0.0
-- Date        : 2026-05-05
-- Author      : Ricardo Enciso
-- Environment : Teradata 20
-- =============================================================================

"""

def has_corporate_header(content):
    """Verifica si el archivo ya tiene el encabezado corporativo"""
    return "-- Component   :" in content or "-- Component:" in content

def add_corporate_header(content, filename):
    """Agrega el encabezado corporativo al contenido"""
    # Extraer el nombre base del archivo para usar como componente
    component_name = filename.replace("_", " ").replace(".sql", "").title()
    
    header = CORPORATE_HEADER.replace("Monthly Report Query", component_name)
    return header + content

def format_sql(content):
    """Formatea el SQL usando sqlparse"""
    try:
        # Parsear el SQL
        parsed = sqlparse.parse(content)
        
        # Reformatar con keywords en mayúsculas y indentación
        formatted = sqlparse.format(
            str(parsed[0]),
            keyword_case='upper',
            reindent=True,
            reindent_aligned=True,
            indent_width=4,
            wrap_after=80
        )
        return formatted
    except Exception as e:
        print(f"Error formateando SQL: {e}")
        return content

def process_sql_file(filepath):
    """Procesa un archivo SQL individual"""
    try:
        # Leer el contenido
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Si el archivo está vacío o tiene solo espacios, saltar
        if not content.strip():
            print(f"Archivo vacío: {filepath.name}")
            return
        
        # Verificar si ya tiene encabezado corporativo
        if has_corporate_header(content):
            print(f"Ya tiene encabezado: {filepath.name}")
            # Solo formatear el SQL si no tiene encabezado
            # Si tiene encabezado, mantenerlo y formatear el resto
            lines = content.split('\n')
            header_end = 0
            for i, line in enumerate(lines):
                if line.strip().startswith("--") or line.strip() == "":
                    header_end = i + 1
                else:
                    break
            
            # Extraer encabezado y SQL
            header = '\n'.join(lines[:header_end])
            sql_content = '\n'.join(lines[header_end:])
            
            # Formatear solo el SQL
            formatted_sql = format_sql(sql_content)
            
            # Guardar con encabezado original + SQL formateado
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(header + '\n' + formatted_sql)
        else:
            # Agregar encabezado corporativo
            content_with_header = add_corporate_header(content, filepath.name)
            
            # Formatear el SQL
            formatted_content = format_sql(content_with_header)
            
            # Guardar
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(formatted_content)
            
            print(f"Procesado: {filepath.name}")
    
    except Exception as e:
        print(f"Error procesando {filepath.name}: {e}")

def main():
    """Función principal para procesar todos los archivos SQL"""
    # Obtener todos los archivos .sql
    sql_files = sorted(SQL_DIR.glob("*.sql"))
    
    print(f"Encontrados {len(sql_files)} archivos SQL")
    
    # Procesar cada archivo
    for sql_file in sql_files:
        process_sql_file(sql_file)
    
    print("Procesamiento completado")

if __name__ == "__main__":
    main()
