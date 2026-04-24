# TD-Stats-Optimizer: Framework de Automatización de Estadísticas Teradata

Una herramienta analítica automatizada en Python para optimizar la recolección de estadísticas en bases de datos Teradata.

## Arquitectura

- **core**: Conexión, inyección de Query Band, seguridad de credenciales
- **collectors**: Extracción masiva de metadata (DBC.StatsV, DBC.TablesV, DBC.ObjectUsage)
- **analyzers**: Motor de reglas usando Pandas para aplicar validaciones
- **skills**: Acciones resultantes (DDLs, reportes, recomendaciones)
- **ui**: Interfaz de Streamlit

## Stack Tecnológico

- Python 3.10+
- teradatasql (Conexión a Teradata)
- pandas (Procesamiento en memoria)
- python-dotenv (Variables de entorno)
- streamlit (Interfaz gráfica)

## Instalación

```bash
pip install -r requirements.txt
```

## Configuración

Crear archivo `.env` con las credenciales de Teradata:

```
TERADATA_HOST=your_server
TERADATA_USER=your_username
TERADATA_PASSWORD=your_password
TERADATA_DATABASE=your_database
```

## Uso

```python
from core.connection import create_connection

conn = create_connection()
if conn.test_connection():
    print("Conexión exitosa a Teradata")
```
