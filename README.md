# 📐 Arquitectura del Proyecto TD-Stats-Optimizer

Una herramienta analítica automatizada en Python para optimizar la recolección de estadísticas en bases de datos Teradata.

---

## 📂 Estructura General

```
teradata-stats/
├── core/                    # Capa de conexión y seguridad
│   └── connection.py        # Gestión de conexiones a Teradata
├── collectors/              # Capa de extracción de datos
│   └── dictionary_ext.py    # Extractor de metadata de diccionario
├── analyzers/               # Capa de análisis
│   └── health_rules.py      # Motor de reglas (detección de anomalías)
├── skills/                  # Capa de recomendaciones
│   └── recommender.py       # Generador de DDLs y reportes
├── ui/                      # Interfaz de usuario
│   └── app.py              # Dashboard Streamlit
├── main_cli.py             # Interfaz CLI
└── requirements.txt        # Dependencias
```

---

## 🔄 Flujo de Datos (Pipeline)

```
┌─────────────────────────────────────────────────────────────┐
│  1. CORE - Conexión y Seguridad                             │
│     └─ Conecta a Teradata con credenciales (.env)           │
│     └─ Inyecta Query Band para auditoría                    │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│  2. COLLECTORS - Extracción Masiva de Metadata              │
│     └─ DBC.StatsV     → Estadísticas de tablas              │
│     └─ DBC.TablesV    → Información de tablas               │
│     └─ DBC.ObjectUsage→ Uso de objetos                      │
│     └─ Output: DataFrame de Pandas                          │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│  3. ANALYZERS - Motor de Reglas                             │
│     └─ Detectar estadísticas obsoletas (> 15 días)          │
│     └─ Identificar diccionario inflado (> 50 stats/tabla)   │
│     └─ Aplicar validaciones con Pandas                      │
│     └─ Output: Lista de anomalías detectadas                │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│  4. SKILLS - Generación de Acciones                         │
│     └─ Generar COLLECT STATISTICS (refrescar stats)         │
│     └─ Generar DROP STATISTICS (limpiar bloat)              │
│     └─ Crear reportes de recomendaciones                    │
│     └─ Output: SQL DDLs y reportes                          │
└────────────────┬────────────────────────────────────────────┘
                 │
┌────────────────▼────────────────────────────────────────────┐
│  5. UI - Visualización (Streamlit o CLI)                    │
│     └─ Dashboard interactivo (web)                          │
│     └─ O ejecución CLI (headless)                           │
└─────────────────────────────────────────────────────────────┘
```

---

## 🏗️ Componentes Clave

| Módulo | Responsabilidad | Entrada | Salida |
|--------|-----------------|---------|--------|
| **core/connection.py** | Conexión a Teradata, Query Band, manejo de credenciales | Variables `.env` | Conexión activa |
| **collectors/dictionary_ext.py** | Extrae metadata del diccionario Teradata | Conexión + DB name | DataFrame con estadísticas |
| **analyzers/health_rules.py** | Detecta anomalías (stats obsoletas, bloat) | DataFrame de stats | Análisis de salud |
| **skills/recommender.py** | Genera DDLs y recomendaciones | Resultados del análisis | SQL scripts + reportes |
| **ui/app.py** | Dashboard Streamlit interactivo | API de módulos | Interfaz gráfica |
| **main_cli.py** | Orquestador CLI | Entrada del usuario | Reportes en archivos |

---

## 📊 Descripción de Capas

### 1️⃣ CORE - Capa de Conexión y Seguridad

**Archivo:** `core/connection.py`

**Responsabilidades:**
- Establecer conexión a Teradata
- Inyectar Query Band para auditoría y trazabilidad
- Gestionar credenciales de forma segura desde variables de entorno
- Validar la conexión

**Características:**
- Uso de `python-dotenv` para credenciales seguras
- Query Band para tracking de operaciones
- Manejo de excepciones de conexión

---

### 2️⃣ COLLECTORS - Capa de Extracción de Datos

**Archivo:** `collectors/dictionary_ext.py`

**Responsabilidades:**
- Extraer metadata del diccionario de Teradata
- Recopilar información de estadísticas: `DBC.StatsV`
- Obtener datos de tablas: `DBC.TablesV`
- Analizar uso de objetos: `DBC.ObjectUsage`

**Output:**
- DataFrame de Pandas con toda la información compilada
- Datos listos para análisis

---

### 3️⃣ ANALYZERS - Capa de Análisis

**Archivo:** `analyzers/health_rules.py`

**Responsabilidades:**
- Detectar estadísticas obsoletas (no actualizadas en > 15 días)
- Identificar diccionario inflado (más de 50 estadísticas por tabla)
- Aplicar reglas de negocio personalizadas
- Generar reportes de anomalías

**Reglas Implementadas:**
- ✅ Detección de stats stale (antiguas)
- ✅ Detección de bloat (inflación de diccionario)
- ✅ Validaciones de integridad

---

### 4️⃣ SKILLS - Capa de Acciones Recomendadas

**Archivo:** `skills/recommender.py`

**Responsabilidades:**
- Generar sentencias `COLLECT STATISTICS` para stats obsoletas
- Generar sentencias `DROP STATISTICS` para limpiar bloat
- Crear reportes ejecutivos con recomendaciones
- Exportar resultados en formato SQL

**Output:**
- Archivos SQL con DDLs optimizados
- Reportes de recomendaciones
- Documentación de hallazgos

---

### 5️⃣ UI - Capa de Presentación

#### Opción A: Dashboard Web (Streamlit)

**Archivo:** `ui/app.py`

**Características:**
- Interfaz interactiva con Streamlit
- Visualización de gráficos y tablas
- Descarga de reportes SQL
- Monitoreo en tiempo real

#### Opción B: Interfaz CLI

**Archivo:** `main_cli.py`

**Características:**
- Ejecución sin dependencias de interfaz gráfica
- Ideal para automatización y scripts
- Generación de archivos SQL timestamped
- Resumen de análisis en consola

---

## 🛠️ Stack Tecnológico

| Tecnología | Propósito | Versión |
|------------|----------|---------|
| **Python** | Lenguaje principal | 3.10+ |
| **teradatasql** | Conexión a Teradata | Última |
| **pandas** | Procesamiento de datos | Última |
| **python-dotenv** | Gestión de credenciales | Última |
| **streamlit** | Interfaz web | Última |

---

## 📋 Instalación

### Requisitos Previos
- Python 3.10 o superior
- Acceso a una base de datos Teradata

### Pasos de Instalación

1. **Clonar el repositorio**
```bash
git clone https://github.com/dbacx/teradata-stats.git
cd teradata-stats
```

2. **Crear archivo de configuración**
```bash
cp .env.example .env
```

3. **Configurar credenciales en `.env`**
```
TERADATA_HOST=your_server
TERADATA_USER=your_username
TERADATA_PASSWORD=your_password
TERADATA_DATABASE=your_database
```

4. **Instalar dependencias**
```bash
pip install -r requirements.txt
```

---

## 🚀 Uso

### Opción 1: Interfaz Web (Streamlit)

```bash
streamlit run ui/app.py
```

Se abrirá un dashboard interactivo en `http://localhost:8501`

### Opción 2: Interfaz CLI

```bash
python main_cli.py
```

Ingresa el nombre de la base de datos cuando se solicite.

### Uso Programático

```python
from core.connection import create_connection
from collectors.dictionary_ext import extract_database_stats
from analyzers.health_rules import StatsAnalyzer
from skills.recommender import DDLRecommender

# 1. Conectar
conn = create_connection()

# 2. Extraer datos
df_stats = extract_database_stats("my_database")

# 3. Analizar
analyzer = StatsAnalyzer()
stale_stats = analyzer.detect_stale_stats(df_stats, days_threshold=15)
bloat = analyzer.detect_dictionary_bloat(df_stats, max_stats_per_table=50)

# 4. Generar recomendaciones
recommender = DDLRecommender()
collect_ddls = recommender.generate_collect_stats(stale_stats)
drop_ddls = recommender.generate_drop_stats(bloat)
```

---

## 📊 Análisis Implementados

### 1. Detección de Estadísticas Obsoletas
- **Método:** Comparar fecha de última actualización vs umbral (default: 15 días)
- **Acción:** Generar sentencias `COLLECT STATISTICS`
- **Beneficio:** Mantener optimizador de Teradata actualizado

### 2. Detección de Diccionario Inflado
- **Método:** Contar estadísticas por tabla vs umbral (default: 50)
- **Acción:** Generar sentencias `DROP STATISTICS` para redundantes
- **Beneficio:** Reducir overhead de mantenimiento

### 3. Validación de Integridad
- **Método:** Verificar consistencia de metadata
- **Acción:** Reportar inconsistencias
- **Beneficio:** Mantener diccionario saludable

---

## 📁 Archivos Generados

### Archivos SQL
```
collect_stats_mydb_20260427_120000.sql
drop_stats_mydb_20260427_120000.sql
```

### Reportes (si aplica)
- Resumen de análisis
- Recomendaciones por acción
- Logs de ejecución

---

## 🔐 Seguridad

- ✅ Credenciales en variables de entorno (`.env`)
- ✅ Query Band para auditoría de operaciones
- ✅ Conexiones encriptadas a Teradata
- ✅ No almacena credenciales en código

---

## 🤝 Contribuciones

Las contribuciones son bienvenidas. Por favor:

1. Fork el repositorio
2. Crea una rama para tu feature (`git checkout -b feature/AmazingFeature`)
3. Commit tus cambios (`git commit -m 'Add some AmazingFeature'`)
4. Push a la rama (`git push origin feature/AmazingFeature`)
5. Abre un Pull Request

---

## 📝 Licencia

Este proyecto está bajo la licencia MIT. Ver el archivo `LICENSE` para más detalles.

---

## 👤 Autor

**dbacx** - [GitHub Profile](https://github.com/dbacx)

---

## 📞 Soporte

Para reportar bugs o solicitar features, abre un issue en:
https://github.com/dbacx/teradata-stats/issues
