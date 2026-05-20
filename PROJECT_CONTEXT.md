# VantageOps — Project Context & State Document

> **Purpose:** This document serves as a system prompt to initialize full project context in an AI assistant session. It describes the architecture, current state, conventions, and critical rules for the VantageOps codebase.

---

## 1. Executive Summary

**VantageOps** is an enterprise-grade **Teradata Assessment Suite** — an observability and auditing platform for Teradata database environments. It is **not** a simple monitoring script. The platform evaluates cluster health, manages database statistics, analyzes space utilization, assesses security configurations, and generates actionable DDL remediation insights for Managed Services and Data Platform operations teams.

The tool targets Teradata DBAs and Data Engineers who need to:
- Audit and optimize database statistics for query performance improvement
- Monitor and manage space utilization across databases
- Evaluate security configurations and user access patterns
- Assess query logging and performance metrics
- Generate copy-paste-ready DDL remediation scripts (COLLECT STATISTICS, DROP TABLE, ALTER TABLE, etc.)

**Repository:** `https://github.com/dbacx/teradata-stats`

---

## 2. Tech Stack & Architecture

### 2.1 Runtime Stack

| Layer | Technology | Purpose |
|-------|-----------|---------|
| Frontend | **Streamlit** (≥1.28) | Web UI with `st.navigation` for multi-page routing |
| Data Processing | **Pandas** (≥2.0) | DataFrame manipulation, type casting, aggregation |
| Visualization | **Plotly** (exclusively) | All charts — gauges, treemaps, scatter, heatmaps, bar charts. **No Matplotlib.** |
| Database Driver | **teradatasql** (≥20.0) | Native Teradata SQL driver with Query Band injection |
| Config | **python-dotenv** (≥1.0) | Environment-based credential isolation |
| Reporting | **openpyxl**, **xlsxwriter**, **python-pptx** | Excel and PowerPoint export for audit evidence |
| SQL Parsing | **sqlparse** (≥0.4) | SQL formatting and analysis |

### 2.2 Architectural Pattern: Collector → Analyzer → UI (MVC)

Every module follows a strict three-layer pipeline:

```
sql/module_N_*/          →  collectors/modN_*_collector.py  →  analyzers/modN_*_analyzer.py  →  ui/pages/N_*.py
(Raw SQL scripts)           (Execute SQL, return Dict[str, DataFrame])  (Inject Severity, generate DDL)     (Render with Plotly/Streamlit)
```

**Layer responsibilities:**

1. **SQL Layer** (`sql/module_N_*/`): Raw `.sql` files with placeholder parameters (e.g., `{unused_days_threshold}`). These are the source of truth for all data extraction logic.

2. **Collector Layer** (`collectors/modN_*_collector.py`): Inherits from `core.base_collector.BaseCollector`. Reads SQL files from disk, replaces placeholders with runtime parameters, executes queries against the Teradata connection, and returns a `Dict[str, pd.DataFrame]` keyed by component name.

3. **Analyzer Layer** (`analyzers/modN_*_analyzer.py`): Inherits from `core.base_analyzer.BaseAnalyzer`. Receives the collector's dictionary, injects a `Severity` column into each DataFrame based on a strict `COMPONENT_SEVERITY` mapping, generates DDL remediation columns (`DDL_Statement`, `DDL_Action`, `Action_SQL`), and registers findings.

4. **UI Layer** (`ui/pages/N_*.py`): Renders the analyzed DataFrames using Plotly charts, `st.metric` KPI cards, `st.dataframe` tables, and `st.tabs`/`st.expander` for layout. Severity-aware color coding throughout.

### 2.3 Backend Patterns

- **RulesEngine** (`analyzers/engine.py`): Strategy Pattern implementation. Manages 16 registered `BaseStatsRule` instances for Module 2. Each rule is independently registered, configured, and executed. Supports `register_all_rules(config)` for batch registration with per-rule parameter overrides.

- **BaseStatsRule** (`analyzers/base_rule.py`): Abstract base for individual analysis rules (e.g., `Rule01Unused`, `Rule06Stale`, `Rule15Bloat`). Each rule implements an `evaluate()` method that processes a DataFrame and returns findings.

- **DDLRecommender** (`skills/recommender.py`): Generates COLLECT STATISTICS and DROP STATISTICS DDL statements from analysis results. Outputs copy-paste-ready Teradata SQL.

- **ExcelReporter** (`skills/reporter.py`): Generates multi-worksheet Excel reports for audit evidence.

- **PPTReporter** (`skills/ppt_reporter.py`): Generates executive summary PowerPoint presentations with per-rule findings.

- **Severity Enum** (`core/base_analyzer.py`): `CRITICAL`, `HIGH`, `MEDIUM`, `LOW`, `INFO`. Contract: Module 2 uses only the first four (no INFO). Module 3 uses all five.

### 2.4 SQL Injection Design

SQL queries live in `sql/module_N_*/` directories as standalone `.sql` files. Collectors read these files at runtime, replace `{placeholder}` tokens with actual values using Python's `str.format()`, then execute them via `teradatasql`. This design allows DBAs to edit SQL independently of Python code.

**SQL file inventory (140 total):**

| Module | Directory | SQL Files |
|--------|-----------|-----------|
| 1. System Information | `sql/module_1_health/` | 1 |
| 2. Statistics Management | `sql/module_2_stats/` | 15 |
| 3. Space Assessment | `sql/module_3_space/` | 11 |
| 4. Database Query Logging | `sql/module_4_dbql/` | 4 |
| 5. Performance Assessment | `sql/module_5_performance/` | 24 |
| 6. Schema Design | `sql/module_6_schema/` | 3 |
| 7. Hardware Utilization | `sql/module_7_hardware/` | 6 |
| 8. Cleanup & Cost Optimization | `sql/module_8_cleanup/` | 2 |
| 9. Monthly Report | `sql/module_9_monthly_report/` | 69 |
| 10. Security | `sql/module_10_security/` | 5 |

---

## 3. Authentication & Configuration

### 3.1 CLI Parameter Passing

The application is launched with a `--client` flag that determines which `.env` file to load:

```bash
streamlit run ui/main.py -- --client EPM
```

This triggers `load_client_environment()` (decorated with `@st.cache_resource`) which:
1. Parses `--client` from `argparse` (default: `"EPM"`)
2. Appends the project root to `sys.path`
3. Loads `{CLIENT_NAME}.env` from the project root (e.g., `EPM.env`)
4. Returns the client name string on success, `None` on failure

### 3.2 Environment Files

Environment files (`EPM.env`, `BCI.env`, etc.) contain Teradata connection credentials:

```env
TERADATA_HOST=<hostname>
TERADATA_USER=<username>
TERADATA_PASSWORD=<password>
TERADATA_DATABASE=<default_database>
```

These files are **not committed** to the repository (listed in `.gitignore`). The `core/connection.py` module reads these variables via `os.getenv()` and establishes connections with automatic Query Band injection (`App=TDStatsOpt;`) for application tracing.

### 3.3 System Database Exclusions

`core/config.py` loads a list of system databases to exclude from analysis from `core/system_databases.txt`. This prevents false positives from DBC, SYSADMIN, PDCRINFO, etc.

### 3.4 Thresholds

Global thresholds are defined in `core/config.py`:

```python
THRESHOLDS = {
    "stats_stale_days": 15,
    "pi_skew_pct": 30,
    "large_scan_size_gb": 10,
    "unused_object_days": 90,
    "space_critical_pct": 80,
    "space_warning_pct": 60,
}
```

---

## 4. Current MVP State (Strict Scope)

### 4.1 Active UI Modules

In `ui/main.py`, the navigation is configured with `st.navigation()` using a single `"MODULOS"` group key. **Only 3 pages are active:**

| Position | Variable | File | Title |
|----------|----------|------|-------|
| Home | `pg_home` | `ui/app.py` | Home |
| 1 | `pg_dbinfo` | `ui/pages/1_System_Information.py` | System Information |
| 2 | `pg_statistics` | `ui/pages/2_Statistics_Management.py` | Statistics Management |

```python
pages = {
    "MODULOS": [
        pg_home,           # Home
        pg_dbinfo,         # 1
        pg_statistics,     # 2
        # pg_space,        # 3  — commented out
        # pg_dbql,         # 4  — commented out
        # ...through 10
    ]
}
```

### 4.2 Home Page (`ui/app.py`)

The Home page serves dual purpose:
- **Corporate landing page** with HTML banner ("VantageOps — Managed Services"), welcome text, and a Teradata link
- **Legacy analysis panel** with sidebar controls (analysis level selector, thresholds, "Ejecutar Análisis" button) that triggers the original `RulesEngine` + `DDLRecommender` pipeline for Module 2

### 4.3 Module 2: Statistics Management

The most mature module. Full pipeline:
- **Collector** (`collectors/mod2_stats_collector.py`): Executes 15 SQL queries against `DBC.StatsV`, `DBC.TablesV`, `DBC.TableSizeV`, etc.
- **Analyzer** (`analyzers/mod2_stats_analyzer.py`): Strict severity mapping for 15 components (5 CRITICAL, 3 HIGH, 4 MEDIUM, 2 LOW, 1 MEDIUM for TDStats). Injects `Severity` column into every DataFrame.
- **UI** (`ui/pages/2_Statistics_Management.py`): KPI cards by severity, component render order by criticality, findings table with filters, DDL remediation scripts with `st.expander`.

**Severity Mapping (Module 2):**

| Severity | Components |
|----------|-----------|
| CRITICAL | Zero Stats, Missing Table, Missing Index, Sampled Skew, Stale by Volume |
| HIGH | Missing Partition, MLPPI Missing Levels, DBC Recommendations |
| MEDIUM | Multicolumn, Bloat, Sample Candidates, Skipped Sample, TDStats Recommendations |
| LOW | Unused Objects, Stale Statistics |

---

## 5. Hidden Core (Modules 3–10)

The backend infrastructure (collectors, analyzers, SQL files) for Modules 3 through 10 **exists and is functional**, but is intentionally disconnected from `st.navigation` in `ui/main.py` for this partial delivery. The physical files are present and importable:

| Module | Collector | Analyzer | UI Page | SQL Dir | Status |
|--------|-----------|----------|---------|---------|--------|
| 3. Space Assessment | `mod3_space_collector.py` | `mod3_space_analyzer.py` | `3_Space.py` | `module_3_space/` (11 files) | Backend complete. UI has 11-tab Plotly dashboard (CDS gauge, forecast, treemap, heatmap, scatter). Disconnected from nav. |
| 4. DBQL | `mod4_config_collector.py` | `mod4_config_analyzer.py` | `4_Database_Query_Logging.py` | `module_4_dbql/` (4 files) | Backend exists. |
| 5. Performance | `mod5_performance_collector.py` | `mod5_performance_analyzer.py` | `5_Performance_Assessment.py` | `module_5_performance/` (24 files) | Backend exists. |
| 6. Schema Design | `mod6_schema_collector.py` | `mod6_schema_analyzer.py` | `6_Schema_Design.py` | `module_6_schema/` (3 files) | Backend exists. |
| 7. Hardware | `mod7_hardware_collector.py` | `mod7_hardware_analyzer.py` | `7_Hardware_Utilization.py` | `module_7_hardware/` (6 files) | Backend exists. |
| 8. Cleanup | `mod8_cleanup_collector.py` | `mod8_cleanup_analyzer.py` | `8_Cleanup_Cost_Optimization.py` | `module_8_cleanup/` (2 files) | Backend exists. |
| 9. Monthly Report | `mod9_monthly_collector.py` | `mod9_monthly_analyzer.py` | `9_Monthly_Report.py` | `module_9_monthly_report/` (69 files) | Backend exists. |
| 10. Security | `mod10_security_collector.py` | `mod10_security_analyzer.py` | `10_Security.py` | `module_10_security/` (5 files) | Backend exists. |

### Module 3 Space Dashboard (Most Advanced Hidden Module)

The Space module has a fully built 11-component Plotly dashboard in `ui/pages/3_Space.py`:

1. **01_CDS_Report** — Gauge semicircular + progress bar + `st.metric` + alerts
2. **02_Space_Capacity_Forecast** — Line chart (historical/forecast/max capacity) + KPIs
3. **03_Suspected_Unused_Objects** — Horizontal bar chart (object count by DB) + Treemap + DDL download
4. **04_Suspected_Duplicate_Objects** — Sunburst + DataFrame
5. **05/06_MVC_Opportunities** — Comparative bar charts (current vs. compressed) + savings metric + DDL
6. **07_Top_20_Databases** — Stacked horizontal bars (color-coded by utilization %) + styled table
7. **08_Top_20_Tables** — Horizontal bars colored by Skew_Pct
8. **09_Top_20_Unused_Databases** — Scatter plot quadrant (days unused × size, bubble = object count)
9. **10_Monthly_Capacity_Snapshot** — Heatmap (MoM Growth %)
10. **11_Database_Space_Utilization** — Scatter with 45° reference line

**Defensive programming applied throughout:**
- `_normalise_columns(df)` helper renames `DatabaseName` → `DataBaseName`, `Tablename` → `TableName` (and all case variants)
- `_has_required_columns(df, required, view_name)` validates column existence with early `st.warning` return
- `pd.to_numeric(..., errors='coerce').fillna(0)` on all metric columns before any arithmetic or Plotly rendering

To re-enable any module, uncomment its `st.Page()` definition and its entry in the `pages` dictionary in `ui/main.py`.

---

## 6. UI/UX Standards

### 6.1 Global CSS Injection

`get_global_css()` in `ui/main.py` (decorated with `@st.cache_resource`) injects a `<style>` block via `st.markdown(..., unsafe_allow_html=True)`. Current rules:

| Rule | Selector | Effect |
|------|----------|--------|
| Broken icon fallback | `.material-symbols-rounded`, `.material-symbols-outlined` | Hide raw icon text (offline environments) |
| Sidebar collapse button | `[data-testid="stSidebarCollapseButton"] span::after` | Replace broken icon with `☰` |
| Expander arrow | `[data-testid="stExpanderToggleIcon"]::after` | Replace broken icon with `▼` |
| Primary buttons | `[data-testid="baseButton-primary"]` | **Teradata Orange `#F37021`** background, white text |
| Button hover | `[data-testid="baseButton-primary"]:hover` | Darker orange `#d9621b` |
| Sidebar nav typography | `[data-testid="stSidebar"] [data-testid="stSidebarNav"] a div` | `font-size: 1.1rem`, `font-weight: 500` |
| Paragraphs & lists | `p, li` | `font-size: 1rem`, `line-height: 1.6`, `color: #333333` |
| Headings | `h1, h2, h3` | `color: #1C1C1E` (near-black for contrast) |
| Anchor link icons | `[data-testid="stHeaderActionElements"]`, `h1 a, h2 a, h3 a`, `[data-testid="StyledLinkIconContainer"]` | `display: none` (hidden) |

### 6.2 Color Palette

| Color | Hex | Usage |
|-------|-----|-------|
| Teradata Orange (official) | `#F37021` | Primary buttons, brand accent |
| Button hover | `#d9621b` | Darker orange for hover state |
| Near-black | `#1C1C1E` | Headings |
| Dark grey | `#333333` | Body text (paragraphs, lists) |
| Light grey background | `#F0F2F6` | Banner background, subtle containers |
| Severity - Critical | `#E24B4A` | Red for critical findings |
| Severity - High | `#FC6623` | Orange for high findings |
| Severity - Medium | `#EF9F27` | Amber/yellow for medium |
| Severity - Low | `#4354E9` | Blue for low findings |
| Chart green | `#639922` | CDS gauge 0-70% zone, compressed size bars |
| Chart blue | `#378ADD` | Progress bars, informational |

### 6.3 Performance Optimization

- `@st.cache_resource` on `load_client_environment()` and `get_global_css()` — executed once per Streamlit process, not on every page rerun
- `pd.set_option("styler.render.max_elements", 2000000)` — prevents truncation on large DataFrames
- `st.set_page_config()` is the **first** Streamlit rendering command (Streamlit Golden Rule)

### 6.4 Bootstrap Sequence in `main.py`

```
1. pd.set_option (pure Python, no ST rendering)
2. load_client_environment() — @st.cache_resource
3. st.set_page_config() — FIRST Streamlit command
4. Validate client loaded (st.error + st.stop if None)
5. st.logo("logo.jpg")
6. st.markdown(get_global_css()) — inject cached CSS
7. Define st.Page instances
8. st.navigation(pages).run()
```

---

## 7. File Structure Reference

```
teradata-stats/
├── core/
│   ├── base_analyzer.py      # BaseAnalyzer ABC + Severity enum
│   ├── base_collector.py     # BaseCollector ABC (SQL read + execute)
│   ├── config.py             # THRESHOLDS dict + SYSTEM_DATABASES
│   ├── connection.py         # TeradataConnection (teradatasql + Query Band)
│   ├── schemas.py            # Teradata DBC view DDL documentation
│   └── system_databases.txt  # Exclusion list for system DBs
├── collectors/
│   ├── mod1_health_collector.py   # Module 1
│   ├── mod2_stats_collector.py    # Module 2 (15 SQL files)
│   ├── mod3_space_collector.py    # Module 3 (11 SQL files)
│   ├── mod4_config_collector.py   # Module 4
│   ├── mod5_performance_collector.py
│   ├── mod6_schema_collector.py
│   ├── mod7_hardware_collector.py
│   ├── mod8_cleanup_collector.py
│   ├── mod9_monthly_collector.py
│   ├── mod10_security_collector.py
│   ├── dictionary_ext.py     # Legacy stats extractor (used by Home)
│   └── dbql_ext.py           # DBQL data extractor
├── analyzers/
│   ├── engine.py             # RulesEngine (Strategy Pattern, 16 rules)
│   ├── base_rule.py          # BaseStatsRule ABC
│   ├── health_rules.py       # Legacy health rules
│   ├── mod1_health_analyzer.py
│   ├── mod2_stats_analyzer.py  # COMPONENT_SEVERITY + COMPONENT_LABELS
│   ├── mod3_space_analyzer.py  # 11 components, CDS ≤ Perm validation
│   ├── mod4_config_analyzer.py
│   ├── mod5_performance_analyzer.py
│   ├── mod6_schema_analyzer.py
│   ├── mod7_hardware_analyzer.py
│   ├── mod8_cleanup_analyzer.py
│   ├── mod9_monthly_analyzer.py
│   ├── mod10_security_analyzer.py
│   └── rules/
│       ├── rule_01_unused.py .. rule_16_urgent_missing.py  # 16 individual rules
├── skills/
│   ├── recommender.py        # DDLRecommender (COLLECT/DROP STATISTICS)
│   ├── reporter.py           # ExcelReporter (multi-worksheet audit)
│   └── ppt_reporter.py       # PPTReporter (executive presentations)
├── sql/
│   ├── module_1_health/      # 1 SQL file
│   ├── module_2_stats/       # 15 SQL files
│   ├── module_3_space/       # 11 SQL files
│   ├── module_4_dbql/        # 4 SQL files
│   ├── module_5_performance/ # 24 SQL files
│   ├── module_6_schema/      # 3 SQL files
│   ├── module_7_hardware/    # 6 SQL files
│   ├── module_8_cleanup/     # 2 SQL files
│   ├── module_9_monthly_report/ # 69 SQL files
│   └── module_10_security/   # 5 SQL files
├── ui/
│   ├── main.py               # Entry point: navigation, CSS, bootstrap
│   ├── app.py                # Home page (banner + legacy analysis)
│   └── pages/
│       ├── 1_System_Information.py
│       ├── 2_Statistics_Management.py
│       ├── 3_Space.py                   # (disconnected from nav)
│       ├── 4_Database_Query_Logging.py  # (disconnected)
│       ├── 5_Performance_Assessment.py  # (disconnected)
│       ├── 6_Schema_Design.py           # (disconnected)
│       ├── 7_Hardware_Utilization.py    # (disconnected)
│       ├── 8_Cleanup_Cost_Optimization.py # (disconnected)
│       ├── 9_Monthly_Report.py          # (disconnected)
│       └── 10_Security.py              # (disconnected)
├── utils/
│   └── csv_logger.py         # Audit CSV logging
├── requirements.txt
├── EPM.env                   # Client credentials (not committed)
├── BCI.env                   # Client credentials (not committed)
├── logo.jpg                  # Sidebar logo
└── .gitignore
```

---

## 8. Critical Technical Rules

1. **Case Sensitivity:** Teradata returns `DataBaseName` (capital B). All DataFrame column references in UI, analyzers, and DDL generation **must** use `DataBaseName`, not `DatabaseName`. The `_normalise_columns()` helper in `3_Space.py` handles this defensively.

2. **Severity Injection:** Every analyzer **must** inject a `Severity` column into each DataFrame before returning to the UI. The UI reads this column for KPI card counts and color-coded badges.

3. **CDS ≤ Perm Validation:** In any space capacity calculation, Compressed Data Storage (CDS) must always be ≤ Permanent Space. The Module 3 analyzer enforces this.

4. **Plotly Only:** All charts use Plotly (`go.*` or `px.*`). Matplotlib is **never** used.

5. **Defensive Type Casting:** All numeric columns must be cast with `pd.to_numeric(..., errors='coerce').fillna(0)` before arithmetic operations or Plotly rendering. Teradata may return numeric columns typed as `object`.

6. **Streamlit Golden Rule:** `st.set_page_config()` must be the **first** Streamlit rendering command in the script. All pure-Python setup (Pandas config, environment loading) must precede it.

7. **No CI Pipeline:** The repository has no CI/CD checks configured. Verification is done locally via `streamlit run ui/main.py -- --client EPM`.

---

## 9. Launch Command

```bash
cd teradata-stats/ui
streamlit run main.py -- --client EPM
```

The `--` separator is required to pass `--client` to the Python script rather than to Streamlit.
