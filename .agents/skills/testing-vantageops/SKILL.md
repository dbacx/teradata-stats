---
name: testing-vantageops
description: Test the VantageOps Streamlit app end-to-end. Use when verifying UI changes, connection flow, or CSV migration.
---

# Testing VantageOps Streamlit App

## Prerequisites

- Python 3.x with dependencies installed (`pip install -r requirements.txt`)
- `teradatasql` package installed (required even without a real DB)

## Starting the App

```bash
cd /path/to/teradata-stats
streamlit run ui/main.py
```

The app runs at `http://localhost:8501`.

**Important:** If you get `ModuleNotFoundError: No module named 'core'`, check that `ui/main.py` sets `sys.path` BEFORE any `from core.xxx import` statements. Python executes module-level imports immediately — putting sys.path setup inside a function that runs later will NOT work.

## Connection System

The app uses `config/connections.csv` for connection credentials. Columns: `Customer Name, System Name, Site ID, Host Name, User Name, Password`.

To generate a template:
```bash
python scripts/create_connections_template.py
```

## Key Test Scenarios

### 1. App Startup (no .env dependency)
- Navigate to `http://localhost:8501`
- Verify: No red error about `.env` or "Error Critico de Configuracion"
- Verify: Sidebar shows "Conexión" section with Cliente/Sistema dropdowns and Conectar button

### 2. Missing connections.csv (soft warning)
- Rename/remove `config/connections.csv`
- Refresh the app
- Verify: Yellow warning "No se encontró config/connections.csv" with setup instructions
- Verify: App does NOT crash (no `st.stop()`)
- Restore the file afterward

### 3. Connection Error Handling
- Click "Conectar" with the example/template credentials
- Verify: Error message like "Error de conexión: Teradata connection failed..."
- Verify: No `.env` mentions in the error
- Verify: App stays functional (no crash)
- Note: Without a real Teradata DB, you cannot test successful connections

### 4. Connect-First Guard on Pages
- Navigate to System Information or Statistics Management
- Click "Ejecutar Analisis" / "Ejecutar" without connecting
- Verify: Red error "Conecta primero desde el sidebar."
- Verify: No traceback about missing env vars

### 5. Code Cleanliness (shell)
```bash
grep -r "load_dotenv" ui/ core/ --include="*.py"   # Must be empty
grep -r "\.env" ui/ core/ --include="*.py"          # Must be empty
```

## Known Limitations

- No real Teradata DB is available in the test environment. Connection flow can only be verified up to the driver's hostname resolution attempt.
- The `.streamlit/config.toml` has `headless = false` — Streamlit will try to open a browser on startup. Set to `true` for headless/CI environments.
- Streamlit caches connections with `@st.cache_resource`. If you change `connections.csv`, you may need to clear cache or restart the app.

## Devin Secrets Needed

None — no real Teradata credentials are needed for basic UI/flow testing. For full integration testing, you would need Teradata host, username, and password configured in `config/connections.csv`.
