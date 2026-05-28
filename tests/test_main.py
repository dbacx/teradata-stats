import pytest
import pandas as pd
from unittest.mock import patch, MagicMock
from streamlit.testing.v1 import AppTest

# ---------------------------------------------------------------------------
# Mock Data
# ---------------------------------------------------------------------------
MOCK_CONNECTIONS_DF = pd.DataFrame({
    "Customer Name": ["ClienteDemo"],
    "System Name": ["VantageProd"],
    "Site ID": ["SITIO-001"],
    "Host Name": ["vantage.local"],
    "User Name": ["dbc"],
    "Password": ["secret"]
})

MOCK_PARAMS = {
    "customer": "ClienteDemo",
    "system": "SITIO-001",
    "host": "vantage.local"
}

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------
@pytest.fixture
def app():
    """Inicializa la aplicación Streamlit aislada para pruebas."""
    # Asegúrate de que la ruta apunte correctamente a tu entrypoint
    return AppTest.from_file("main.py").run()


# ---------------------------------------------------------------------------
# Test Cases
# ---------------------------------------------------------------------------
@patch("core.connections_manager.load_connections")
def test_missing_csv_soft_warning(mock_load, app):
    """Escenario 2: Prueba la advertencia cuando no existe connections.csv"""
    # Simulamos que la función devuelve un DataFrame vacío (archivo no existe o sin datos)
    mock_load.return_value = pd.DataFrame()
    
    app.run()
    
    assert not app.exception, "La aplicación crasheó inesperadamente."
    assert len(app.sidebar.warning) > 0, "No se mostró la advertencia en el sidebar."
    assert "No se encontró config/connections.csv" in app.sidebar.warning[0].value

@patch("core.connections_manager.load_connections")
def test_app_startup_with_csv(mock_load, app):
    """Escenario 1: Prueba la carga correcta del sidebar con datos del CSV."""
    mock_load.return_value = MOCK_CONNECTIONS_DF
    
    app.run()
    
    assert not app.exception
    # Verifica que el dropdown de clientes se pobló correctamente
    assert app.sidebar.selectbox(key="sb_customer").value == "ClienteDemo"

@patch("core.connections_manager.load_connections")
@patch("core.connection.create_connection_from_params")
@patch("core.connections_manager.get_connection_params")
def test_connection_error_handling(mock_get_params, mock_create_conn, mock_load, app):
    """Escenario 3: Prueba el manejo de excepciones al fallar la conexión a Teradata."""
    mock_load.return_value = MOCK_CONNECTIONS_DF
    mock_get_params.return_value = MOCK_PARAMS
    # Forzamos una excepción simulando que Teradata está inaccesible
    mock_create_conn.side_effect = Exception("Teradata Network Unreachable")
    
    app.run()
    
    # Simula el clic en el botón 'Conectar' (tipo primary)
    app.sidebar.button[0].click().run()
    
    assert not app.exception, "La aplicación crasheó por la excepción de Teradata."
    assert app.session_state["td_connected"] == False
    assert len(app.sidebar.error) > 0
    assert "Error de conexión: Teradata Network Unreachable" in app.sidebar.error[0].value

@patch("core.connections_manager.load_connections")
@patch("core.connection.create_connection_from_params")
@patch("core.connections_manager.get_connection_params")
def test_successful_connection_mock(mock_get_params, mock_create_conn, mock_load, app):
    """Prueba adicional: Valida el flujo exitoso inyectando un objeto de conexión falso."""
    mock_load.return_value = MOCK_CONNECTIONS_DF
    mock_get_params.return_value = MOCK_PARAMS
    
    # Creamos un mock del objeto de conexión de Teradata
    mock_conn = MagicMock()
    mock_create_conn.return_value = mock_conn
    
    app.run()
    app.sidebar.button[0].click().run()
    
    assert not app.exception
    assert app.session_state["td_connected"] == True
    assert app.session_state["td_conn"] == mock_conn
    assert len(app.sidebar.success) > 0
    assert "Conectado a vantage.local" in app.sidebar.success[0].value