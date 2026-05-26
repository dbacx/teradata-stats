"""
Teradata Database Connection Module

This module provides secure connection functionality to Teradata databases
with proper Query Band injection for application tracing and credential management.
"""

import logging
from typing import Optional, Dict, Any
import teradatasql

# Configure logging
from core.logging_config import configure_logging
configure_logging()
logger = logging.getLogger(__name__)


class TeradataConnection:
    """
    Manages Teradata database connections with security and tracing capabilities.
    """
    
    def __init__(self, host: Optional[str] = None, user: Optional[str] = None, 
                 password: Optional[str] = None, database: Optional[str] = None):
        
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.query_band = 'App=TDStatsOpt;'
        
        if not all([self.host, self.user, self.password]):
            raise ValueError("Missing required connection parameters (host, user, password).")
    
    def connect(self) -> teradatasql.connect:
        """
        Establish database connection with Query Band injection.
        """
        try:
            # Construcción dinámica y segura de parámetros
            conn_params = {
                "host": self.host,
                "user": self.user,
                "password": self.password
            }
            
            # Solo pasamos la base de datos si fue configurada
            if self.database:
                conn_params["database"] = self.database
                
            conn = teradatasql.connect(**conn_params)
            
            # Inject Query Band for application tracing
            self._inject_query_band(conn)
            
            logger.info(f"Successfully connected to Teradata: {self.host}")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to connect to Teradata: {str(e)}")
            raise ConnectionError(f"Teradata connection failed: {str(e)}")
    
    def _inject_query_band(self, conn: teradatasql.connect) -> None:
        """
        Inject Query Band for application tracing and monitoring.
        """
        try:
            cursor = conn.cursor()
            query_band_sql = f"SET QUERY_BAND = '{self.query_band}' FOR SESSION;"
            cursor.execute(query_band_sql)
            cursor.close()
            logger.info(f"Query Band injected: {self.query_band}")
            
        except Exception as e:
            logger.warning(f"Failed to inject Query Band: {str(e)}")
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> list:
        """
        Execute SQL query with automatic connection management.
        """
        conn = None
        try:
            conn = self.connect()
            cursor = conn.cursor()
            
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            
            results = cursor.fetchall()
            cursor.close()
            
            logger.info(f"Query executed successfully. Rows returned: {len(results)}")
            return results
            
        except Exception as e:
            logger.error(f"Query execution failed: {str(e)}")
            raise
        finally:
            if conn:
                conn.close()
    
    def test_connection(self) -> bool:
        """
        Test database connectivity.
        """
        try:
            conn = self.connect()
            cursor = conn.cursor()
            cursor.execute("SELECT 1 as test_connection")
            result = cursor.fetchone()
            cursor.close()
            conn.close()
            
            if result and result[0] == 1:
                logger.info("Connection test successful")
                return True
            else:
                logger.error("Connection test failed: unexpected result")
                return False
                
        except Exception as e:
            logger.error(f"Connection test failed: {str(e)}")
            return False


def create_connection_from_params(params: dict):
    """Create a TeradataConnection from a params dict and return the live connection.

    Expected keys: host, user, password, database (optional).
    """
    td = TeradataConnection(
        host=params.get("host"),
        user=params.get("user"),
        password=params.get("password"),
        database=params.get("database"),
    )
    return td.connect()