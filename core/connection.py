"""
Teradata Database Connection Module

This module provides secure connection functionality to Teradata databases
with proper Query Band injection for application tracing and credential management
through environment variables.
"""

import os
import glob
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import teradatasql

# Configure logging
from core.logging_config import configure_logging
configure_logging()
logger = logging.getLogger(__name__)

# Load environment variables — detect .env file dynamically
# Priority: config/*.env > root *.env > default .env
_project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_env_candidates = [
    *glob.glob(os.path.join(_project_root, 'config', '*.env')),
    *glob.glob(os.path.join(_project_root, '*.env')),
]
_env_candidates = [p for p in _env_candidates if not p.endswith('.env.example')]
_env_loaded = False
for _env_path in _env_candidates:
    if os.path.isfile(_env_path):
        load_dotenv(_env_path)
        logger.info(f"Loaded environment from: {os.path.basename(_env_path)}")
        _env_loaded = True
        break
if not _env_loaded:
    load_dotenv()  # fallback to default .env


class TeradataConnection:
    """
    Manages Teradata database connections with security and tracing capabilities.
    """
    
    def __init__(self, host: Optional[str] = None, user: Optional[str] = None, 
                 password: Optional[str] = None, database: Optional[str] = None):
        
        self.host = host or os.getenv('TERADATA_HOST')
        self.user = user or os.getenv('TERADATA_USER')
        self.password = password or os.getenv('TERADATA_PASSWORD')
        self.database = database or os.getenv('TERADATA_DATABASE')
        self.query_band = 'App=TDStatsOpt;'
        
        if not all([self.host, self.user, self.password]):
            raise ValueError("Missing required connection parameters. Check your .env file.")
    
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


def create_connection() -> TeradataConnection:
    """Factory function"""
    return TeradataConnection()