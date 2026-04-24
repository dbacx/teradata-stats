"""
Teradata Database Connection Module

This module provides secure connection functionality to Teradata databases
with proper Query Band injection for application tracing and credential management
through environment variables.
"""

import os
import logging
from typing import Optional, Dict, Any
from dotenv import load_dotenv
import teradatasql

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Load environment variables
load_dotenv()


class TeradataConnection:
    """
    Manages Teradata database connections with security and tracing capabilities.
    
    Attributes:
        host: Teradata server hostname
        user: Database username
        password: Database password
        database: Default database name
        query_band: Query band for application tracing
    """
    
    def __init__(self, host: Optional[str] = None, user: Optional[str] = None, 
                 password: Optional[str] = None, database: Optional[str] = None):
        """
        Initialize Teradata connection parameters.
        
        Args:
            host: Teradata server hostname (from env if None)
            user: Database username (from env if None)
            password: Database password (from env if None)
            database: Default database name (from env if None)
        """
        self.host = host or os.getenv('TERADATA_HOST')
        self.user = user or os.getenv('TERADATA_USER')
        self.password = password or os.getenv('TERADATA_PASSWORD')
        self.database = database or os.getenv('TERADATA_DATABASE')
        self.query_band = 'App=TDStatsOpt;'
        
        if not all([self.host, self.user, self.password]):
            raise ValueError("Missing required connection parameters. Check environment variables.")
    
    def get_connection_string(self) -> str:
        """
        Generate Teradata connection string.
        
        Returns:
            Formatted connection string for teradatasql
        """
        return f"dbcName={self.host};user={self.user};password={self.password};database={self.database}"
    
    def connect(self) -> teradatasql.connect:
        """
        Establish database connection with Query Band injection.
        
        Returns:
            Active Teradata connection object
            
        Raises:
            ConnectionError: If connection fails
        """
        try:
            conn = teradatasql.connect(self.get_connection_string())
            
            # Inject Query Band for application tracing
            self._inject_query_band(conn)
            
            logger.info(f"Successfully connected to Teradata: {self.host}/{self.database}")
            return conn
            
        except Exception as e:
            logger.error(f"Failed to connect to Teradata: {str(e)}")
            raise ConnectionError(f"Teradata connection failed: {str(e)}")
    
    def _inject_query_band(self, conn: teradatasql.connect) -> None:
        """
        Inject Query Band for application tracing and monitoring.
        
        Args:
            conn: Active database connection
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
        
        Args:
            query: SQL query string
            params: Optional query parameters
            
        Returns:
            Query results as list of tuples
            
        Raises:
            Exception: If query execution fails
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
        
        Returns:
            True if connection successful, False otherwise
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
    """
    Factory function to create Teradata connection from environment variables.
    
    Returns:
        Configured TeradataConnection instance
    """
    return TeradataConnection()


if __name__ == "__main__":
    # Example usage and connection test
    try:
        td_conn = create_connection()
        if td_conn.test_connection():
            print("Teradata connection successful!")
        else:
            print("Teradata connection failed!")
    except Exception as e:
        print(f"Error: {str(e)}")
