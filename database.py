import os
import snowflake.connector
from snowflake.connector import DictCursor
from typing import List, Dict, Any, Optional
import json

class SnowflakeConnector:
    """Connector for Snowflake database operations"""
    
    def __init__(self):
        """Initialize Snowflake connection parameters from environment variables"""
        self.connection_params = {
            'user': os.getenv('SNOWFLAKE_USER', 'your-snowflake-user'),
            'password': os.getenv('SNOWFLAKE_PASSWORD', 'your-snowflake-password'),
            'account': os.getenv('SNOWFLAKE_ACCOUNT', 'your-snowflake-account'),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', 'COMPUTE_WH'),
            'database': os.getenv('SNOWFLAKE_DATABASE', 'your-database'),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', 'PUBLIC'),
            'role': os.getenv('SNOWFLAKE_ROLE', 'ACCOUNTADMIN')
        }
        
        # Check if we're using default values
        default_values = [
            'your-snowflake-user', 'your-snowflake-password', 
            'your-snowflake-account', 'your-database'
        ]
        
        if any(param in default_values for param in self.connection_params.values()):
            print("Warning: Using default Snowflake connection parameters. Please set environment variables:")
            print("SNOWFLAKE_USER, SNOWFLAKE_PASSWORD, SNOWFLAKE_ACCOUNT, SNOWFLAKE_DATABASE")
    
    def get_connection(self):
        """Create and return a Snowflake connection"""
        try:
            conn = snowflake.connector.connect(**self.connection_params)
            return conn
        except Exception as e:
            raise Exception(f"Failed to connect to Snowflake: {str(e)}")
    
    def test_connection(self) -> bool:
        """Test the Snowflake connection"""
        try:
            conn = self.get_connection()
            cursor = conn.cursor()
            cursor.execute("SELECT 1")
            result = cursor.fetchone()
            conn.close()
            return result is not None
        except Exception as e:
            print(f"Connection test failed: {e}")
            return False
    
    def execute_query(self, sql_query: str) -> Optional[List[Dict[str, Any]]]:
        """
        Execute a SELECT query and return results as list of dictionaries
        
        Args:
            sql_query (str): The SQL query to execute
            
        Returns:
            List[Dict[str, Any]]: Query results or None if failed
            
        Raises:
            Exception: If query execution fails
        """
        # Security check - only allow SELECT queries
        sql_clean = sql_query.strip().upper()
        if not sql_clean.startswith('SELECT') and not sql_clean.startswith('WITH'):
            raise Exception("Only SELECT queries are allowed for security reasons")
        
        # Check for dangerous keywords
        dangerous_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'TRUNCATE', 'MERGE', 'COPY', 'PUT', 'GET'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in sql_clean:
                raise Exception(f"Query contains prohibited keyword: {keyword}")
        
        conn = None
        try:
            conn = self.get_connection()
            cursor = conn.cursor(DictCursor)
            
            # Execute the query
            cursor.execute(sql_query)
            
            # Fetch all results
            results = cursor.fetchall()
            
            # Convert to list of dictionaries for JSON serialization
            if results:
                # Convert any complex types to strings for JSON serialization
                json_results = []
                for row in results:
                    json_row = {}
                    for key, value in row.items():
                        if value is None:
                            json_row[key] = None
                        elif isinstance(value, (str, int, float, bool)):
                            json_row[key] = value
                        else:
                            # Convert other types (dates, decimals, etc.) to string
                            json_row[key] = str(value)
                    json_results.append(json_row)
                return json_results
            else:
                return []
                
        except snowflake.connector.errors.ProgrammingError as e:
            # Re-raise with more specific error message
            error_msg = str(e)
            if "does not exist" in error_msg.lower():
                raise Exception(f"Database object not found: {error_msg}")
            elif "invalid identifier" in error_msg.lower():
                raise Exception(f"Invalid column or table name: {error_msg}")
            elif "syntax error" in error_msg.lower():
                raise Exception(f"SQL syntax error: {error_msg}")
            else:
                raise Exception(f"Query execution error: {error_msg}")
        
        except Exception as e:
            raise Exception(f"Database error: {str(e)}")
        
        finally:
            if conn:
                conn.close()
    
    def get_table_info(self, table_name: str) -> Optional[Dict[str, Any]]:
        """
        Get information about a table's structure
        
        Args:
            table_name (str): Name of the table
            
        Returns:
            Dict with table information or None if failed
        """
        try:
            # Get column information
            columns_query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_NAME = '{table_name.upper()}'
            ORDER BY ORDINAL_POSITION
            """
            
            columns = self.execute_query(columns_query)
            
            # Get row count
            count_query = f'SELECT COUNT(*) as row_count FROM "{table_name}"'
            count_result = self.execute_query(count_query)
            row_count = count_result[0]['ROW_COUNT'] if count_result else 0
            
            return {
                'table_name': table_name,
                'columns': columns,
                'row_count': row_count
            }
            
        except Exception as e:
            print(f"Error getting table info for {table_name}: {e}")
            return None
    
    def list_tables(self) -> List[str]:
        """
        List all tables in the current database/schema
        
        Returns:
            List of table names
        """
        try:
            query = """
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = CURRENT_SCHEMA()
            ORDER BY TABLE_NAME
            """
            
            results = self.execute_query(query)
            return [row['TABLE_NAME'] for row in results] if results else []
            
        except Exception as e:
            print(f"Error listing tables: {e}")
            return []
