import os
import psycopg2
import psycopg2.extras
from typing import Dict, List, Optional, Any
import logging
from datetime import datetime

class PostgreSQLConnector:
    """Connector for PostgreSQL database operations"""
    
    def __init__(self):
        """Initialize PostgreSQL connection parameters from environment variables"""
        self.logger = logging.getLogger("genbi.postgres_connector")
        
        # Get connection parameters from environment
        self.host = os.getenv('PGHOST', 'localhost')
        self.port = os.getenv('PGPORT', '5432')
        self.database = os.getenv('PGDATABASE', 'postgres')
        self.user = os.getenv('PGUSER', 'postgres')
        self.password = os.getenv('PGPASSWORD', '')
        
        self.connection = None
        self.logger.info(f"PostgreSQL connector initialized for {self.host}:{self.port}/{self.database}")
    
    def get_connection(self):
        """Create and return a PostgreSQL connection"""
        try:
            if self.connection is None or self.connection.closed:
                self.connection = psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.user,
                    password=self.password
                )
                self.logger.info("PostgreSQL connection established")
            
            return self.connection
            
        except Exception as e:
            self.logger.error(f"Failed to connect to PostgreSQL: {e}")
            raise
    
    def test_connection(self) -> bool:
        """Test the PostgreSQL connection"""
        try:
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                return result is not None
                
        except Exception as e:
            self.logger.error(f"Connection test failed: {e}")
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
        try:
            conn = self.get_connection()
            
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                self.logger.info(f"Executing query: {sql_query[:100]}...")
                cursor.execute(sql_query)
                
                # Only fetch results for SELECT queries
                if sql_query.strip().upper().startswith('SELECT'):
                    results = cursor.fetchall()
                    # Convert RealDictRow to regular dict
                    return [dict(row) for row in results]
                else:
                    # For non-SELECT queries, commit and return empty result
                    conn.commit()
                    return []
                    
        except Exception as e:
            self.logger.error(f"Query execution failed: {e}")
            if self.connection:
                self.connection.rollback()
            raise Exception(f"Database query failed: {str(e)}")
    
    def get_table_info(self, table_name: str, schema: str = 'public') -> Optional[Dict[str, Any]]:
        """
        Get information about a table's structure
        
        Args:
            table_name (str): Name of the table
            schema (str): Schema name (default: public)
            
        Returns:
            Dict with table information or None if failed
        """
        try:
            query = """
            SELECT 
                column_name,
                data_type,
                is_nullable,
                column_default,
                character_maximum_length,
                numeric_precision,
                numeric_scale
            FROM information_schema.columns 
            WHERE table_name = %s AND table_schema = %s
            ORDER BY ordinal_position
            """
            
            conn = self.get_connection()
            with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cursor:
                cursor.execute(query, (table_name, schema))
                columns = [dict(row) for row in cursor.fetchall()]
            
            # Get row count
            count_query = f'SELECT COUNT(*) as row_count FROM "{schema}"."{table_name}"'
            with conn.cursor() as cursor:
                cursor.execute(count_query)
                row_count = cursor.fetchone()[0]
            
            return {
                'table_name': table_name,
                'schema': schema,
                'columns': columns,
                'row_count': row_count
            }
            
        except Exception as e:
            self.logger.error(f"Failed to get table info for {table_name}: {e}")
            return None
    
    def list_tables(self, schema: str = 'public') -> List[str]:
        """
        List all tables in the specified schema
        
        Args:
            schema (str): Schema name (default: public)
            
        Returns:
            List of table names
        """
        try:
            query = """
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = %s AND table_type = 'BASE TABLE'
            ORDER BY table_name
            """
            
            conn = self.get_connection()
            with conn.cursor() as cursor:
                cursor.execute(query, (schema,))
                tables = [row[0] for row in cursor.fetchall()]
            
            return tables
            
        except Exception as e:
            self.logger.error(f"Failed to list tables: {e}")
            return []
    
    def create_sample_data(self):
        """Create sample data for testing purposes"""
        try:
            conn = self.get_connection()
            
            # Create sample tables with business data
            sample_queries = [
                """
                CREATE TABLE IF NOT EXISTS customers (
                    customer_id SERIAL PRIMARY KEY,
                    customer_name VARCHAR(100) NOT NULL,
                    email VARCHAR(100),
                    city VARCHAR(50),
                    state VARCHAR(50),
                    country VARCHAR(50),
                    registration_date DATE,
                    customer_segment VARCHAR(20)
                )
                """,
                
                """
                CREATE TABLE IF NOT EXISTS products (
                    product_id SERIAL PRIMARY KEY,
                    product_name VARCHAR(100) NOT NULL,
                    category VARCHAR(50),
                    price DECIMAL(10,2),
                    cost DECIMAL(10,2),
                    launch_date DATE
                )
                """,
                
                """
                CREATE TABLE IF NOT EXISTS orders (
                    order_id SERIAL PRIMARY KEY,
                    customer_id INTEGER REFERENCES customers(customer_id),
                    order_date DATE,
                    order_amount DECIMAL(10,2),
                    order_status VARCHAR(20),
                    shipping_city VARCHAR(50),
                    shipping_country VARCHAR(50)
                )
                """,
                
                """
                CREATE TABLE IF NOT EXISTS order_items (
                    order_item_id SERIAL PRIMARY KEY,
                    order_id INTEGER REFERENCES orders(order_id),
                    product_id INTEGER REFERENCES products(product_id),
                    quantity INTEGER,
                    unit_price DECIMAL(10,2),
                    total_amount DECIMAL(10,2)
                )
                """
            ]
            
            with conn.cursor() as cursor:
                # Create tables first
                for query in sample_queries:
                    cursor.execute(query)
                
                # Clear all data first to avoid foreign key issues
                cursor.execute("DELETE FROM order_items")
                cursor.execute("DELETE FROM orders") 
                cursor.execute("DELETE FROM products")
                cursor.execute("DELETE FROM customers")
                
                # Insert sample data in correct order
                self._insert_sample_customers(cursor)
                self._insert_sample_products(cursor)
                self._insert_sample_orders(cursor)
                self._insert_sample_order_items(cursor)
                
                conn.commit()
                self.logger.info("Sample data created successfully")
                
        except Exception as e:
            self.logger.error(f"Failed to create sample data: {e}")
            if self.connection:
                self.connection.rollback()
            raise
    
    def _insert_sample_customers(self, cursor):
        """Insert sample customer data"""
        customers_data = [
            ('John Smith', 'john.smith@email.com', 'New York', 'NY', 'USA', '2023-01-15', 'Premium'),
            ('Sarah Johnson', 'sarah.j@email.com', 'Los Angeles', 'CA', 'USA', '2023-02-20', 'Standard'),
            ('Mike Brown', 'mike.brown@email.com', 'Chicago', 'IL', 'USA', '2023-03-10', 'Premium'),
            ('Emily Davis', 'emily.d@email.com', 'Houston', 'TX', 'USA', '2023-04-05', 'Standard'),
            ('David Wilson', 'david.w@email.com', 'Phoenix', 'AZ', 'USA', '2023-05-12', 'Premium'),
            ('Lisa Anderson', 'lisa.a@email.com', 'Philadelphia', 'PA', 'USA', '2023-06-18', 'Standard'),
            ('Tom Martinez', 'tom.m@email.com', 'San Antonio', 'TX', 'USA', '2023-07-22', 'Premium'),
            ('Jessica Taylor', 'jessica.t@email.com', 'San Diego', 'CA', 'USA', '2023-08-14', 'Standard'),
            ('Chris Garcia', 'chris.g@email.com', 'Dallas', 'TX', 'USA', '2023-09-08', 'Premium'),
            ('Amanda White', 'amanda.w@email.com', 'San Jose', 'CA', 'USA', '2023-10-25', 'Standard')
        ]
        
        # Data already cleared in main function
        
        for customer in customers_data:
            cursor.execute("""
                INSERT INTO customers (customer_name, email, city, state, country, registration_date, customer_segment)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            """, customer)
    
    def _insert_sample_products(self, cursor):
        """Insert sample product data"""
        products_data = [
            ('Wireless Headphones', 'Electronics', 99.99, 45.00, '2023-01-01'),
            ('Smart Watch', 'Electronics', 299.99, 150.00, '2023-01-15'),
            ('Laptop Stand', 'Accessories', 49.99, 20.00, '2023-02-01'),
            ('USB-C Cable', 'Accessories', 19.99, 8.00, '2023-02-15'),
            ('Bluetooth Speaker', 'Electronics', 79.99, 35.00, '2023-03-01'),
            ('Phone Case', 'Accessories', 24.99, 10.00, '2023-03-15'),
            ('Tablet', 'Electronics', 399.99, 200.00, '2023-04-01'),
            ('Wireless Charger', 'Accessories', 34.99, 15.00, '2023-04-15'),
            ('Gaming Mouse', 'Electronics', 59.99, 25.00, '2023-05-01'),
            ('Keyboard', 'Electronics', 89.99, 40.00, '2023-05-15')
        ]
        
        # Data already cleared in main function
        
        for product in products_data:
            cursor.execute("""
                INSERT INTO products (product_name, category, price, cost, launch_date)
                VALUES (%s, %s, %s, %s, %s)
            """, product)
    
    def _insert_sample_orders(self, cursor):
        """Insert sample order data"""
        orders_data = [
            (1, '2023-11-01', 149.98, 'Delivered', 'New York', 'USA'),
            (2, '2023-11-02', 399.99, 'Delivered', 'Los Angeles', 'USA'),
            (3, '2023-11-03', 79.99, 'Shipped', 'Chicago', 'USA'),
            (1, '2023-11-04', 299.99, 'Delivered', 'New York', 'USA'),
            (4, '2023-11-05', 129.98, 'Processing', 'Houston', 'USA'),
            (5, '2023-11-06', 189.97, 'Delivered', 'Phoenix', 'USA'),
            (2, '2023-11-07', 99.99, 'Shipped', 'Los Angeles', 'USA'),
            (6, '2023-11-08', 84.98, 'Delivered', 'Philadelphia', 'USA'),
            (3, '2023-11-09', 349.98, 'Processing', 'Chicago', 'USA'),
            (7, '2023-11-10', 199.98, 'Delivered', 'San Antonio', 'USA'),
            (8, '2023-11-11', 119.98, 'Shipped', 'San Diego', 'USA'),
            (9, '2023-11-12', 229.97, 'Delivered', 'Dallas', 'USA'),
            (10, '2023-11-13', 174.97, 'Processing', 'San Jose', 'USA'),
            (1, '2023-11-14', 89.99, 'Delivered', 'New York', 'USA'),
            (4, '2023-11-15', 259.98, 'Shipped', 'Houston', 'USA')
        ]
        
        # Data already cleared in main function
        
        for order in orders_data:
            cursor.execute("""
                INSERT INTO orders (customer_id, order_date, order_amount, order_status, shipping_city, shipping_country)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, order)
    
    def _insert_sample_order_items(self, cursor):
        """Insert sample order item data"""
        # Get order and product IDs
        cursor.execute("SELECT order_id FROM orders ORDER BY order_id")
        order_ids = [row[0] for row in cursor.fetchall()]
        
        cursor.execute("SELECT product_id, price FROM products ORDER BY product_id")
        products = cursor.fetchall()
        
        order_items_data = [
            (1, 1, 1, 99.99, 99.99),    # Order 1: Wireless Headphones
            (1, 3, 1, 49.99, 49.99),    # Order 1: Laptop Stand
            (2, 7, 1, 399.99, 399.99),  # Order 2: Tablet
            (3, 5, 1, 79.99, 79.99),    # Order 3: Bluetooth Speaker
            (4, 2, 1, 299.99, 299.99),  # Order 4: Smart Watch
            (5, 1, 1, 99.99, 99.99),    # Order 5: Wireless Headphones
            (5, 6, 1, 24.99, 24.99),    # Order 5: Phone Case
            (6, 2, 1, 299.99, 299.99),  # Order 6: Smart Watch
            (6, 4, 2, 19.99, 39.98),    # Order 6: USB-C Cables (qty 2)
            (7, 1, 1, 99.99, 99.99),    # Order 7: Wireless Headphones
            (8, 6, 2, 24.99, 49.98),    # Order 8: Phone Cases (qty 2)
            (8, 8, 1, 34.99, 34.99),    # Order 8: Wireless Charger
            (9, 9, 1, 59.99, 59.99),    # Order 9: Gaming Mouse
            (9, 10, 1, 89.99, 89.99),   # Order 9: Keyboard
            (10, 2, 1, 299.99, 299.99), # Order 10: Smart Watch
            (11, 1, 1, 99.99, 99.99),   # Order 11: Wireless Headphones
            (12, 5, 1, 79.99, 79.99),   # Order 12: Bluetooth Speaker
            (13, 7, 1, 399.99, 399.99), # Order 13: Tablet
            (14, 10, 1, 89.99, 89.99),  # Order 14: Keyboard
            (15, 2, 1, 299.99, 299.99)  # Order 15: Smart Watch
        ]
        
        cursor.execute("DELETE FROM order_items")
        
        for item in order_items_data:
            cursor.execute("""
                INSERT INTO order_items (order_id, product_id, quantity, unit_price, total_amount)
                VALUES (%s, %s, %s, %s, %s)
            """, item)
    
    def close_connection(self):
        """Close the database connection"""
        if self.connection and not self.connection.closed:
            self.connection.close()
            self.logger.info("PostgreSQL connection closed")