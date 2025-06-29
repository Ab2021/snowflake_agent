from typing import List, Dict, Any, Optional
import json
from llm_client import LLMClient
from database import SnowflakeConnector

class BIWorkflow:
    """Manages the three-node BI workflow: generate_sql, analyze_data, fix_sql"""
    
    def __init__(self, current_date: str):
        """
        Initialize the BI workflow
        
        Args:
            current_date (str): Current date in YYYY-MM-DD format
        """
        self.llm_client = LLMClient()
        self.current_date = current_date
    
    def generate_sql(self, question: str, semantic_context: str) -> Optional[str]:
        """
        Generate SQL query from natural language question
        
        Args:
            question (str): User's natural language question
            semantic_context (str): Database schema and context information
            
        Returns:
            str: Generated SQL query or None if failed
        """
        if not semantic_context.strip():
            raise ValueError("Semantic context is required for SQL generation")
        
        if not question.strip():
            raise ValueError("Question is required for SQL generation")
        
        return self.llm_client.generate_sql_query(
            question=question,
            semantic_context=semantic_context,
            current_date=self.current_date
        )
    
    def analyze_data(self, question: str, query_result: List[Dict[str, Any]]) -> Optional[str]:
        """
        Analyze query results and provide business insights
        
        Args:
            question (str): Original user question
            query_result (List[Dict]): Results from SQL query execution
            
        Returns:
            str: Analysis and insights or None if failed
        """
        if not question.strip():
            raise ValueError("Original question is required for data analysis")
        
        if query_result is None:
            raise ValueError("Query result is required for data analysis")
        
        # Convert query result to JSON string for LLM processing
        try:
            query_result_json = json.dumps(query_result, indent=2, default=str)
        except Exception as e:
            # Fallback to string representation if JSON serialization fails
            query_result_json = str(query_result)
        
        return self.llm_client.analyze_query_results(
            question=question,
            query_result=query_result_json
        )
    
    def fix_sql(self, question: str, semantic_context: str, failed_sql_query: str, database_error: str) -> Optional[str]:
        """
        Fix a failed SQL query based on error message
        
        Args:
            question (str): Original user question
            semantic_context (str): Database schema and context information
            failed_sql_query (str): The SQL query that failed
            database_error (str): Error message from the database
            
        Returns:
            str: Fixed SQL query or None if failed
        """
        if not all([question.strip(), semantic_context.strip(), failed_sql_query.strip(), database_error.strip()]):
            raise ValueError("All parameters are required for SQL fixing")
        
        return self.llm_client.fix_sql_query(
            question=question,
            semantic_context=semantic_context,
            failed_sql_query=failed_sql_query,
            database_error=database_error
        )
    
    def execute_full_workflow(self, question: str, semantic_context: str, max_retries: int = 2) -> Dict[str, Any]:
        """
        Execute the complete BI workflow with error handling and retries
        
        Args:
            question (str): User's natural language question
            semantic_context (str): Database schema and context information
            max_retries (int): Maximum number of SQL fix attempts
            
        Returns:
            Dict containing workflow results and metadata
        """
        result = {
            'success': False,
            'question': question,
            'sql_query': None,
            'query_results': None,
            'analysis': None,
            'attempts': 0,
            'errors': []
        }
        
        try:
            # Step 1: Generate SQL
            sql_query = self.generate_sql(question, semantic_context)
            if not sql_query:
                result['errors'].append("Failed to generate SQL query")
                return result
            
            result['sql_query'] = sql_query
            
            # Step 2: Execute SQL with retries
            connector = SnowflakeConnector()
            current_sql = sql_query
            
            for attempt in range(max_retries + 1):
                result['attempts'] = attempt + 1
                
                try:
                    # Execute the query
                    query_results = connector.execute_query(current_sql)
                    result['query_results'] = query_results
                    
                    # Step 3: Analyze results
                    analysis = self.analyze_data(question, query_results)
                    result['analysis'] = analysis
                    result['success'] = True
                    
                    # Update final SQL query used
                    result['sql_query'] = current_sql
                    break
                    
                except Exception as e:
                    error_msg = str(e)
                    result['errors'].append(f"Attempt {attempt + 1}: {error_msg}")
                    
                    if attempt < max_retries:
                        # Try to fix the SQL
                        fixed_sql = self.fix_sql(
                            question=question,
                            semantic_context=semantic_context,
                            failed_sql_query=current_sql,
                            database_error=error_msg
                        )
                        
                        if fixed_sql and fixed_sql != current_sql:
                            current_sql = fixed_sql
                            result['sql_query'] = current_sql
                        else:
                            result['errors'].append(f"Unable to fix SQL query on attempt {attempt + 1}")
                            break
            
        except Exception as e:
            result['errors'].append(f"Workflow error: {str(e)}")
        
        return result
    
    def validate_sql_security(self, sql_query: str) -> tuple[bool, str]:
        """
        Validate SQL query for security (read-only operations)
        
        Args:
            sql_query (str): SQL query to validate
            
        Returns:
            tuple: (is_valid, error_message)
        """
        if not sql_query or not sql_query.strip():
            return False, "Empty SQL query"
        
        sql_clean = sql_query.strip().upper()
        
        # Must start with SELECT or WITH (for CTEs)
        if not sql_clean.startswith('SELECT') and not sql_clean.startswith('WITH'):
            return False, "Only SELECT queries are allowed"
        
        # Check for dangerous keywords
        dangerous_keywords = [
            'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE', 'ALTER',
            'TRUNCATE', 'MERGE', 'COPY', 'PUT', 'GET', 'GRANT', 'REVOKE'
        ]
        
        for keyword in dangerous_keywords:
            if keyword in sql_clean:
                return False, f"Query contains prohibited keyword: {keyword}"
        
        return True, "Query is valid"
    
    def get_query_metadata(self, sql_query: str) -> Dict[str, Any]:
        """
        Extract metadata from SQL query for better understanding
        
        Args:
            sql_query (str): SQL query to analyze
            
        Returns:
            Dict containing query metadata
        """
        metadata = {
            'query_type': 'SELECT',
            'estimated_complexity': 'simple',
            'has_aggregations': False,
            'has_joins': False,
            'has_subqueries': False,
            'has_window_functions': False,
            'tables_referenced': []
        }
        
        if not sql_query:
            return metadata
        
        sql_upper = sql_query.upper()
        
        # Check for complexity indicators
        if any(func in sql_upper for func in ['SUM(', 'COUNT(', 'AVG(', 'MAX(', 'MIN(']):
            metadata['has_aggregations'] = True
            metadata['estimated_complexity'] = 'medium'
        
        if any(join in sql_upper for join in ['JOIN', 'INNER JOIN', 'LEFT JOIN', 'RIGHT JOIN']):
            metadata['has_joins'] = True
            metadata['estimated_complexity'] = 'medium'
        
        if 'SELECT' in sql_upper.replace('SELECT', '', 1):  # Multiple SELECT statements
            metadata['has_subqueries'] = True
            metadata['estimated_complexity'] = 'complex'
        
        if any(func in sql_upper for func in ['ROW_NUMBER(', 'RANK(', 'DENSE_RANK(', 'OVER(']):
            metadata['has_window_functions'] = True
            metadata['estimated_complexity'] = 'complex'
        
        # Try to extract table names (basic regex would be better but keeping simple)
        # This is a simplified approach - in production, you'd want proper SQL parsing
        words = sql_query.replace('"', '').split()
        for i, word in enumerate(words):
            if word.upper() == 'FROM' and i + 1 < len(words):
                table_name = words[i + 1].split()[0].replace(',', '')
                if table_name not in metadata['tables_referenced']:
                    metadata['tables_referenced'].append(table_name)
        
        return metadata
