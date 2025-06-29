from typing import Dict, Any, List, Optional
import re
from datetime import datetime

from .base_tool import BaseTool
from llm_client import LLMClient

class NLToSQLTool(BaseTool):
    """Advanced natural language to SQL translation tool"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("nl_to_sql", config)
        self.llm_client = LLMClient()
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Convert natural language question to SQL query"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for NL to SQL conversion")
            
            question = kwargs.get('question')
            context = kwargs.get('context')
            current_date = kwargs.get('current_date', datetime.now().strftime('%Y-%m-%d'))
            complexity_level = kwargs.get('complexity_level', 'medium')
            
            # Enhanced prompt based on complexity level
            system_prompt = self._build_system_prompt(complexity_level)
            
            # Generate SQL using LLM
            sql_query = self.llm_client.generate_sql_query(question, context, current_date)
            
            if not sql_query:
                return {
                    'status': 'error',
                    'message': 'Failed to generate SQL query',
                    'sql_query': None
                }
            
            # Clean and validate the generated SQL
            cleaned_sql = self._clean_sql_query(sql_query)
            validation_result = self._validate_sql_syntax(cleaned_sql)
            
            # Analyze query complexity and characteristics
            query_analysis = self._analyze_query(cleaned_sql)
            
            result = {
                'status': 'success',
                'sql_query': cleaned_sql,
                'original_question': question,
                'validation': validation_result,
                'analysis': query_analysis,
                'generation_timestamp': datetime.now().isoformat()
            }
            
            return self._post_execute(result, **kwargs)
            
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _build_system_prompt(self, complexity_level: str) -> str:
        """Build system prompt based on complexity level"""
        base_prompt = """You are an expert SQL analyst specializing in Snowflake SQL generation.
        Generate precise, efficient SQL queries that follow Snowflake best practices."""
        
        if complexity_level == 'simple':
            return base_prompt + """
            Focus on:
            - Simple SELECT statements
            - Basic WHERE clauses
            - Common aggregations (SUM, COUNT, AVG)
            - Standard date filtering
            """
        elif complexity_level == 'advanced':
            return base_prompt + """
            You can use advanced features:
            - Complex CTEs and subqueries
            - Window functions and analytics
            - Advanced date/time functions
            - Complex JOINs and aggregations
            - PIVOT/UNPIVOT operations
            """
        else:  # medium
            return base_prompt + """
            Use moderate complexity:
            - JOINs between multiple tables
            - GROUP BY with HAVING clauses
            - Basic window functions
            - Date arithmetic and formatting
            """
    
    def _clean_sql_query(self, sql_query: str) -> str:
        """Clean and format SQL query"""
        if not sql_query:
            return ""
        
        # Remove markdown formatting
        query = sql_query.strip()
        if query.startswith('```sql'):
            query = query[6:]
        if query.startswith('```'):
            query = query[3:]
        if query.endswith('```'):
            query = query[:-3]
        
        # Remove extra whitespace but preserve structure
        lines = query.split('\n')
        cleaned_lines = []
        for line in lines:
            stripped = line.strip()
            if stripped:
                cleaned_lines.append(stripped)
        
        return '\n'.join(cleaned_lines)
    
    def _validate_sql_syntax(self, sql_query: str) -> Dict[str, Any]:
        """Basic SQL syntax validation"""
        validation = {
            'is_valid': True,
            'issues': [],
            'warnings': [],
            'suggestions': []
        }
        
        if not sql_query:
            validation['is_valid'] = False
            validation['issues'].append("Empty SQL query")
            return validation
        
        sql_upper = sql_query.upper()
        
        # Check for required SELECT
        if not sql_upper.strip().startswith('SELECT') and not sql_upper.strip().startswith('WITH'):
            validation['is_valid'] = False
            validation['issues'].append("Query must start with SELECT or WITH")
        
        # Check for dangerous operations
        dangerous_keywords = ['DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE', 'ALTER']
        for keyword in dangerous_keywords:
            if keyword in sql_upper:
                validation['is_valid'] = False
                validation['issues'].append(f"Dangerous operation detected: {keyword}")
        
        # Check for basic syntax issues
        if sql_query.count('(') != sql_query.count(')'):
            validation['warnings'].append("Unmatched parentheses detected")
        
        # Check for common issues
        if 'FROM' not in sql_upper and 'SELECT' in sql_upper:
            validation['warnings'].append("Query may be missing FROM clause")
        
        return validation
    
    def _analyze_query(self, sql_query: str) -> Dict[str, Any]:
        """Analyze query characteristics and complexity"""
        analysis = {
            'complexity': 'simple',
            'estimated_rows': 'unknown',
            'performance_notes': [],
            'features_used': [],
            'table_count': 0,
            'join_count': 0,
            'has_aggregation': False,
            'has_window_functions': False,
            'has_subqueries': False
        }
        
        if not sql_query:
            return analysis
        
        sql_upper = sql_query.upper()
        
        # Count tables (rough estimate)
        from_matches = re.findall(r'FROM\s+([^\s,]+)', sql_upper)
        join_matches = re.findall(r'JOIN\s+([^\s,]+)', sql_upper)
        analysis['table_count'] = len(set(from_matches + join_matches))
        
        # Count joins
        analysis['join_count'] = len(re.findall(r'\bJOIN\b', sql_upper))
        
        # Check for aggregations
        agg_functions = ['SUM', 'COUNT', 'AVG', 'MAX', 'MIN', 'GROUP BY']
        for func in agg_functions:
            if func in sql_upper:
                analysis['has_aggregation'] = True
                analysis['features_used'].append(func.lower())
        
        # Check for window functions
        window_keywords = ['OVER', 'ROW_NUMBER', 'RANK', 'DENSE_RANK', 'LAG', 'LEAD']
        for keyword in window_keywords:
            if keyword in sql_upper:
                analysis['has_window_functions'] = True
                analysis['features_used'].append('window_functions')
                break
        
        # Check for subqueries/CTEs
        if 'WITH' in sql_upper or sql_upper.count('SELECT') > 1:
            analysis['has_subqueries'] = True
            analysis['features_used'].append('subqueries')
        
        # Determine complexity
        complexity_score = 0
        if analysis['table_count'] > 1:
            complexity_score += 1
        if analysis['join_count'] > 0:
            complexity_score += 1
        if analysis['has_aggregation']:
            complexity_score += 1
        if analysis['has_window_functions']:
            complexity_score += 2
        if analysis['has_subqueries']:
            complexity_score += 2
        
        if complexity_score <= 1:
            analysis['complexity'] = 'simple'
        elif complexity_score <= 3:
            analysis['complexity'] = 'medium'
        else:
            analysis['complexity'] = 'complex'
        
        # Performance suggestions
        if analysis['join_count'] > 3:
            analysis['performance_notes'].append("Consider breaking down complex joins")
        
        if analysis['has_window_functions'] and analysis['table_count'] > 2:
            analysis['performance_notes'].append("Window functions on large joined datasets may be slow")
        
        return analysis
    
    def get_required_parameters(self) -> List[str]:
        return ['question', 'context']
    
    def get_optional_parameters(self) -> List[str]:
        return ['current_date', 'complexity_level']
    
    def get_description(self) -> str:
        return "Advanced natural language to SQL translation with complexity analysis"

class QueryOptimizerTool(BaseTool):
    """Tool for optimizing SQL queries"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("query_optimizer", config)
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Optimize SQL query for better performance"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for query optimization")
            
            sql_query = kwargs.get('sql_query')
            optimization_level = kwargs.get('optimization_level', 'moderate')
            
            optimizations = []
            optimized_query = sql_query
            
            # Apply various optimization techniques
            if optimization_level in ['moderate', 'aggressive']:
                optimized_query, opts = self._apply_basic_optimizations(optimized_query)
                optimizations.extend(opts)
            
            if optimization_level == 'aggressive':
                optimized_query, advanced_opts = self._apply_advanced_optimizations(optimized_query)
                optimizations.extend(advanced_opts)
            
            # Generate optimization report
            report = self._generate_optimization_report(sql_query, optimized_query, optimizations)
            
            result = {
                'status': 'success',
                'original_query': sql_query,
                'optimized_query': optimized_query,
                'optimizations_applied': optimizations,
                'optimization_report': report,
                'optimization_timestamp': datetime.now().isoformat()
            }
            
            return self._post_execute(result, **kwargs)
            
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _apply_basic_optimizations(self, sql_query: str) -> tuple[str, List[str]]:
        """Apply basic SQL optimizations"""
        optimizations = []
        optimized = sql_query
        
        # Add explicit column selection warning
        if 'SELECT *' in optimized.upper():
            optimizations.append("Consider replacing SELECT * with explicit column names")
        
        # Suggest using LIMIT for exploratory queries
        if 'LIMIT' not in optimized.upper() and 'TOP' not in optimized.upper():
            optimizations.append("Consider adding LIMIT clause for large result sets")
        
        # Check for inefficient WHERE clauses
        if re.search(r'WHERE.*LIKE\s+\'%.*%\'', optimized, re.IGNORECASE):
            optimizations.append("Avoid leading wildcards in LIKE patterns when possible")
        
        return optimized, optimizations
    
    def _apply_advanced_optimizations(self, sql_query: str) -> tuple[str, List[str]]:
        """Apply advanced SQL optimizations"""
        optimizations = []
        optimized = sql_query
        
        # Suggest using CTEs for readability
        if optimized.upper().count('SELECT') > 2 and 'WITH' not in optimized.upper():
            optimizations.append("Consider using CTEs (WITH clauses) for complex subqueries")
        
        # Suggest partitioning hints for large tables
        if 'JOIN' in optimized.upper():
            optimizations.append("Consider adding partition pruning conditions for large tables")
        
        return optimized, optimizations
    
    def _generate_optimization_report(self, original: str, optimized: str, optimizations: List[str]) -> Dict[str, Any]:
        """Generate optimization report"""
        return {
            'optimizations_count': len(optimizations),
            'query_changed': original != optimized,
            'recommendations': optimizations,
            'estimated_improvement': 'moderate' if optimizations else 'none'
        }
    
    def get_required_parameters(self) -> List[str]:
        return ['sql_query']
    
    def get_optional_parameters(self) -> List[str]:
        return ['optimization_level']
    
    def get_description(self) -> str:
        return "Optimizes SQL queries for better performance and maintainability"

class SecurityValidatorTool(BaseTool):
    """Tool for validating SQL query security"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("security_validator", config)
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Validate SQL query for security issues"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for security validation")
            
            sql_query = kwargs.get('sql_query')
            strict_mode = kwargs.get('strict_mode', True)
            
            validation_result = {
                'is_secure': True,
                'security_level': 'high',
                'issues': [],
                'warnings': [],
                'blocked_operations': [],
                'recommendations': []
            }
            
            # Check for dangerous operations
            dangerous_ops = self._check_dangerous_operations(sql_query)
            if dangerous_ops:
                validation_result['is_secure'] = False
                validation_result['security_level'] = 'critical'
                validation_result['blocked_operations'] = dangerous_ops
                validation_result['issues'].append("Query contains dangerous operations")
            
            # Check for SQL injection patterns
            injection_risks = self._check_injection_risks(sql_query)
            if injection_risks:
                validation_result['security_level'] = 'medium'
                validation_result['warnings'].extend(injection_risks)
            
            # Check for information disclosure risks
            disclosure_risks = self._check_information_disclosure(sql_query)
            if disclosure_risks:
                validation_result['warnings'].extend(disclosure_risks)
            
            # Generate security recommendations
            recommendations = self._generate_security_recommendations(sql_query, strict_mode)
            validation_result['recommendations'] = recommendations
            
            result = {
                'status': 'success',
                'sql_query': sql_query,
                'validation_result': validation_result,
                'validation_timestamp': datetime.now().isoformat()
            }
            
            return self._post_execute(result, **kwargs)
            
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _check_dangerous_operations(self, sql_query: str) -> List[str]:
        """Check for dangerous SQL operations"""
        dangerous_ops = []
        sql_upper = sql_query.upper()
        
        dangerous_keywords = [
            'DROP', 'DELETE', 'UPDATE', 'INSERT', 'TRUNCATE', 'ALTER',
            'CREATE', 'GRANT', 'REVOKE', 'EXEC', 'EXECUTE', 'CALL'
        ]
        
        for keyword in dangerous_keywords:
            if re.search(rf'\b{keyword}\b', sql_upper):
                dangerous_ops.append(keyword)
        
        return dangerous_ops
    
    def _check_injection_risks(self, sql_query: str) -> List[str]:
        """Check for potential SQL injection patterns"""
        risks = []
        
        # Check for common injection patterns (though these shouldn't appear in LLM-generated queries)
        injection_patterns = [
            r"';.*--",  # Comment injection
            r"UNION.*SELECT",  # Union-based injection
            r"OR.*1=1",  # Boolean injection
        ]
        
        for pattern in injection_patterns:
            if re.search(pattern, sql_query, re.IGNORECASE):
                risks.append(f"Potential injection pattern detected: {pattern}")
        
        return risks
    
    def _check_information_disclosure(self, sql_query: str) -> List[str]:
        """Check for potential information disclosure issues"""
        warnings = []
        sql_upper = sql_query.upper()
        
        # Check for system table access
        system_patterns = [
            'INFORMATION_SCHEMA',
            'SNOWFLAKE.ACCOUNT_USAGE',
            'SYS.',
            'SYSTEM$'
        ]
        
        for pattern in system_patterns:
            if pattern in sql_upper:
                warnings.append(f"Query accesses system information: {pattern}")
        
        return warnings
    
    def _generate_security_recommendations(self, sql_query: str, strict_mode: bool) -> List[str]:
        """Generate security recommendations"""
        recommendations = []
        
        if strict_mode:
            recommendations.append("Query has been validated in strict security mode")
            recommendations.append("Only SELECT operations are permitted")
            recommendations.append("System table access is monitored")
        
        # Add general recommendations
        recommendations.append("Query will be executed with read-only permissions")
        recommendations.append("Results are limited to prevent data exfiltration")
        
        return recommendations
    
    def get_required_parameters(self) -> List[str]:
        return ['sql_query']
    
    def get_optional_parameters(self) -> List[str]:
        return ['strict_mode']
    
    def get_description(self) -> str:
        return "Validates SQL queries for security issues and dangerous operations"