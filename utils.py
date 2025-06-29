from typing import List, Dict, Any, Optional
import pandas as pd
import json
from datetime import datetime, date

def format_query_result(results: List[Dict[str, Any]], max_rows: int = 100) -> str:
    """
    Format query results for display
    
    Args:
        results: Query results as list of dictionaries
        max_rows: Maximum number of rows to display
        
    Returns:
        Formatted string representation of results
    """
    if not results:
        return "No data returned from query"
    
    # Limit results if too many rows
    display_results = results[:max_rows]
    truncated = len(results) > max_rows
    
    # Convert to DataFrame for better formatting
    try:
        df = pd.DataFrame(display_results)
        formatted = df.to_string(index=False, max_rows=max_rows)
        
        if truncated:
            formatted += f"\n\n... ({len(results) - max_rows} more rows truncated)"
        
        return formatted
    except Exception:
        # Fallback to JSON formatting
        formatted = json.dumps(display_results, indent=2, default=str)
        if truncated:
            formatted += f"\n\n... ({len(results) - max_rows} more rows truncated)"
        return formatted

def generate_chart_suggestions(results: List[Dict[str, Any]]) -> List[Dict[str, str]]:
    """
    Generate chart type suggestions based on query results
    
    Args:
        results: Query results as list of dictionaries
        
    Returns:
        List of chart suggestions with type and reasoning
    """
    suggestions = []
    
    if not results or len(results) == 0:
        return suggestions
    
    # Convert to DataFrame for analysis
    try:
        df = pd.DataFrame(results)
    except Exception:
        return suggestions
    
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime']).columns.tolist()
    
    # Single row - metrics
    if len(df) == 1:
        if numeric_cols:
            suggestions.append({
                'type': 'metrics',
                'reason': 'Single row with numeric values - perfect for key metrics display'
            })
    
    # Bar chart for categorical + numeric
    elif len(categorical_cols) >= 1 and len(numeric_cols) >= 1 and len(df) <= 50:
        suggestions.append({
            'type': 'bar',
            'reason': f'Categorical data ({categorical_cols[0]}) with numeric values ({numeric_cols[0]}) - ideal for comparison'
        })
    
    # Line chart for time series
    elif date_cols and numeric_cols:
        suggestions.append({
            'type': 'line',
            'reason': f'Date/time data with numeric values - perfect for trend analysis'
        })
    
    # Scatter plot for two numeric columns
    elif len(numeric_cols) >= 2:
        suggestions.append({
            'type': 'scatter',
            'reason': f'Multiple numeric columns - useful for correlation analysis'
        })
    
    # Pie chart for categorical data with counts
    elif len(categorical_cols) == 1 and len(numeric_cols) == 1 and len(df) <= 10:
        suggestions.append({
            'type': 'pie',
            'reason': 'Small number of categories with values - good for proportion analysis'
        })
    
    # Table for complex data
    if len(df.columns) > 4 or len(df) > 100:
        suggestions.append({
            'type': 'table',
            'reason': 'Complex data with many columns or rows - table format provides full detail'
        })
    
    return suggestions

def validate_semantic_context(context: str) -> Dict[str, Any]:
    """
    Validate and analyze semantic context
    
    Args:
        context: Semantic context string
        
    Returns:
        Dictionary with validation results and analysis
    """
    result = {
        'is_valid': False,
        'issues': [],
        'suggestions': [],
        'table_count': 0,
        'has_relationships': False,
        'has_column_descriptions': False
    }
    
    if not context or not context.strip():
        result['issues'].append("Semantic context is empty")
        result['suggestions'].append("Please provide database schema information including table names, column names, and relationships")
        return result
    
    context_lower = context.lower()
    
    # Check for table indicators
    table_indicators = ['table', 'schema', 'column', 'field']
    if not any(indicator in context_lower for indicator in table_indicators):
        result['issues'].append("No table or schema information detected")
        result['suggestions'].append("Include table names and column definitions")
    
    # Estimate table count (rough heuristic)
    table_keywords = context_lower.count('table') + context_lower.count('schema')
    result['table_count'] = max(1, table_keywords)  # At least 1 if any schema info
    
    # Check for relationships
    relationship_indicators = ['join', 'foreign key', 'references', 'relationship', 'related']
    if any(indicator in context_lower for indicator in relationship_indicators):
        result['has_relationships'] = True
    else:
        result['suggestions'].append("Consider adding table relationships and join conditions")
    
    # Check for column descriptions
    if any(word in context_lower for word in ['description', 'comment', 'meaning', 'represents']):
        result['has_column_descriptions'] = True
    else:
        result['suggestions'].append("Add column descriptions to improve query accuracy")
    
    # Basic validation passed if we have some schema info
    if table_keywords > 0:
        result['is_valid'] = True
    
    return result

def format_error_message(error: Exception) -> str:
    """
    Format error messages for user-friendly display
    
    Args:
        error: Exception object
        
    Returns:
        User-friendly error message
    """
    error_str = str(error).lower()
    
    # Common error patterns and user-friendly translations
    if 'does not exist' in error_str or 'not found' in error_str:
        return "❌ Database object not found. Please check your table and column names in the semantic context."
    
    elif 'invalid identifier' in error_str:
        return "❌ Invalid column or table name. Ensure all names are correctly spelled and exist in your database."
    
    elif 'syntax error' in error_str:
        return "❌ SQL syntax error. The generated query has invalid syntax."
    
    elif 'connection' in error_str or 'connect' in error_str:
        return "❌ Database connection failed. Please check your Snowflake connection settings."
    
    elif 'authentication' in error_str or 'login' in error_str:
        return "❌ Authentication failed. Please verify your Snowflake credentials."
    
    elif 'permission' in error_str or 'access' in error_str:
        return "❌ Permission denied. You may not have access to the requested data."
    
    else:
        return f"❌ Error: {str(error)}"

def serialize_for_json(obj: Any) -> Any:
    """
    Serialize objects for JSON compatibility
    
    Args:
        obj: Object to serialize
        
    Returns:
        JSON-serializable object
    """
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    elif isinstance(obj, pd.Timestamp):
        return obj.isoformat()
    elif hasattr(obj, '__dict__'):
        return str(obj)
    else:
        return obj

def clean_sql_query(sql_query: str) -> str:
    """
    Clean and format SQL query
    
    Args:
        sql_query: Raw SQL query string
        
    Returns:
        Cleaned SQL query
    """
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
    
    # Remove extra whitespace
    query = ' '.join(query.split())
    
    return query.strip()

def truncate_text(text: str, max_length: int = 1000) -> str:
    """
    Truncate text to maximum length
    
    Args:
        text: Text to truncate
        max_length: Maximum length
        
    Returns:
        Truncated text with ellipsis if needed
    """
    if not text or len(text) <= max_length:
        return text
    
    return text[:max_length - 3] + "..."

def extract_table_names_from_context(context: str) -> List[str]:
    """
    Extract table names from semantic context (basic implementation)
    
    Args:
        context: Semantic context string
        
    Returns:
        List of potential table names
    """
    table_names = []
    
    if not context:
        return table_names
    
    # Look for patterns like "table_name" or "TABLE: table_name"
    import re
    
    # Pattern 1: "table_name" (quoted identifiers)
    quoted_pattern = r'"([A-Za-z_][A-Za-z0-9_]*)"'
    quoted_matches = re.findall(quoted_pattern, context)
    table_names.extend(quoted_matches)
    
    # Pattern 2: TABLE: table_name or Table: table_name
    table_pattern = r'(?i)table[:\s]+([A-Za-z_][A-Za-z0-9_]*)'
    table_matches = re.findall(table_pattern, context)
    table_names.extend(table_matches)
    
    # Remove duplicates and return
    return list(set(table_names))
