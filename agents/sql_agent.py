from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

from .base_agent import BaseAgent
from tools.sql_tools import NLToSQLTool, QueryOptimizerTool, SecurityValidatorTool

class SQLAgent(BaseAgent):
    """Agent responsible for SQL query generation and optimization"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("sql_agent", config)
        
        # Register tools
        self.register_tool("nl_to_sql", NLToSQLTool(config))
        self.register_tool("optimizer", QueryOptimizerTool(config))
        self.register_tool("security_validator", SecurityValidatorTool(config))
        
        # Agent configuration
        self.max_retries = self.config.get('max_retries', 3)
        self.optimization_level = self.config.get('optimization_level', 'moderate')
        self.security_mode = self.config.get('security_mode', 'strict')
        self.complexity_threshold = self.config.get('complexity_threshold', 'medium')
    
    def execute(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute SQL-related tasks"""
        if not self.validate_input(task):
            return {
                'status': 'error',
                'message': 'Invalid task input',
                'agent': self.name
            }
        
        task_type = task.get('type')
        
        try:
            if task_type == 'generate_sql':
                return self._generate_sql(task)
            elif task_type == 'optimize_sql':
                return self._optimize_sql(task)
            elif task_type == 'validate_security':
                return self._validate_security(task)
            elif task_type == 'fix_sql':
                return self._fix_sql(task)
            elif task_type == 'complete_workflow':
                return self._complete_sql_workflow(task)
            else:
                return {
                    'status': 'error',
                    'message': f'Unknown task type: {task_type}',
                    'agent': self.name
                }
        
        except Exception as e:
            self.logger.error(f"SQL agent task failed: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'agent': self.name
            }
    
    def _generate_sql(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Generate SQL query from natural language"""
        question = task.get('question')
        context = task.get('context')
        current_date = task.get('current_date', datetime.now().strftime('%Y-%m-%d'))
        
        if not question or not context:
            return {
                'status': 'error',
                'message': 'Question and context are required for SQL generation',
                'agent': self.name
            }
        
        self.logger.info(f"Generating SQL for question: {question[:100]}...")
        
        # Determine complexity level based on question
        complexity_level = self._determine_complexity_level(question)
        
        # Generate SQL using NL to SQL tool
        sql_result = self.use_tool('nl_to_sql',
                                 question=question,
                                 context=context,
                                 current_date=current_date,
                                 complexity_level=complexity_level)
        
        if sql_result.get('status') != 'success':
            return {
                'status': 'error',
                'message': 'Failed to generate SQL query',
                'sql_result': sql_result,
                'agent': self.name
            }
        
        sql_query = sql_result.get('sql_query')
        
        # Validate security
        security_result = self.use_tool('security_validator',
                                      sql_query=sql_query,
                                      strict_mode=self.security_mode == 'strict')
        
        if not security_result.get('validation_result', {}).get('is_secure', False):
            return {
                'status': 'error',
                'message': 'Generated SQL failed security validation',
                'security_issues': security_result.get('validation_result', {}),
                'agent': self.name
            }
        
        # Store generation context
        self.update_context('last_generation', {
            'question': question,
            'sql_query': sql_query,
            'sql_result': sql_result,
            'security_result': security_result,
            'complexity_level': complexity_level
        })
        
        return {
            'status': 'success',
            'message': 'SQL query generated successfully',
            'sql_query': sql_query,
            'generation_details': sql_result,
            'security_validation': security_result,
            'complexity_level': complexity_level,
            'agent': self.name
        }
    
    def _optimize_sql(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize SQL query for better performance"""
        sql_query = task.get('sql_query')
        
        if not sql_query:
            # Try to get from context
            last_gen = self.get_context('last_generation')
            if last_gen:
                sql_query = last_gen.get('sql_query')
        
        if not sql_query:
            return {
                'status': 'error',
                'message': 'No SQL query provided for optimization',
                'agent': self.name
            }
        
        self.logger.info("Optimizing SQL query")
        
        # Optimize using query optimizer tool
        optimization_result = self.use_tool('optimizer',
                                          sql_query=sql_query,
                                          optimization_level=self.optimization_level)
        
        return {
            'status': 'success',
            'message': 'SQL optimization completed',
            'optimization_result': optimization_result,
            'agent': self.name
        }
    
    def _validate_security(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Validate SQL query security"""
        sql_query = task.get('sql_query')
        
        if not sql_query:
            return {
                'status': 'error',
                'message': 'No SQL query provided for security validation',
                'agent': self.name
            }
        
        self.logger.info("Validating SQL security")
        
        # Validate using security validator tool
        security_result = self.use_tool('security_validator',
                                      sql_query=sql_query,
                                      strict_mode=self.security_mode == 'strict')
        
        return {
            'status': 'success',
            'message': 'Security validation completed',
            'security_result': security_result,
            'agent': self.name
        }
    
    def _fix_sql(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Fix SQL query based on error feedback"""
        question = task.get('question')
        context = task.get('context')
        failed_sql = task.get('failed_sql')
        error_message = task.get('error_message')
        
        if not all([question, context, failed_sql, error_message]):
            return {
                'status': 'error',
                'message': 'Missing required parameters for SQL fixing',
                'agent': self.name
            }
        
        self.logger.info("Attempting to fix SQL query")
        
        # Create enhanced context with error information
        enhanced_context = f"""
{context}

PREVIOUS FAILED QUERY:
{failed_sql}

ERROR MESSAGE:
{error_message}

Please generate a corrected SQL query that addresses the error above.
"""
        
        # Generate corrected SQL
        fix_result = self.use_tool('nl_to_sql',
                                 question=question,
                                 context=enhanced_context,
                                 current_date=datetime.now().strftime('%Y-%m-%d'),
                                 complexity_level=self._determine_complexity_level(question))
        
        if fix_result.get('status') != 'success':
            return {
                'status': 'error',
                'message': 'Failed to generate corrected SQL query',
                'fix_result': fix_result,
                'agent': self.name
            }
        
        corrected_sql = fix_result.get('sql_query')
        
        # Validate the corrected SQL
        security_result = self.use_tool('security_validator',
                                      sql_query=corrected_sql,
                                      strict_mode=self.security_mode == 'strict')
        
        return {
            'status': 'success',
            'message': 'SQL query corrected successfully',
            'corrected_sql': corrected_sql,
            'fix_details': fix_result,
            'security_validation': security_result,
            'agent': self.name
        }
    
    def _complete_sql_workflow(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Complete end-to-end SQL workflow with retries"""
        question = task.get('question')
        context = task.get('context')
        max_retries = task.get('max_retries', self.max_retries)
        
        workflow_log = []
        current_sql = None
        
        # Step 1: Initial SQL generation
        gen_task = {
            'type': 'generate_sql',
            'question': question,
            'context': context
        }
        
        gen_result = self._generate_sql(gen_task)
        workflow_log.append(('generate_sql', gen_result))
        
        if gen_result.get('status') != 'success':
            return {
                'status': 'error',
                'message': 'Failed to generate initial SQL',
                'workflow_log': workflow_log,
                'agent': self.name
            }
        
        current_sql = gen_result.get('sql_query')
        
        # Step 2: Optimization (if requested)
        if task.get('optimize', True):
            opt_task = {'type': 'optimize_sql', 'sql_query': current_sql}
            opt_result = self._optimize_sql(opt_task)
            workflow_log.append(('optimize_sql', opt_result))
            
            # Use optimized query if available
            if (opt_result.get('status') == 'success' and 
                opt_result.get('optimization_result', {}).get('optimized_query')):
                optimized_query = opt_result['optimization_result']['optimized_query']
                if optimized_query != current_sql:
                    current_sql = optimized_query
        
        # Store final workflow result
        self.update_context('workflow_result', {
            'final_sql': current_sql,
            'workflow_log': workflow_log,
            'retries_available': max_retries
        })
        
        return {
            'status': 'success',
            'message': 'SQL workflow completed successfully',
            'final_sql': current_sql,
            'workflow_log': workflow_log,
            'agent': self.name
        }
    
    def _determine_complexity_level(self, question: str) -> str:
        """Determine complexity level based on question characteristics"""
        question_lower = question.lower()
        
        # Simple indicators
        simple_indicators = ['total', 'sum', 'count', 'average', 'max', 'min']
        
        # Complex indicators
        complex_indicators = [
            'compare', 'trend', 'growth', 'year over year', 'correlation',
            'top', 'bottom', 'rank', 'percentile', 'moving average',
            'pivot', 'cross-tab', 'breakdown by'
        ]
        
        # Advanced indicators
        advanced_indicators = [
            'cohort', 'funnel', 'attribution', 'statistical',
            'regression', 'forecasting', 'anomaly', 'clustering'
        ]
        
        if any(indicator in question_lower for indicator in advanced_indicators):
            return 'advanced'
        elif any(indicator in question_lower for indicator in complex_indicators):
            return 'medium'
        elif any(indicator in question_lower for indicator in simple_indicators):
            return 'simple'
        else:
            return 'medium'  # Default to medium
    
    def get_required_fields(self) -> List[str]:
        return ['type']
    
    def get_capabilities(self) -> List[str]:
        return [
            'natural_language_to_sql_conversion',
            'query_optimization',
            'security_validation',
            'sql_error_correction',
            'complexity_analysis',
            'performance_recommendations'
        ]
    
    def retry_with_fix(self, question: str, context: str, failed_sql: str, error_message: str) -> Dict[str, Any]:
        """Retry SQL generation with error correction"""
        fix_task = {
            'type': 'fix_sql',
            'question': question,
            'context': context,
            'failed_sql': failed_sql,
            'error_message': error_message
        }
        
        return self._fix_sql(fix_task)
    
    def get_generation_statistics(self) -> Dict[str, Any]:
        """Get SQL generation statistics"""
        last_gen = self.get_context('last_generation')
        workflow_result = self.get_context('workflow_result')
        
        stats = {
            'agent_name': self.name,
            'max_retries': self.max_retries,
            'optimization_level': self.optimization_level,
            'security_mode': self.security_mode,
            'last_generation_available': last_gen is not None,
            'workflow_completed': workflow_result is not None
        }
        
        if last_gen:
            stats['last_complexity_level'] = last_gen.get('complexity_level')
            stats['last_generation_secure'] = last_gen.get('security_result', {}).get('validation_result', {}).get('is_secure', False)
        
        return stats