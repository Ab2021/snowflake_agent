from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

from .base_agent import BaseAgent
from .schema_agent import SchemaAgent
from .sql_agent import SQLAgent
from .analysis_agent import AnalysisAgent
from database import SnowflakeConnector
from postgres_connector import PostgreSQLConnector
from cost_optimization import CostOptimizedOrchestrator

class OrchestratorAgent(BaseAgent):
    """Master agent that orchestrates the complete BI workflow"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("orchestrator_agent", config)
        
        # Initialize sub-agents
        self.schema_agent = SchemaAgent(config.get('schema_agent', {}))
        self.sql_agent = SQLAgent(config.get('sql_agent', {}))
        self.analysis_agent = AnalysisAgent(config.get('analysis_agent', {}))
        
        # Database connectors - try PostgreSQL first, fallback to Snowflake
        try:
            self.db_connector = PostgreSQLConnector()
            if self.db_connector.test_connection():
                self.db_type = 'postgresql'
                self.logger.info("Using PostgreSQL database")
            else:
                raise Exception("PostgreSQL connection failed")
        except:
            self.db_connector = SnowflakeConnector()
            self.db_type = 'snowflake'
            self.logger.info("Using Snowflake database")
        
        # Orchestrator configuration
        self.max_retries = self.config.get('max_retries', 3)
        self.auto_optimize = self.config.get('auto_optimize', True)
        self.include_analysis = self.config.get('include_analysis', True)
        self.cache_schema = self.config.get('cache_schema', True)
        
        # Cost optimization
        self.cost_optimizer = CostOptimizedOrchestrator()
        self.enable_cost_optimization = self.config.get('enable_cost_optimization', True)
    
    def execute(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute orchestrated BI workflow tasks"""
        if not self.validate_input(task):
            return {
                'status': 'error',
                'message': 'Invalid task input',
                'agent': self.name
            }
        
        task_type = task.get('type')
        
        try:
            if task_type == 'complete_bi_workflow':
                return self._complete_bi_workflow(task)
            elif task_type == 'initialize_system':
                return self._initialize_system(task)
            elif task_type == 'query_with_context':
                return self._query_with_context(task)
            elif task_type == 'fix_and_retry':
                return self._fix_and_retry(task)
            elif task_type == 'get_system_status':
                return self._get_system_status(task)
            else:
                return {
                    'status': 'error',
                    'message': f'Unknown task type: {task_type}',
                    'agent': self.name
                }
        
        except Exception as e:
            self.logger.error(f"Orchestrator agent task failed: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'agent': self.name,
                'workflow_log': self.get_context('workflow_log', [])
            }
    
    def _complete_bi_workflow(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute complete BI workflow from question to insights"""
        question = task.get('question')
        user_context = task.get('user_context', '')
        database = task.get('database')
        schema = task.get('schema', 'PUBLIC')
        
        if not question:
            return {
                'status': 'error',
                'message': 'Question is required for BI workflow',
                'agent': self.name
            }
        
        self.logger.info(f"Starting complete BI workflow for: {question[:100]}...")
        
        workflow_log = []
        workflow_start = datetime.now()
        
        try:
            # Step 1: Ensure schema context is available
            context_result = self._ensure_schema_context(database, schema, question)
            workflow_log.append(('schema_context', context_result))
            
            if context_result.get('status') != 'success':
                return self._create_workflow_result('error', 'Failed to get schema context', workflow_log)
            
            schema_context = context_result.get('context', '')
            
            # Step 2: Generate SQL with retries
            sql_result = self._generate_sql_with_retries(question, schema_context, user_context)
            workflow_log.append(('sql_generation', sql_result))
            
            if sql_result.get('status') != 'success':
                return self._create_workflow_result('error', 'Failed to generate SQL', workflow_log)
            
            final_sql = sql_result.get('final_sql')
            
            # Step 3: Execute SQL query
            execution_result = self._execute_sql_safely(final_sql)
            workflow_log.append(('sql_execution', execution_result))
            
            if execution_result.get('status') != 'success':
                # Try to fix and retry
                fix_result = self._attempt_sql_fix(question, schema_context, final_sql, execution_result.get('error'))
                workflow_log.append(('sql_fix_attempt', fix_result))
                
                if fix_result.get('status') == 'success':
                    # Retry execution with fixed SQL
                    fixed_sql = fix_result.get('corrected_sql')
                    execution_result = self._execute_sql_safely(fixed_sql)
                    workflow_log.append(('sql_execution_retry', execution_result))
                    final_sql = fixed_sql
                
                if execution_result.get('status') != 'success':
                    return self._create_workflow_result('error', 'SQL execution failed after retry', workflow_log)
            
            query_results = execution_result.get('results', [])
            
            # Step 4: Perform analysis (if enabled and results available)
            analysis_result = None
            if self.include_analysis and query_results:
                analysis_result = self._perform_comprehensive_analysis(question, query_results, user_context)
                workflow_log.append(('analysis', analysis_result))
            
            # Step 5: Compile final response
            workflow_duration = (datetime.now() - workflow_start).total_seconds()
            
            response = {
                'status': 'success',
                'message': 'BI workflow completed successfully',
                'workflow_duration_seconds': workflow_duration,
                'question': question,
                'sql_query': final_sql,
                'results': query_results,
                'result_count': len(query_results) if query_results else 0,
                'analysis': analysis_result.get('comprehensive_results') if analysis_result else None,
                'insights': analysis_result.get('comprehensive_results', {}).get('insights', {}).get('insight_result', {}).get('insights') if analysis_result else None,
                'workflow_log': workflow_log,
                'agent': self.name
            }
            
            # Store successful workflow
            self.update_context('last_successful_workflow', response)
            
            return response
            
        except Exception as e:
            self.logger.error(f"BI workflow failed: {e}")
            return self._create_workflow_result('error', str(e), workflow_log)
    
    def _ensure_schema_context(self, database: str, schema: str, question: str) -> Dict[str, Any]:
        """Ensure schema context is available for the query"""
        # Check if we should use cached schema context
        if self.cache_schema and self.get_context('schema_context_cached'):
            cached_context = self.get_context('cached_schema_context')
            if cached_context:
                self.logger.info("Using cached schema context")
                return {
                    'status': 'success',
                    'context': cached_context,
                    'source': 'cache'
                }
        
        # Get context from schema agent
        schema_task = {
            'type': 'get_context',
            'query_text': question,
            'database': database,
            'schema': schema
        }
        
        context_result = self.schema_agent.execute(schema_task)
        
        if context_result.get('status') == 'success':
            context = context_result.get('context', '')
            
            # Cache the context if enabled
            if self.cache_schema:
                self.update_context('cached_schema_context', context)
                self.update_context('schema_context_cached', True)
            
            return {
                'status': 'success',
                'context': context,
                'source': 'schema_agent',
                'suggested_tables': context_result.get('suggested_tables', [])
            }
        else:
            return context_result
    
    def _generate_sql_with_retries(self, question: str, schema_context: str, user_context: str = '') -> Dict[str, Any]:
        """Generate SQL with optimization and retries"""
        # Combine contexts
        full_context = schema_context
        if user_context:
            full_context += f"\n\nADDITIONAL CONTEXT:\n{user_context}"
        
        # Generate SQL using SQL agent
        sql_task = {
            'type': 'complete_workflow',
            'question': question,
            'context': full_context,
            'optimize': self.auto_optimize,
            'max_retries': self.max_retries
        }
        
        return self.sql_agent.execute(sql_task)
    
    def _execute_sql_safely(self, sql_query: str) -> Dict[str, Any]:
        """Execute SQL query safely with error handling"""
        try:
            self.logger.info("Executing SQL query")
            results = self.db_connector.execute_query(sql_query)
            
            return {
                'status': 'success',
                'results': results,
                'result_count': len(results) if results else 0
            }
            
        except Exception as e:
            self.logger.error(f"SQL execution failed: {e}")
            return {
                'status': 'error',
                'error': str(e),
                'sql_query': sql_query
            }
    
    def _attempt_sql_fix(self, question: str, context: str, failed_sql: str, error_message: str) -> Dict[str, Any]:
        """Attempt to fix failed SQL query"""
        self.logger.info("Attempting to fix SQL query")
        
        fix_task = {
            'type': 'fix_sql',
            'question': question,
            'context': context,
            'failed_sql': failed_sql,
            'error_message': error_message
        }
        
        return self.sql_agent.execute(fix_task)
    
    def _perform_comprehensive_analysis(self, question: str, query_results: List[Dict], business_context: str = '') -> Dict[str, Any]:
        """Perform comprehensive analysis on query results"""
        self.logger.info("Performing comprehensive analysis")
        
        analysis_task = {
            'type': 'comprehensive_analysis',
            'data': query_results,
            'question': question,
            'business_context': business_context
        }
        
        return self.analysis_agent.execute(analysis_task)
    
    def _initialize_system(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Initialize the BI system by building schema catalog"""
        database = task.get('database')
        schema = task.get('schema', 'PUBLIC')
        
        self.logger.info("Initializing BI system")
        
        # Initialize schema agent
        schema_task = {
            'type': 'build_catalog',
            'database': database,
            'schema': schema
        }
        
        schema_result = self.schema_agent.execute(schema_task)
        
        if schema_result.get('status') == 'success':
            self.update_context('system_initialized', True)
            self.update_context('initialization_timestamp', datetime.now().isoformat())
            
            return {
                'status': 'success',
                'message': 'BI system initialized successfully',
                'schema_result': schema_result,
                'agent': self.name
            }
        else:
            return {
                'status': 'error',
                'message': 'Failed to initialize BI system',
                'schema_result': schema_result,
                'agent': self.name
            }
    
    def _query_with_context(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute query with provided context (bypass schema discovery)"""
        question = task.get('question')
        context = task.get('context')
        
        if not question or not context:
            return {
                'status': 'error',
                'message': 'Question and context are required',
                'agent': self.name
            }
        
        # Use SQL agent directly
        sql_task = {
            'type': 'complete_workflow',
            'question': question,
            'context': context,
            'optimize': self.auto_optimize
        }
        
        sql_result = self.sql_agent.execute(sql_task)
        
        if sql_result.get('status') != 'success':
            return sql_result
        
        # Execute the query
        final_sql = sql_result.get('final_sql')
        execution_result = self._execute_sql_safely(final_sql)
        
        if execution_result.get('status') != 'success':
            return execution_result
        
        # Perform analysis if enabled
        results = execution_result.get('results', [])
        analysis_result = None
        
        if self.include_analysis and results:
            analysis_result = self._perform_comprehensive_analysis(question, results)
        
        return {
            'status': 'success',
            'message': 'Query executed successfully',
            'sql_query': final_sql,
            'results': results,
            'analysis': analysis_result,
            'agent': self.name
        }
    
    def _fix_and_retry(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Fix and retry a failed query"""
        question = task.get('question')
        context = task.get('context')
        failed_sql = task.get('failed_sql')
        error_message = task.get('error_message')
        
        # Attempt fix
        fix_result = self._attempt_sql_fix(question, context, failed_sql, error_message)
        
        if fix_result.get('status') != 'success':
            return fix_result
        
        # Execute fixed query
        corrected_sql = fix_result.get('corrected_sql')
        execution_result = self._execute_sql_safely(corrected_sql)
        
        return {
            'status': execution_result.get('status'),
            'message': 'Fix and retry completed',
            'corrected_sql': corrected_sql,
            'results': execution_result.get('results'),
            'fix_details': fix_result,
            'agent': self.name
        }
    
    def _get_system_status(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Get comprehensive system status"""
        status = {
            'orchestrator': self.get_status(),
            'schema_agent': self.schema_agent.get_status(),
            'sql_agent': self.sql_agent.get_status(),
            'analysis_agent': self.analysis_agent.get_status(),
            'database_connection': self._check_database_connection(),
            'system_initialized': self.get_context('system_initialized', False),
            'last_workflow_success': self.get_context('last_successful_workflow') is not None
        }
        
        return {
            'status': 'success',
            'message': 'System status retrieved',
            'system_status': status,
            'agent': self.name
        }
    
    def _check_database_connection(self) -> Dict[str, Any]:
        """Check database connection status"""
        try:
            connection_ok = self.db_connector.test_connection()
            return {
                'status': 'connected' if connection_ok else 'disconnected',
                'timestamp': datetime.now().isoformat()
            }
        except Exception as e:
            return {
                'status': 'error',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
    
    def _create_workflow_result(self, status: str, message: str, workflow_log: List) -> Dict[str, Any]:
        """Create standardized workflow result"""
        return {
            'status': status,
            'message': message,
            'workflow_log': workflow_log,
            'agent': self.name,
            'timestamp': datetime.now().isoformat()
        }
    
    def get_required_fields(self) -> List[str]:
        return ['type']
    
    def get_capabilities(self) -> List[str]:
        return [
            'complete_bi_workflow_orchestration',
            'multi_agent_coordination',
            'error_handling_and_recovery',
            'query_execution_management',
            'comprehensive_analysis_coordination',
            'system_initialization',
            'workflow_optimization'
        ]
    
    def refresh_system(self, database: str = None, schema: str = 'PUBLIC') -> Dict[str, Any]:
        """Refresh entire system with latest schema"""
        # Clear caches
        self.context.clear()
        
        # Refresh schema agent
        schema_refresh = self.schema_agent.refresh_catalog(database, schema)
        
        if schema_refresh.get('status') == 'success':
            self.update_context('system_refreshed', datetime.now().isoformat())
            return {
                'status': 'success',
                'message': 'System refreshed successfully',
                'refresh_details': schema_refresh
            }
        else:
            return {
                'status': 'error',
                'message': 'Failed to refresh system',
                'refresh_details': schema_refresh
            }