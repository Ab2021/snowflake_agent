from typing import Dict, Any, List, Optional
import logging

from .base_agent import BaseAgent
from tools.schema_tools import SchemaDiscoveryTool, RelationshipMapperTool, SemanticCatalogTool
from schema.catalog import SchemaCatalog

class SchemaAgent(BaseAgent):
    """Agent responsible for database schema discovery and management"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("schema_agent", config)
        
        # Register tools
        self.register_tool("discovery", SchemaDiscoveryTool(config))
        self.register_tool("relationship_mapper", RelationshipMapperTool(config))
        self.register_tool("semantic_catalog", SemanticCatalogTool(config))
        
        # Initialize catalog
        self.catalog = SchemaCatalog()
        
        # Agent configuration
        self.auto_discovery = self.config.get('auto_discovery', True)
        self.discovery_frequency = self.config.get('discovery_frequency', 'daily')
        self.include_system_tables = self.config.get('include_system_tables', False)
    
    def execute(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute schema-related tasks"""
        if not self.validate_input(task):
            return {
                'status': 'error',
                'message': 'Invalid task input',
                'agent': self.name
            }
        
        task_type = task.get('type')
        
        try:
            if task_type == 'discover_schema':
                return self._discover_schema(task)
            elif task_type == 'build_catalog':
                return self._build_catalog(task)
            elif task_type == 'get_context':
                return self._get_context_for_query(task)
            elif task_type == 'validate_catalog':
                return self._validate_catalog(task)
            elif task_type == 'add_business_context':
                return self._add_business_context(task)
            else:
                return {
                    'status': 'error',
                    'message': f'Unknown task type: {task_type}',
                    'agent': self.name
                }
        
        except Exception as e:
            self.logger.error(f"Schema agent task failed: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'agent': self.name
            }
    
    def _discover_schema(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Discover database schema"""
        self.logger.info("Starting schema discovery")
        
        # Use discovery tool
        discovery_result = self.use_tool('discovery', 
                                       database=task.get('database'),
                                       schema=task.get('schema', 'PUBLIC'),
                                       include_system_tables=self.include_system_tables)
        
        if discovery_result.get('status') == 'error':
            return discovery_result
        
        discovered_tables = discovery_result.get('discovered_tables', [])
        
        # Discover relationships
        relationship_result = self.use_tool('relationship_mapper',
                                          tables=discovered_tables,
                                          schema=task.get('schema', 'PUBLIC'))
        
        relationships = relationship_result.get('relationships', [])
        
        # Store discovery results in context
        self.update_context('last_discovery', {
            'tables': discovered_tables,
            'relationships': relationships,
            'discovery_result': discovery_result,
            'relationship_result': relationship_result
        })
        
        return {
            'status': 'success',
            'message': f'Discovered {len(discovered_tables)} tables and {len(relationships)} relationships',
            'tables_discovered': len(discovered_tables),
            'relationships_discovered': len(relationships),
            'discovery_details': discovery_result,
            'relationship_details': relationship_result,
            'agent': self.name
        }
    
    def _build_catalog(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Build semantic catalog from discovered schema"""
        self.logger.info("Building semantic catalog")
        
        # Get discovered data from context or perform discovery
        discovery_data = self.get_context('last_discovery')
        
        if not discovery_data:
            # Perform discovery first
            discovery_task = {
                'type': 'discover_schema',
                'database': task.get('database'),
                'schema': task.get('schema', 'PUBLIC')
            }
            discovery_result = self._discover_schema(discovery_task)
            
            if discovery_result.get('status') != 'success':
                return discovery_result
            
            discovery_data = self.get_context('last_discovery')
        
        # Build catalog using semantic catalog tool
        catalog_result = self.use_tool('semantic_catalog',
                                     action='build',
                                     tables=discovery_data['tables'],
                                     relationships=discovery_data['relationships'])
        
        # Update context with catalog
        self.update_context('catalog_built', True)
        self.update_context('catalog_stats', catalog_result.get('statistics', {}))
        
        return {
            'status': 'success',
            'message': 'Semantic catalog built successfully',
            'catalog_result': catalog_result,
            'agent': self.name
        }
    
    def _get_context_for_query(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Get relevant schema context for a query"""
        query_text = task.get('query_text', '')
        
        if not query_text:
            return {
                'status': 'error',
                'message': 'No query text provided',
                'agent': self.name
            }
        
        # Ensure catalog is built
        if not self.get_context('catalog_built'):
            build_result = self._build_catalog(task)
            if build_result.get('status') != 'success':
                return build_result
        
        # Get context using semantic catalog tool
        context_result = self.use_tool('semantic_catalog',
                                     action='get_context',
                                     query_text=query_text)
        
        return {
            'status': 'success',
            'message': 'Context retrieved successfully',
            'context': context_result.get('context', ''),
            'suggested_tables': context_result.get('suggested_tables', []),
            'related_tables': context_result.get('related_tables', []),
            'context_length': context_result.get('context_length', 0),
            'agent': self.name
        }
    
    def _validate_catalog(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Validate the current catalog"""
        self.logger.info("Validating catalog")
        
        # Use semantic catalog tool for validation
        validation_result = self.use_tool('semantic_catalog', action='validate')
        
        return {
            'status': 'success',
            'message': 'Catalog validation completed',
            'validation_result': validation_result,
            'agent': self.name
        }
    
    def _add_business_context(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Add business context to the catalog"""
        context_type = task.get('context_type')  # metric, dimension, rule, join
        name = task.get('name')
        definition = task.get('definition')
        description = task.get('description')
        
        if not all([context_type, name, definition]):
            return {
                'status': 'error',
                'message': 'Missing required parameters: context_type, name, definition',
                'agent': self.name
            }
        
        # Add business context using semantic catalog tool
        context_result = self.use_tool('semantic_catalog',
                                     action='add_business_context',
                                     context_type=context_type,
                                     name=name,
                                     definition=definition,
                                     description=description)
        
        return {
            'status': 'success',
            'message': f'Added {context_type}: {name}',
            'context_result': context_result,
            'agent': self.name
        }
    
    def get_required_fields(self) -> List[str]:
        return ['type']
    
    def get_capabilities(self) -> List[str]:
        return [
            'database_schema_discovery',
            'relationship_mapping',
            'semantic_catalog_management',
            'business_context_enrichment',
            'schema_validation',
            'query_context_generation'
        ]
    
    def get_schema_statistics(self) -> Dict[str, Any]:
        """Get current schema statistics"""
        stats = self.catalog.get_statistics()
        
        # Add agent-specific statistics
        stats['agent_name'] = self.name
        stats['auto_discovery_enabled'] = self.auto_discovery
        stats['discovery_frequency'] = self.discovery_frequency
        stats['last_discovery'] = self.get_context('last_discovery') is not None
        stats['catalog_built'] = self.get_context('catalog_built', False)
        
        return stats
    
    def refresh_catalog(self, database: str = None, schema: str = 'PUBLIC') -> Dict[str, Any]:
        """Refresh the catalog with latest schema information"""
        self.logger.info("Refreshing catalog")
        
        # Clear previous context
        self.context.clear()
        
        # Perform fresh discovery and catalog build
        discovery_task = {
            'type': 'discover_schema',
            'database': database,
            'schema': schema
        }
        
        discovery_result = self._discover_schema(discovery_task)
        if discovery_result.get('status') != 'success':
            return discovery_result
        
        build_task = {
            'type': 'build_catalog',
            'database': database,
            'schema': schema
        }
        
        return self._build_catalog(build_task)