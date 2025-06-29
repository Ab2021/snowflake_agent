import json
import os
from typing import Dict, List, Optional, Any
from datetime import datetime
import logging

from .models import Table, Column, Relationship, SemanticLayer, RelationshipType

class SchemaCatalog:
    """Manages the schema catalog with persistence and versioning"""
    
    def __init__(self, catalog_path: str = "schema_catalog.json"):
        self.catalog_path = catalog_path
        self.semantic_layer = SemanticLayer(name="GenBI_Catalog")
        self.logger = logging.getLogger("genbi.schema.catalog")
        self.version = "1.0.0"
        self.last_updated = None
        
        # Load existing catalog if it exists
        self.load()
    
    def add_table(self, table: Table):
        """Add or update a table in the catalog"""
        self.semantic_layer.add_table(table)
        self.last_updated = datetime.now()
        self.logger.info(f"Added/updated table: {table.name}")
    
    def get_table(self, table_name: str) -> Optional[Table]:
        """Get table by name"""
        return self.semantic_layer.get_table(table_name)
    
    def get_all_tables(self) -> Dict[str, Table]:
        """Get all tables in the catalog"""
        return self.semantic_layer.tables
    
    def add_relationship(self, relationship: Relationship):
        """Add a relationship between tables"""
        source_table = self.get_table(relationship.source_table)
        if source_table:
            source_table.relationships.append(relationship)
            self.last_updated = datetime.now()
            self.logger.info(f"Added relationship: {relationship.source_table} -> {relationship.target_table}")
    
    def add_business_metric(self, name: str, sql_expression: str, description: str = None):
        """Add a business metric definition"""
        self.semantic_layer.add_business_metric(name, sql_expression, description)
        self.last_updated = datetime.now()
        self.logger.info(f"Added business metric: {name}")
    
    def add_business_dimension(self, name: str, column_reference: str, description: str = None):
        """Add a business dimension definition"""
        self.semantic_layer.add_business_dimension(name, column_reference, description)
        self.last_updated = datetime.now()
        self.logger.info(f"Added business dimension: {name}")
    
    def add_common_join(self, name: str, join_sql: str):
        """Add a common join pattern"""
        self.semantic_layer.common_joins[name] = join_sql
        self.last_updated = datetime.now()
        self.logger.info(f"Added common join: {name}")
    
    def add_business_rule(self, rule: str):
        """Add a business rule"""
        self.semantic_layer.business_rules.append(rule)
        self.last_updated = datetime.now()
        self.logger.info(f"Added business rule: {rule[:50]}...")
    
    def get_context_for_llm(self, table_names: List[str] = None) -> str:
        """Get context string for LLM, optionally filtered by table names"""
        if table_names:
            # Create filtered semantic layer
            filtered_layer = SemanticLayer(name="Filtered_Context")
            filtered_layer.business_metrics = self.semantic_layer.business_metrics
            filtered_layer.business_dimensions = self.semantic_layer.business_dimensions
            filtered_layer.common_joins = self.semantic_layer.common_joins
            filtered_layer.business_rules = self.semantic_layer.business_rules
            filtered_layer.glossary = self.semantic_layer.glossary
            
            # Add only requested tables
            for table_name in table_names:
                table = self.get_table(table_name)
                if table:
                    filtered_layer.add_table(table)
            
            return filtered_layer.get_context_for_llm()
        else:
            return self.semantic_layer.get_context_for_llm()
    
    def find_related_tables(self, table_name: str, max_depth: int = 2) -> List[str]:
        """Find tables related to the given table within max_depth"""
        related_tables = set()
        current_tables = {table_name}
        
        for depth in range(max_depth):
            next_tables = set()
            
            for current_table in current_tables:
                table = self.get_table(current_table)
                if not table:
                    continue
                
                # Find tables this table relates to
                for rel in table.relationships:
                    if rel.source_table == table.name:
                        next_tables.add(rel.target_table)
                    elif rel.target_table == table.name:
                        next_tables.add(rel.source_table)
                
                # Also check for relationships where this table is the target
                for _, other_table in self.semantic_layer.tables.items():
                    for rel in other_table.relationships:
                        if rel.target_table == table.name:
                            next_tables.add(rel.source_table)
                        elif rel.source_table == table.name:
                            next_tables.add(rel.target_table)
            
            # Remove tables we've already seen
            next_tables -= related_tables
            next_tables -= current_tables
            
            related_tables.update(next_tables)
            current_tables = next_tables
            
            if not current_tables:
                break
        
        return list(related_tables)
    
    def get_table_suggestions(self, query_text: str) -> List[str]:
        """Get table suggestions based on query text"""
        suggestions = []
        query_lower = query_text.lower()
        
        # Check table names and business names
        for table_name, table in self.semantic_layer.tables.items():
            if (table.name.lower() in query_lower or 
                (table.business_name and table.business_name.lower() in query_lower)):
                suggestions.append(table.name)
                continue
            
            # Check column names
            for column in table.columns:
                if (column.name.lower() in query_lower or 
                    (column.business_name and column.business_name.lower() in query_lower)):
                    suggestions.append(table.name)
                    break
        
        # Check business metrics and dimensions
        for metric_name in self.semantic_layer.business_metrics:
            if metric_name.lower() in query_lower:
                # Find tables that might contain this metric
                # This is a simplified approach - could be more sophisticated
                for table_name, table in self.semantic_layer.tables.items():
                    if any(col.semantic_type == "measure" for col in table.columns):
                        suggestions.append(table.name)
        
        return list(set(suggestions))  # Remove duplicates
    
    def validate_catalog(self) -> Dict[str, List[str]]:
        """Validate the catalog and return any issues found"""
        issues = {
            'errors': [],
            'warnings': [],
            'suggestions': []
        }
        
        # Check for tables without relationships
        isolated_tables = []
        for table_name, table in self.semantic_layer.tables.items():
            if not table.relationships:
                # Check if other tables reference this one
                is_referenced = False
                for _, other_table in self.semantic_layer.tables.items():
                    if any(rel.target_table == table.name for rel in other_table.relationships):
                        is_referenced = True
                        break
                
                if not is_referenced:
                    isolated_tables.append(table.name)
        
        if isolated_tables:
            issues['warnings'].append(f"Tables without relationships: {', '.join(isolated_tables)}")
        
        # Check for missing business names
        missing_business_names = []
        for table_name, table in self.semantic_layer.tables.items():
            if not table.business_name:
                missing_business_names.append(table.name)
        
        if missing_business_names:
            issues['suggestions'].append(f"Consider adding business names for: {', '.join(missing_business_names)}")
        
        # Check for columns without descriptions
        columns_without_desc = []
        for table_name, table in self.semantic_layer.tables.items():
            for column in table.columns:
                if not column.description and not column.business_name:
                    columns_without_desc.append(f"{table.name}.{column.name}")
        
        if columns_without_desc and len(columns_without_desc) <= 10:  # Only show first 10
            issues['suggestions'].append(f"Consider adding descriptions for columns: {', '.join(columns_without_desc[:10])}")
        
        return issues
    
    def save(self):
        """Save the catalog to disk"""
        try:
            catalog_data = {
                'version': self.version,
                'last_updated': self.last_updated.isoformat() if self.last_updated else None,
                'semantic_layer': self.semantic_layer.to_dict()
            }
            
            with open(self.catalog_path, 'w') as f:
                json.dump(catalog_data, f, indent=2, default=str)
            
            self.logger.info(f"Saved catalog to {self.catalog_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to save catalog: {e}")
            raise
    
    def load(self):
        """Load the catalog from disk"""
        if not os.path.exists(self.catalog_path):
            self.logger.info("No existing catalog found, starting with empty catalog")
            return
        
        try:
            with open(self.catalog_path, 'r') as f:
                catalog_data = json.load(f)
            
            self.version = catalog_data.get('version', '1.0.0')
            if catalog_data.get('last_updated'):
                self.last_updated = datetime.fromisoformat(catalog_data['last_updated'])
            
            # Reconstruct semantic layer
            semantic_data = catalog_data.get('semantic_layer', {})
            self.semantic_layer = SemanticLayer(
                name=semantic_data.get('name', 'GenBI_Catalog'),
                description=semantic_data.get('description')
            )
            
            # Load tables
            for table_name, table_data in semantic_data.get('tables', {}).items():
                table = self._dict_to_table(table_data)
                self.semantic_layer.tables[table_name] = table
            
            # Load other semantic layer data
            self.semantic_layer.business_metrics = semantic_data.get('business_metrics', {})
            self.semantic_layer.business_dimensions = semantic_data.get('business_dimensions', {})
            self.semantic_layer.common_joins = semantic_data.get('common_joins', {})
            self.semantic_layer.business_rules = semantic_data.get('business_rules', [])
            self.semantic_layer.glossary = semantic_data.get('glossary', {})
            
            self.logger.info(f"Loaded catalog from {self.catalog_path}")
            
        except Exception as e:
            self.logger.error(f"Failed to load catalog: {e}")
            # Continue with empty catalog rather than failing
    
    def _dict_to_table(self, table_data: Dict[str, Any]) -> Table:
        """Convert dictionary to Table object"""
        columns = []
        for col_data in table_data.get('columns', []):
            column = Column(
                name=col_data['name'],
                data_type=col_data['data_type'],
                business_name=col_data.get('business_name'),
                description=col_data.get('description'),
                is_nullable=col_data.get('is_nullable', True),
                is_primary_key=col_data.get('is_primary_key', False),
                is_foreign_key=col_data.get('is_foreign_key', False),
                default_value=col_data.get('default_value'),
                max_length=col_data.get('max_length'),
                precision=col_data.get('precision'),
                scale=col_data.get('scale'),
                comment=col_data.get('comment'),
                semantic_type=col_data.get('semantic_type')
            )
            columns.append(column)
        
        relationships = []
        for rel_data in table_data.get('relationships', []):
            relationship = Relationship(
                source_table=rel_data['source_table'],
                target_table=rel_data['target_table'],
                source_column=rel_data['source_column'],
                target_column=rel_data['target_column'],
                relationship_type=RelationshipType(rel_data['relationship_type']),
                name=rel_data.get('name'),
                description=rel_data.get('description'),
                is_enforced=rel_data.get('is_enforced', False)
            )
            relationships.append(relationship)
        
        table = Table(
            name=table_data['name'],
            schema=table_data['schema'],
            database=table_data['database'],
            business_name=table_data.get('business_name'),
            description=table_data.get('description'),
            table_type=table_data.get('table_type', 'TABLE'),
            columns=columns,
            relationships=relationships,
            row_count=table_data.get('row_count'),
            size_bytes=table_data.get('size_bytes'),
            last_modified=datetime.fromisoformat(table_data['last_modified']) if table_data.get('last_modified') else None,
            comment=table_data.get('comment'),
            tags=table_data.get('tags', [])
        )
        
        return table
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get catalog statistics"""
        stats = {
            'table_count': len(self.semantic_layer.tables),
            'column_count': sum(len(table.columns) for table in self.semantic_layer.tables.values()),
            'relationship_count': sum(len(table.relationships) for table in self.semantic_layer.tables.values()),
            'business_metrics_count': len(self.semantic_layer.business_metrics),
            'business_dimensions_count': len(self.semantic_layer.business_dimensions),
            'common_joins_count': len(self.semantic_layer.common_joins),
            'business_rules_count': len(self.semantic_layer.business_rules),
            'glossary_terms_count': len(self.semantic_layer.glossary),
            'version': self.version,
            'last_updated': self.last_updated.isoformat() if self.last_updated else None
        }
        
        return stats