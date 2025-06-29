from typing import Dict, Any, List, Optional
import re
from datetime import datetime

from .base_tool import BaseTool
from database import SnowflakeConnector
from postgres_connector import PostgreSQLConnector
from schema.models import Table, Column, Relationship, RelationshipType
from schema.catalog import SchemaCatalog

class SchemaDiscoveryTool(BaseTool):
    """Tool for discovering database schema information"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("schema_discovery", config)
        self.connector = None
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Discover schema information from the database"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for schema discovery")
            
            # Try PostgreSQL first, fallback to Snowflake
            try:
                self.connector = PostgreSQLConnector()
                if not self.connector.test_connection():
                    raise Exception("PostgreSQL connection failed")
                self.db_type = 'postgresql'
                self.logger.info("Using PostgreSQL for schema discovery")
            except:
                self.connector = SnowflakeConnector()
                self.db_type = 'snowflake'
                self.logger.info("Using Snowflake for schema discovery")
            database = kwargs.get('database')
            schema = kwargs.get('schema', 'PUBLIC')
            include_system_tables = kwargs.get('include_system_tables', False)
            
            discovered_tables = []
            
            # Get all tables in the schema - adapt query for database type
            if hasattr(self, 'db_type') and self.db_type == 'postgresql':
                tables_query = f"""
                SELECT table_name as TABLE_NAME, table_type as TABLE_TYPE, 
                       '' as COMMENT, 0 as ROW_COUNT, 0 as BYTES
                FROM information_schema.tables
                WHERE table_schema = '{schema.lower()}'
                """
            else:
                tables_query = f"""
                SELECT TABLE_NAME, TABLE_TYPE, COMMENT, ROW_COUNT, BYTES
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = '{schema.upper()}'
                """
            
            if not include_system_tables:
                tables_query += " AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')"
            
            tables_result = self.connector.execute_query(tables_query)
            
            if not tables_result:
                return {'discovered_tables': [], 'message': 'No tables found'}
            
            for table_info in tables_result:
                table_name = table_info.get('TABLE_NAME') or table_info.get('table_name')
                
                # Get column information - adapt query for database type
                if hasattr(self, 'db_type') and self.db_type == 'postgresql':
                    columns_query = f"""
                    SELECT 
                        column_name as COLUMN_NAME,
                        data_type as DATA_TYPE,
                        is_nullable as IS_NULLABLE,
                        column_default as COLUMN_DEFAULT,
                        '' as COMMENT,
                        character_maximum_length as CHARACTER_MAXIMUM_LENGTH,
                        numeric_precision as NUMERIC_PRECISION,
                        numeric_scale as NUMERIC_SCALE
                    FROM information_schema.columns
                    WHERE table_schema = '{schema.lower()}'
                    AND table_name = '{table_name.lower()}'
                    ORDER BY ordinal_position
                    """
                else:
                    columns_query = f"""
                    SELECT 
                        COLUMN_NAME,
                        DATA_TYPE,
                        IS_NULLABLE,
                        COLUMN_DEFAULT,
                        COMMENT,
                        CHARACTER_MAXIMUM_LENGTH,
                        NUMERIC_PRECISION,
                        NUMERIC_SCALE
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{schema.upper()}'
                    AND TABLE_NAME = '{table_name}'
                    ORDER BY ORDINAL_POSITION
                    """
                
                columns_result = self.connector.execute_query(columns_query)
                
                # Create column objects
                columns = []
                for col_info in columns_result or []:
                    column_name = col_info.get('COLUMN_NAME') or col_info.get('column_name')
                    column = Column(
                        name=column_name,
                        data_type=col_info.get('DATA_TYPE') or col_info.get('data_type'),
                        is_nullable=(col_info.get('IS_NULLABLE') or col_info.get('is_nullable')) == 'YES',
                        default_value=col_info.get('COLUMN_DEFAULT') or col_info.get('column_default'),
                        comment=col_info.get('COMMENT') or col_info.get('comment') or '',
                        max_length=col_info.get('CHARACTER_MAXIMUM_LENGTH') or col_info.get('character_maximum_length'),
                        precision=col_info.get('NUMERIC_PRECISION') or col_info.get('numeric_precision'),
                        scale=col_info.get('NUMERIC_SCALE') or col_info.get('numeric_scale')
                    )
                    
                    # Infer semantic types
                    column.semantic_type = self._infer_semantic_type(column.name, column.data_type)
                    
                    columns.append(column)
                
                # Create table object
                table = Table(
                    name=table_name,
                    schema=schema,
                    database=database or 'UNKNOWN',
                    table_type=table_info.get('TABLE_TYPE', 'TABLE'),
                    columns=columns,
                    row_count=table_info.get('ROW_COUNT'),
                    size_bytes=table_info.get('BYTES'),
                    comment=table_info.get('COMMENT')
                )
                
                discovered_tables.append(table)
            
            result = {
                'discovered_tables': discovered_tables,
                'table_count': len(discovered_tables),
                'total_columns': sum(len(t.columns) for t in discovered_tables),
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            return self._post_execute(result, **kwargs)
            
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _infer_semantic_type(self, column_name: str, data_type: str) -> Optional[str]:
        """Infer semantic type from column name and data type"""
        name_lower = column_name.lower()
        
        # Common patterns
        if any(pattern in name_lower for pattern in ['email', 'mail']):
            return 'email'
        elif any(pattern in name_lower for pattern in ['phone', 'tel', 'mobile']):
            return 'phone'
        elif any(pattern in name_lower for pattern in ['url', 'link', 'website']):
            return 'url'
        elif any(pattern in name_lower for pattern in ['date', 'time', 'created', 'updated']):
            return 'datetime'
        elif any(pattern in name_lower for pattern in ['id', '_id', 'key']):
            return 'identifier'
        elif any(pattern in name_lower for pattern in ['amount', 'price', 'cost', 'value', 'total']):
            return 'currency'
        elif any(pattern in name_lower for pattern in ['count', 'qty', 'quantity', 'num']):
            return 'quantity'
        elif 'address' in name_lower:
            return 'address'
        elif 'name' in name_lower:
            return 'name'
        elif 'description' in name_lower:
            return 'description'
        
        return None
    
    def get_required_parameters(self) -> List[str]:
        return []
    
    def get_optional_parameters(self) -> List[str]:
        return ['database', 'schema', 'include_system_tables']
    
    def get_description(self) -> str:
        return "Discovers database schema information including tables, columns, and metadata"

class RelationshipMapperTool(BaseTool):
    """Tool for discovering relationships between tables"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("relationship_mapper", config)
        self.connector = None
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Discover relationships between tables"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for relationship mapping")
            
            # Try PostgreSQL first, fallback to Snowflake
            try:
                self.connector = PostgreSQLConnector()
                if not self.connector.test_connection():
                    raise Exception("PostgreSQL connection failed")
                self.db_type = 'postgresql'
            except:
                self.connector = SnowflakeConnector()
                self.db_type = 'snowflake'
            tables = kwargs.get('tables', [])
            schema = kwargs.get('schema', 'PUBLIC')
            
            relationships = []
            
            # Method 1: Find explicit foreign key constraints
            fk_query = f"""
            SELECT 
                tc.TABLE_NAME as source_table,
                kcu.COLUMN_NAME as source_column,
                ccu.TABLE_NAME as target_table,
                ccu.COLUMN_NAME as target_column,
                tc.CONSTRAINT_NAME
            FROM INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu 
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
            JOIN INFORMATION_SCHEMA.CONSTRAINT_COLUMN_USAGE ccu 
                ON ccu.CONSTRAINT_NAME = tc.CONSTRAINT_NAME
            WHERE tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            AND tc.TABLE_SCHEMA = '{schema.upper()}'
            """
            
            try:
                fk_result = self.connector.execute_query(fk_query)
                for fk_info in fk_result or []:
                    relationship = Relationship(
                        source_table=fk_info['SOURCE_TABLE'],
                        target_table=fk_info['TARGET_TABLE'],
                        source_column=fk_info['SOURCE_COLUMN'],
                        target_column=fk_info['TARGET_COLUMN'],
                        relationship_type=RelationshipType.MANY_TO_ONE,
                        name=fk_info['CONSTRAINT_NAME'],
                        is_enforced=True
                    )
                    relationships.append(relationship)
            except:
                # Foreign key discovery might not work in all Snowflake editions
                self.logger.warning("Could not discover explicit foreign key constraints")
            
            # Method 2: Infer relationships from naming patterns
            inferred_relationships = self._infer_relationships_from_naming(tables)
            relationships.extend(inferred_relationships)
            
            result = {
                'relationships': relationships,
                'relationship_count': len(relationships),
                'explicit_fk_count': sum(1 for r in relationships if r.is_enforced),
                'inferred_count': sum(1 for r in relationships if not r.is_enforced),
                'discovery_timestamp': datetime.now().isoformat()
            }
            
            return self._post_execute(result, **kwargs)
            
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _infer_relationships_from_naming(self, tables: List[Table]) -> List[Relationship]:
        """Infer relationships based on column naming patterns"""
        relationships = []
        
        # Create mappings for faster lookup
        table_by_name = {table.name: table for table in tables}
        
        for table in tables:
            for column in table.columns:
                # Look for foreign key patterns like "customer_id", "user_id", etc.
                if column.name.lower().endswith('_id') and not column.is_primary_key:
                    # Extract potential target table name
                    potential_table = column.name[:-3]  # Remove "_id"
                    
                    # Try different variations
                    variations = [
                        potential_table.upper(),
                        potential_table.upper() + 'S',  # plural
                        potential_table.upper()[:-1] if potential_table.endswith('s') else None,  # singular
                        potential_table.upper() + '_DIM',  # dimension table
                        potential_table.upper() + '_FACT'  # fact table
                    ]
                    
                    for variation in variations:
                        if variation and variation in table_by_name:
                            target_table = table_by_name[variation]
                            
                            # Look for matching primary key column
                            target_pk = None
                            for target_col in target_table.columns:
                                if (target_col.is_primary_key or 
                                    target_col.name.lower() in ['id', variation.lower() + '_id']):
                                    target_pk = target_col
                                    break
                            
                            if target_pk:
                                relationship = Relationship(
                                    source_table=table.name,
                                    target_table=target_table.name,
                                    source_column=column.name,
                                    target_column=target_pk.name,
                                    relationship_type=RelationshipType.MANY_TO_ONE,
                                    description=f"Inferred from naming pattern: {column.name}",
                                    is_enforced=False
                                )
                                relationships.append(relationship)
                            break
        
        return relationships
    
    def get_required_parameters(self) -> List[str]:
        return ['tables']
    
    def get_optional_parameters(self) -> List[str]:
        return ['schema']
    
    def get_description(self) -> str:
        return "Discovers relationships between tables using foreign keys and naming patterns"

class SemanticCatalogTool(BaseTool):
    """Tool for building and managing semantic catalog"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("semantic_catalog", config)
        self.catalog = SchemaCatalog()
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Build or update semantic catalog"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for semantic cataloging")
            
            action = kwargs.get('action', 'build')
            tables = kwargs.get('tables', [])
            relationships = kwargs.get('relationships', [])
            
            if action == 'build':
                return self._build_catalog(tables, relationships)
            elif action == 'add_business_context':
                return self._add_business_context(**kwargs)
            elif action == 'validate':
                return self._validate_catalog()
            elif action == 'get_context':
                return self._get_context_for_query(**kwargs)
            else:
                raise ValueError(f"Unknown action: {action}")
                
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _build_catalog(self, tables: List[Table], relationships: List[Relationship]) -> Dict[str, Any]:
        """Build the semantic catalog from discovered tables and relationships"""
        # Add tables to catalog
        for table in tables:
            self.catalog.add_table(table)
        
        # Add relationships
        for relationship in relationships:
            self.catalog.add_relationship(relationship)
        
        # Add default business context
        self._add_default_business_context(tables)
        
        # Save catalog
        self.catalog.save()
        
        stats = self.catalog.get_statistics()
        validation_results = self.catalog.validate_catalog()
        
        return {
            'status': 'success',
            'message': 'Semantic catalog built successfully',
            'statistics': stats,
            'validation': validation_results
        }
    
    def _add_default_business_context(self, tables: List[Table]):
        """Add default business context based on table and column patterns"""
        
        # Add common business metrics
        for table in tables:
            table_name_lower = table.name.lower()
            
            # Sales metrics
            if 'sales' in table_name_lower or 'order' in table_name_lower:
                for column in table.columns:
                    if 'amount' in column.name.lower() or 'total' in column.name.lower():
                        self.catalog.add_business_metric(
                            f"Total_{table.name}_Amount",
                            f"SUM(\"{table.name}\".\"{column.name}\")",
                            f"Total amount from {table.business_name or table.name}"
                        )
            
            # Customer metrics
            if 'customer' in table_name_lower or 'user' in table_name_lower:
                self.catalog.add_business_metric(
                    f"Total_{table.name}_Count",
                    f"COUNT(DISTINCT \"{table.name}\".\"{'ID' if any(c.name == 'ID' for c in table.columns) else table.columns[0].name}\")",
                    f"Total number of unique {table.business_name or table.name.lower()}"
                )
        
        # Add common business rules
        business_rules = [
            "All monetary amounts should be in USD unless specified otherwise",
            "Date filters should respect business calendar (exclude weekends for business metrics)",
            "Customer data should be filtered to exclude test accounts when analyzing production metrics",
            "Historical comparisons should use same-period-last-year unless specified otherwise"
        ]
        
        for rule in business_rules:
            self.catalog.add_business_rule(rule)
    
    def _add_business_context(self, **kwargs) -> Dict[str, Any]:
        """Add business context like metrics, dimensions, rules"""
        context_type = kwargs.get('context_type')
        name = kwargs.get('name')
        definition = kwargs.get('definition')
        description = kwargs.get('description')
        
        if context_type == 'metric':
            self.catalog.add_business_metric(name, definition, description)
        elif context_type == 'dimension':
            self.catalog.add_business_dimension(name, definition, description)
        elif context_type == 'rule':
            self.catalog.add_business_rule(definition)
        elif context_type == 'join':
            self.catalog.add_common_join(name, definition)
        
        self.catalog.save()
        
        return {
            'status': 'success',
            'message': f'Added {context_type}: {name}',
            'statistics': self.catalog.get_statistics()
        }
    
    def _validate_catalog(self) -> Dict[str, Any]:
        """Validate the catalog and return issues"""
        validation_results = self.catalog.validate_catalog()
        stats = self.catalog.get_statistics()
        
        return {
            'status': 'success',
            'validation_results': validation_results,
            'statistics': stats,
            'recommendations': self._generate_recommendations(validation_results, stats)
        }
    
    def _get_context_for_query(self, **kwargs) -> Dict[str, Any]:
        """Get relevant context for a specific query"""
        query_text = kwargs.get('query_text', '')
        
        # Get table suggestions based on query
        suggested_tables = self.catalog.get_table_suggestions(query_text)
        
        # Get context for relevant tables
        if suggested_tables:
            context = self.catalog.get_context_for_llm(suggested_tables)
            related_tables = []
            for table_name in suggested_tables:
                related = self.catalog.find_related_tables(table_name, max_depth=1)
                related_tables.extend(related)
            
            # Include related tables in context
            all_relevant_tables = list(set(suggested_tables + related_tables))
            full_context = self.catalog.get_context_for_llm(all_relevant_tables)
        else:
            # Use full context if no specific tables identified
            full_context = self.catalog.get_context_for_llm()
            all_relevant_tables = list(self.catalog.get_all_tables().keys())
        
        return {
            'status': 'success',
            'context': full_context,
            'suggested_tables': suggested_tables,
            'related_tables': all_relevant_tables,
            'context_length': len(full_context)
        }
    
    def _generate_recommendations(self, validation_results: Dict, stats: Dict) -> List[str]:
        """Generate recommendations for improving the catalog"""
        recommendations = []
        
        if stats['table_count'] > 0 and stats['relationship_count'] == 0:
            recommendations.append("Consider adding relationships between tables to improve query accuracy")
        
        if stats['business_metrics_count'] < 5:
            recommendations.append("Add more business metrics to help with common analytical queries")
        
        if stats['business_rules_count'] < 3:
            recommendations.append("Define business rules to ensure consistent query interpretation")
        
        if validation_results.get('warnings'):
            recommendations.append("Address validation warnings to improve data model completeness")
        
        return recommendations
    
    def get_required_parameters(self) -> List[str]:
        return ['action']
    
    def get_optional_parameters(self) -> List[str]:
        return ['tables', 'relationships', 'context_type', 'name', 'definition', 'description', 'query_text']
    
    def get_description(self) -> str:
        return "Builds and manages semantic catalog with business context for improved query generation"