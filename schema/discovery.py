from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

from database import SnowflakeConnector
from .models import Table, Column, Relationship, RelationshipType

class SchemaDiscovery:
    """Handles automated database schema discovery"""
    
    def __init__(self, connector: SnowflakeConnector = None):
        self.connector = connector or SnowflakeConnector()
        self.logger = logging.getLogger("genbi.schema.discovery")
    
    def discover_database_schema(self, database: str = None, schema: str = 'PUBLIC', 
                                include_system_tables: bool = False) -> Dict[str, Any]:
        """Discover complete database schema"""
        self.logger.info(f"Starting schema discovery for {database}.{schema}")
        
        discovery_result = {
            'tables': [],
            'relationships': [],
            'discovery_metadata': {
                'timestamp': datetime.now().isoformat(),
                'database': database,
                'schema': schema,
                'include_system_tables': include_system_tables
            }
        }
        
        try:
            # Discover tables
            tables = self._discover_tables(schema, include_system_tables)
            discovery_result['tables'] = tables
            
            # Discover relationships
            relationships = self._discover_relationships(tables, schema)
            discovery_result['relationships'] = relationships
            
            # Add summary statistics
            discovery_result['discovery_metadata'].update({
                'tables_found': len(tables),
                'relationships_found': len(relationships),
                'total_columns': sum(len(table.columns) for table in tables),
                'status': 'success'
            })
            
            self.logger.info(f"Discovery completed: {len(tables)} tables, {len(relationships)} relationships")
            
        except Exception as e:
            self.logger.error(f"Schema discovery failed: {e}")
            discovery_result['discovery_metadata'].update({
                'status': 'error',
                'error': str(e)
            })
        
        return discovery_result
    
    def _discover_tables(self, schema: str, include_system_tables: bool) -> List[Table]:
        """Discover all tables in the schema"""
        tables = []
        
        # Get table list
        tables_query = f"""
        SELECT 
            TABLE_NAME,
            TABLE_TYPE,
            COMMENT,
            ROW_COUNT,
            BYTES,
            CREATED,
            LAST_ALTERED
        FROM INFORMATION_SCHEMA.TABLES
        WHERE TABLE_SCHEMA = '{schema.upper()}'
        """
        
        if not include_system_tables:
            tables_query += " AND TABLE_TYPE IN ('BASE TABLE', 'VIEW')"
        
        tables_result = self.connector.execute_query(tables_query)
        
        if not tables_result:
            return tables
        
        for table_info in tables_result:
            table_name = table_info['TABLE_NAME']
            
            # Get column information for this table
            columns = self._discover_table_columns(table_name, schema)
            
            # Create table object
            table = Table(
                name=table_name,
                schema=schema,
                database=table_info.get('TABLE_CATALOG', 'UNKNOWN'),
                table_type=table_info.get('TABLE_TYPE', 'TABLE'),
                columns=columns,
                row_count=table_info.get('ROW_COUNT'),
                size_bytes=table_info.get('BYTES'),
                comment=table_info.get('COMMENT'),
                last_modified=table_info.get('LAST_ALTERED')
            )
            
            # Add business name inference
            table.business_name = self._infer_business_name(table_name)
            
            tables.append(table)
        
        return tables
    
    def _discover_table_columns(self, table_name: str, schema: str) -> List[Column]:
        """Discover columns for a specific table"""
        columns = []
        
        columns_query = f"""
        SELECT 
            COLUMN_NAME,
            DATA_TYPE,
            IS_NULLABLE,
            COLUMN_DEFAULT,
            COMMENT,
            CHARACTER_MAXIMUM_LENGTH,
            NUMERIC_PRECISION,
            NUMERIC_SCALE,
            ORDINAL_POSITION
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{schema.upper()}'
        AND TABLE_NAME = '{table_name}'
        ORDER BY ORDINAL_POSITION
        """
        
        columns_result = self.connector.execute_query(columns_query)
        
        if not columns_result:
            return columns
        
        # Get primary key information
        primary_keys = self._get_primary_keys(table_name, schema)
        
        for col_info in columns_result:
            column_name = col_info['COLUMN_NAME']
            
            column = Column(
                name=column_name,
                data_type=col_info['DATA_TYPE'],
                is_nullable=col_info['IS_NULLABLE'] == 'YES',
                is_primary_key=column_name in primary_keys,
                default_value=col_info.get('COLUMN_DEFAULT'),
                comment=col_info.get('COMMENT'),
                max_length=col_info.get('CHARACTER_MAXIMUM_LENGTH'),
                precision=col_info.get('NUMERIC_PRECISION'),
                scale=col_info.get('NUMERIC_SCALE')
            )
            
            # Infer semantic information
            column.business_name = self._infer_column_business_name(column_name)
            column.semantic_type = self._infer_semantic_type(column_name, column.data_type)
            
            columns.append(column)
        
        return columns
    
    def _get_primary_keys(self, table_name: str, schema: str) -> List[str]:
        """Get primary key columns for a table"""
        try:
            pk_query = f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE
            WHERE TABLE_SCHEMA = '{schema.upper()}'
            AND TABLE_NAME = '{table_name}'
            AND CONSTRAINT_NAME LIKE '%PRIMARY%'
            """
            
            pk_result = self.connector.execute_query(pk_query)
            return [row['COLUMN_NAME'] for row in pk_result or []]
            
        except Exception:
            # If primary key discovery fails, try common patterns
            return self._infer_primary_keys(table_name)
    
    def _infer_primary_keys(self, table_name: str) -> List[str]:
        """Infer primary keys based on common naming patterns"""
        common_pk_names = [
            'ID',
            f'{table_name}_ID',
            f'{table_name.rstrip("S")}_ID',  # Remove trailing S
            'PK',
            'KEY'
        ]
        
        # This would need to be validated against actual columns
        # For now, return common patterns
        return ['ID'] if 'ID' in common_pk_names else []
    
    def _discover_relationships(self, tables: List[Table], schema: str) -> List[Relationship]:
        """Discover relationships between tables"""
        relationships = []
        
        # Try to discover explicit foreign key constraints
        try:
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
                
        except Exception as e:
            self.logger.warning(f"Could not discover explicit foreign keys: {e}")
        
        # Infer relationships from naming patterns
        inferred_relationships = self._infer_relationships(tables)
        relationships.extend(inferred_relationships)
        
        return relationships
    
    def _infer_relationships(self, tables: List[Table]) -> List[Relationship]:
        """Infer relationships based on naming patterns"""
        relationships = []
        table_by_name = {table.name: table for table in tables}
        
        for table in tables:
            for column in table.columns:
                # Look for foreign key patterns
                if column.name.upper().endswith('_ID') and not column.is_primary_key:
                    potential_target = column.name[:-3]  # Remove '_ID'
                    
                    # Try different variations
                    target_variations = [
                        potential_target.upper(),
                        potential_target.upper() + 'S',  # Plural
                        potential_target.upper()[:-1] if potential_target.endswith('S') else None,  # Singular
                        potential_target.upper() + '_DIM',  # Dimension table
                        potential_target.upper() + '_MASTER'  # Master table
                    ]
                    
                    for variation in target_variations:
                        if variation and variation in table_by_name:
                            target_table = table_by_name[variation]
                            
                            # Find matching column in target table
                            target_column = None
                            for target_col in target_table.columns:
                                if (target_col.is_primary_key or 
                                    target_col.name.upper() in ['ID', variation + '_ID']):
                                    target_column = target_col
                                    break
                            
                            if target_column:
                                relationship = Relationship(
                                    source_table=table.name,
                                    target_table=target_table.name,
                                    source_column=column.name,
                                    target_column=target_column.name,
                                    relationship_type=RelationshipType.MANY_TO_ONE,
                                    description=f"Inferred from naming pattern: {column.name}",
                                    is_enforced=False
                                )
                                relationships.append(relationship)
                                
                                # Mark column as foreign key
                                column.is_foreign_key = True
                            break
        
        return relationships
    
    def _infer_business_name(self, table_name: str) -> str:
        """Infer business-friendly table name"""
        # Convert snake_case to Title Case
        name = table_name.replace('_', ' ').title()
        
        # Handle common suffixes
        if name.endswith(' Dim'):
            name = name[:-4] + ' Dimension'
        elif name.endswith(' Fact'):
            name = name[:-5] + ' Facts'
        elif name.endswith(' Master'):
            name = name[:-7] + ' Master Data'
        
        return name
    
    def _infer_column_business_name(self, column_name: str) -> str:
        """Infer business-friendly column name"""
        # Convert snake_case to Title Case
        name = column_name.replace('_', ' ').title()
        
        # Handle common patterns
        if name.endswith(' Id'):
            name = name[:-3] + ' ID'
        elif name.endswith(' Cd'):
            name = name[:-3] + ' Code'
        elif name.endswith(' Dt'):
            name = name[:-3] + ' Date'
        elif name.endswith(' Amt'):
            name = name[:-4] + ' Amount'
        elif name.endswith(' Qty'):
            name = name[:-4] + ' Quantity'
        
        return name
    
    def _infer_semantic_type(self, column_name: str, data_type: str) -> Optional[str]:
        """Infer semantic type from column name and data type"""
        name_lower = column_name.lower()
        
        # Email patterns
        if any(pattern in name_lower for pattern in ['email', 'mail']):
            return 'email'
        
        # Phone patterns
        elif any(pattern in name_lower for pattern in ['phone', 'tel', 'mobile', 'contact']):
            return 'phone'
        
        # URL patterns
        elif any(pattern in name_lower for pattern in ['url', 'link', 'website', 'web']):
            return 'url'
        
        # Date/Time patterns
        elif any(pattern in name_lower for pattern in ['date', 'time', 'created', 'updated', 'modified']):
            return 'datetime'
        
        # Identifier patterns
        elif any(pattern in name_lower for pattern in ['id', '_id', 'key', 'code']):
            return 'identifier'
        
        # Currency patterns
        elif any(pattern in name_lower for pattern in ['amount', 'price', 'cost', 'value', 'total', 'sum']):
            return 'currency'
        
        # Quantity patterns
        elif any(pattern in name_lower for pattern in ['count', 'qty', 'quantity', 'num', 'number']):
            return 'quantity'
        
        # Address patterns
        elif any(pattern in name_lower for pattern in ['address', 'street', 'city', 'state', 'zip']):
            return 'address'
        
        # Name patterns
        elif any(pattern in name_lower for pattern in ['name', 'title', 'label']):
            return 'name'
        
        # Description patterns
        elif any(pattern in name_lower for pattern in ['description', 'desc', 'comment', 'note']):
            return 'description'
        
        # Status patterns
        elif any(pattern in name_lower for pattern in ['status', 'state', 'flag', 'active']):
            return 'status'
        
        return None