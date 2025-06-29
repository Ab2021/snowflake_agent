from dataclasses import dataclass, field
from typing import List, Dict, Optional, Any
from datetime import datetime
from enum import Enum

class ColumnType(Enum):
    """Standard column data types"""
    STRING = "STRING"
    INTEGER = "INTEGER"
    FLOAT = "FLOAT"
    DECIMAL = "DECIMAL"
    DATE = "DATE"
    DATETIME = "DATETIME"
    BOOLEAN = "BOOLEAN"
    JSON = "JSON"
    ARRAY = "ARRAY"

class RelationshipType(Enum):
    """Types of table relationships"""
    ONE_TO_ONE = "ONE_TO_ONE"
    ONE_TO_MANY = "ONE_TO_MANY"
    MANY_TO_ONE = "MANY_TO_ONE"
    MANY_TO_MANY = "MANY_TO_MANY"

@dataclass
class Column:
    """Represents a database column with semantic information"""
    name: str
    data_type: str
    business_name: Optional[str] = None
    description: Optional[str] = None
    is_nullable: bool = True
    is_primary_key: bool = False
    is_foreign_key: bool = False
    default_value: Optional[str] = None
    max_length: Optional[int] = None
    precision: Optional[int] = None
    scale: Optional[int] = None
    comment: Optional[str] = None
    semantic_type: Optional[str] = None  # e.g., "email", "phone", "currency"
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'name': self.name,
            'data_type': self.data_type,
            'business_name': self.business_name,
            'description': self.description,
            'is_nullable': self.is_nullable,
            'is_primary_key': self.is_primary_key,
            'is_foreign_key': self.is_foreign_key,
            'default_value': self.default_value,
            'max_length': self.max_length,
            'precision': self.precision,
            'scale': self.scale,
            'comment': self.comment,
            'semantic_type': self.semantic_type
        }

@dataclass
class Relationship:
    """Represents a relationship between tables"""
    source_table: str
    target_table: str
    source_column: str
    target_column: str
    relationship_type: RelationshipType
    name: Optional[str] = None
    description: Optional[str] = None
    is_enforced: bool = False
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'source_table': self.source_table,
            'target_table': self.target_table,
            'source_column': self.source_column,
            'target_column': self.target_column,
            'relationship_type': self.relationship_type.value,
            'name': self.name,
            'description': self.description,
            'is_enforced': self.is_enforced
        }

@dataclass
class Table:
    """Represents a database table with semantic information"""
    name: str
    schema: str
    database: str
    business_name: Optional[str] = None
    description: Optional[str] = None
    table_type: str = "TABLE"  # TABLE, VIEW, MATERIALIZED_VIEW
    columns: List[Column] = field(default_factory=list)
    relationships: List[Relationship] = field(default_factory=list)
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    last_modified: Optional[datetime] = None
    comment: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    
    def get_column(self, column_name: str) -> Optional[Column]:
        """Get column by name"""
        for column in self.columns:
            if column.name.lower() == column_name.lower():
                return column
        return None
    
    def get_primary_keys(self) -> List[Column]:
        """Get all primary key columns"""
        return [col for col in self.columns if col.is_primary_key]
    
    def get_foreign_keys(self) -> List[Column]:
        """Get all foreign key columns"""
        return [col for col in self.columns if col.is_foreign_key]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'name': self.name,
            'schema': self.schema,
            'database': self.database,
            'business_name': self.business_name,
            'description': self.description,
            'table_type': self.table_type,
            'columns': [col.to_dict() for col in self.columns],
            'relationships': [rel.to_dict() for rel in self.relationships],
            'row_count': self.row_count,
            'size_bytes': self.size_bytes,
            'last_modified': self.last_modified.isoformat() if self.last_modified else None,
            'comment': self.comment,
            'tags': self.tags
        }

@dataclass
class SemanticLayer:
    """Represents the semantic layer with business-friendly mappings"""
    name: str
    description: Optional[str] = None
    tables: Dict[str, Table] = field(default_factory=dict)
    business_metrics: Dict[str, str] = field(default_factory=dict)  # metric_name -> SQL expression
    business_dimensions: Dict[str, str] = field(default_factory=dict)  # dimension_name -> column reference
    common_joins: Dict[str, str] = field(default_factory=dict)  # join_name -> SQL join clause
    business_rules: List[str] = field(default_factory=list)
    glossary: Dict[str, str] = field(default_factory=dict)  # term -> definition
    
    def add_table(self, table: Table):
        """Add table to semantic layer"""
        full_name = f"{table.database}.{table.schema}.{table.name}"
        self.tables[full_name] = table
    
    def get_table(self, table_name: str) -> Optional[Table]:
        """Get table by name (supports partial matching)"""
        # Try exact match first
        if table_name in self.tables:
            return self.tables[table_name]
        
        # Try partial match
        for full_name, table in self.tables.items():
            if table.name.lower() == table_name.lower():
                return table
        
        return None
    
    def add_business_metric(self, name: str, sql_expression: str, description: str = None):
        """Add a business metric definition"""
        self.business_metrics[name] = sql_expression
        if description:
            self.glossary[name] = description
    
    def add_business_dimension(self, name: str, column_reference: str, description: str = None):
        """Add a business dimension definition"""
        self.business_dimensions[name] = column_reference
        if description:
            self.glossary[name] = description
    
    def get_context_for_llm(self) -> str:
        """Generate context string for LLM"""
        context_parts = []
        
        # Add business glossary
        if self.glossary:
            context_parts.append("=== BUSINESS GLOSSARY ===")
            for term, definition in self.glossary.items():
                context_parts.append(f"{term}: {definition}")
            context_parts.append("")
        
        # Add business metrics
        if self.business_metrics:
            context_parts.append("=== BUSINESS METRICS ===")
            for metric, sql in self.business_metrics.items():
                context_parts.append(f"{metric}: {sql}")
            context_parts.append("")
        
        # Add table information
        context_parts.append("=== DATABASE SCHEMA ===")
        for table_name, table in self.tables.items():
            context_parts.append(f"\nTable: {table.name}")
            if table.business_name:
                context_parts.append(f"Business Name: {table.business_name}")
            if table.description:
                context_parts.append(f"Description: {table.description}")
            
            context_parts.append("Columns:")
            for col in table.columns:
                col_info = f"  - {col.name} ({col.data_type})"
                if col.business_name:
                    col_info += f" [Business: {col.business_name}]"
                if col.description:
                    col_info += f" - {col.description}"
                if col.is_primary_key:
                    col_info += " [PRIMARY KEY]"
                if col.is_foreign_key:
                    col_info += " [FOREIGN KEY]"
                context_parts.append(col_info)
            
            # Add relationships
            if table.relationships:
                context_parts.append("Relationships:")
                for rel in table.relationships:
                    context_parts.append(f"  - {rel.source_table}.{rel.source_column} -> {rel.target_table}.{rel.target_column} ({rel.relationship_type.value})")
        
        # Add common joins
        if self.common_joins:
            context_parts.append("\n=== COMMON JOINS ===")
            for join_name, join_sql in self.common_joins.items():
                context_parts.append(f"{join_name}: {join_sql}")
        
        # Add business rules
        if self.business_rules:
            context_parts.append("\n=== BUSINESS RULES ===")
            for rule in self.business_rules:
                context_parts.append(f"- {rule}")
        
        return "\n".join(context_parts)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary representation"""
        return {
            'name': self.name,
            'description': self.description,
            'tables': {name: table.to_dict() for name, table in self.tables.items()},
            'business_metrics': self.business_metrics,
            'business_dimensions': self.business_dimensions,
            'common_joins': self.common_joins,
            'business_rules': self.business_rules,
            'glossary': self.glossary
        }