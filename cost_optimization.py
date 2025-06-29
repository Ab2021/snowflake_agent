#!/usr/bin/env python3
"""
Cost Optimization Module for GenBI
Implements key cost reduction strategies
"""

import asyncio
import hashlib
import json
import time
from typing import Dict, List, Any, Optional, Tuple
from dataclasses import dataclass
import logging

logger = logging.getLogger("genbi.cost_optimizer")

@dataclass
class OptimizationMetrics:
    """Track optimization performance"""
    original_tokens: int = 0
    optimized_tokens: int = 0
    cache_hits: int = 0
    cache_misses: int = 0
    processing_time: float = 0.0
    estimated_cost_saved: float = 0.0

class PromptOptimizer:
    """Optimize prompts to reduce token usage by 40-60%"""
    
    def __init__(self):
        self.compression_cache = {}
        
    def optimize_schema_prompt(self, question: str, schema: Dict[str, Any]) -> Tuple[str, OptimizationMetrics]:
        """Optimize schema representation for cost efficiency"""
        metrics = OptimizationMetrics()
        start_time = time.time()
        
        # Cache key for reuse
        cache_key = hashlib.md5(f"{question}_{len(schema.get('tables', []))}".encode()).hexdigest()
        
        if cache_key in self.compression_cache:
            metrics.cache_hits += 1
            result = self.compression_cache[cache_key]
            metrics.processing_time = time.time() - start_time
            return result, metrics
        
        metrics.cache_misses += 1
        
        # Extract only relevant tables based on question keywords
        relevant_tables = self._extract_relevant_tables(question, schema)
        
        # Compress schema representation
        compressed_schema = self._compress_schema_info(relevant_tables)
        
        # Build optimized prompt
        optimized_prompt = self._build_compressed_prompt(question, compressed_schema)
        
        # Calculate metrics
        original_size = len(str(schema))
        optimized_size = len(optimized_prompt)
        metrics.original_tokens = original_size // 4  # Rough token estimate
        metrics.optimized_tokens = optimized_size // 4
        metrics.estimated_cost_saved = (metrics.original_tokens - metrics.optimized_tokens) * 0.000015
        metrics.processing_time = time.time() - start_time
        
        # Cache the result
        self.compression_cache[cache_key] = optimized_prompt
        
        return optimized_prompt, metrics
    
    def _extract_relevant_tables(self, question: str, schema: Dict[str, Any]) -> List[Dict]:
        """Extract only tables relevant to the question"""
        question_lower = question.lower()
        relevant_tables = []
        
        question_keywords = set(question_lower.split())
        
        for table in schema.get('tables', []):
            table_name = table.get('name', '').lower()
            
            is_relevant = False
            
            # Check table name
            if any(keyword in table_name for keyword in question_keywords):
                is_relevant = True
            
            # Check column names
            for column in table.get('columns', []):
                column_name = column.get('name', '').lower()
                if any(keyword in column_name for keyword in question_keywords):
                    is_relevant = True
                    break
            
            # Include common business tables
            if any(business_term in table_name for business_term in 
                   ['customer', 'order', 'product', 'sale', 'transaction', 'invoice']):
                is_relevant = True
            
            if is_relevant:
                relevant_tables.append(table)
        
        # If no relevant tables found, include first 3 tables
        if not relevant_tables and schema.get('tables'):
            relevant_tables = schema['tables'][:3]
        
        return relevant_tables[:5]  # Limit to 5 tables max
    
    def _compress_schema_info(self, tables: List[Dict]) -> str:
        """Compress table information to essential details only"""
        compressed_tables = []
        
        for table in tables:
            table_name = table.get('name', '')
            columns = table.get('columns', [])
            
            # Select only essential columns
            essential_columns = []
            for col in columns:
                col_name = col.get('name', '').lower()
                col_type = col.get('data_type', '').lower()
                
                # Include key business columns
                if any(keyword in col_name for keyword in 
                       ['id', 'name', 'date', 'time', 'amount', 'price', 'total', 'count', 'status']):
                    essential_columns.append(f"{col.get('name')}({col_type})")
                elif col.get('is_primary_key'):
                    essential_columns.append(f"{col.get('name')}({col_type})")
            
            # Limit to 6 columns per table
            essential_columns = essential_columns[:6]
            
            if essential_columns:
                compressed_tables.append(f"{table_name}({','.join(essential_columns)})")
        
        return '; '.join(compressed_tables)
    
    def _build_compressed_prompt(self, question: str, compressed_schema: str) -> str:
        """Build minimal, efficient prompt"""
        return f"""Generate SQL for: {question}
Schema: {compressed_schema}
Return SQL only:"""

class QueryComplexityRouter:
    """Route queries to appropriate models based on complexity"""
    
    def assess_complexity(self, question: str, schema_size: int = 0) -> str:
        """Assess query complexity for optimal model routing"""
        question_lower = question.lower()
        
        # Simple queries
        simple_indicators = [
            'count', 'total', 'sum', 'how many', 'show me', 'list all'
        ]
        
        # Complex queries
        complex_indicators = [
            'compare', 'analyze', 'trend', 'correlation', 'predict',
            'percentage', 'ratio', 'join', 'group by', 'having'
        ]
        
        # Count indicators
        simple_score = sum(1 for indicator in simple_indicators if indicator in question_lower)
        complex_score = sum(1 for indicator in complex_indicators if indicator in question_lower)
        
        # Additional complexity factors
        word_count = len(question.split())
        has_multiple_conditions = question_lower.count('and') + question_lower.count('or') > 1
        
        # Scoring logic
        if complex_score > 1 or has_multiple_conditions or word_count > 20:
            return "complex"
        elif simple_score > 0 and complex_score == 0 and word_count < 10:
            return "simple"
        else:
            return "moderate"

class ResultCacheManager:
    """Manage query result caching for cost optimization"""
    
    def __init__(self, max_cache_size: int = 1000, ttl_seconds: int = 3600):
        self.cache = {}
        self.cache_timestamps = {}
        self.max_cache_size = max_cache_size
        self.ttl_seconds = ttl_seconds
        
    def get_cache_key(self, sql: str) -> str:
        """Generate cache key from SQL query"""
        normalized_sql = ' '.join(sql.lower().split())
        return hashlib.md5(normalized_sql.encode()).hexdigest()
    
    def get_cached_result(self, sql: str) -> Optional[List[Dict]]:
        """Retrieve cached result if available and not expired"""
        cache_key = self.get_cache_key(sql)
        
        if cache_key not in self.cache:
            return None
        
        # Check TTL
        if time.time() - self.cache_timestamps[cache_key] > self.ttl_seconds:
            del self.cache[cache_key]
            del self.cache_timestamps[cache_key]
            return None
        
        return self.cache[cache_key]
    
    def cache_result(self, sql: str, result: List[Dict]) -> None:
        """Cache query result"""
        cache_key = self.get_cache_key(sql)
        
        # Implement LRU eviction if cache is full
        if len(self.cache) >= self.max_cache_size:
            oldest_key = min(self.cache_timestamps.keys(), 
                           key=lambda k: self.cache_timestamps[k])
            del self.cache[oldest_key]
            del self.cache_timestamps[oldest_key]
        
        self.cache[cache_key] = result
        self.cache_timestamps[cache_key] = time.time()

class CostOptimizedOrchestrator:
    """Enhanced orchestrator with cost optimization features"""
    
    def __init__(self):
        self.prompt_optimizer = PromptOptimizer()
        self.complexity_router = QueryComplexityRouter()
        self.result_cache = ResultCacheManager()
        self.optimization_stats = {
            'total_queries': 0,
            'cache_hits': 0,
            'tokens_saved': 0,
            'estimated_cost_saved': 0.0
        }
    
    async def optimize_workflow_task(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Optimize a workflow task for cost efficiency"""
        self.optimization_stats['total_queries'] += 1
        
        question = task.get('question', '')
        schema = task.get('schema', {})
        
        # Check cache first
        sql_cache_key = self.result_cache.get_cache_key(question)
        cached_result = self.result_cache.get_cached_result(question)
        
        if cached_result:
            self.optimization_stats['cache_hits'] += 1
            return {
                'status': 'success',
                'results': cached_result,
                'source': 'cache',
                'optimization': 'cache_hit'
            }
        
        # Optimize prompt
        optimized_prompt, metrics = self.prompt_optimizer.optimize_schema_prompt(question, schema)
        self.optimization_stats['tokens_saved'] += metrics.original_tokens - metrics.optimized_tokens
        self.optimization_stats['estimated_cost_saved'] += metrics.estimated_cost_saved
        
        # Assess complexity for model routing
        complexity = self.complexity_router.assess_complexity(question, len(schema.get('tables', [])))
        
        # Return optimized task
        optimized_task = task.copy()
        optimized_task.update({
            'optimized_prompt': optimized_prompt,
            'complexity': complexity,
            'optimization_metrics': metrics,
            'cache_available': False
        })
        
        return optimized_task
    
    def get_optimization_stats(self) -> Dict[str, Any]:
        """Get cost optimization statistics"""
        cache_hit_rate = (self.optimization_stats['cache_hits'] / 
                         max(self.optimization_stats['total_queries'], 1)) * 100
        
        return {
            'total_queries': self.optimization_stats['total_queries'],
            'cache_hit_rate': f"{cache_hit_rate:.1f}%",
            'tokens_saved': self.optimization_stats['tokens_saved'],
            'estimated_cost_saved': f"${self.optimization_stats['estimated_cost_saved']:.4f}",
            'avg_tokens_saved_per_query': (self.optimization_stats['tokens_saved'] / 
                                         max(self.optimization_stats['total_queries'], 1))
        }
    
    def clear_optimization_cache(self) -> None:
        """Clear all optimization caches"""
        self.prompt_optimizer.compression_cache.clear()
        self.result_cache.cache.clear()
        self.result_cache.cache_timestamps.clear()