#!/usr/bin/env python3
"""
Cost and Performance Monitoring for GenBI
"""

import time
import json
import logging
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, asdict
from datetime import datetime, timedelta
import threading

@dataclass
class QueryMetrics:
    """Metrics for individual queries"""
    timestamp: datetime
    question: str
    complexity: str
    processing_time: float
    tokens_used: int
    cache_hit: bool
    optimization_applied: bool
    cost_estimated: float
    success: bool
    error_message: Optional[str] = None

@dataclass
class SystemMetrics:
    """Overall system performance metrics"""
    total_queries: int = 0
    successful_queries: int = 0
    cache_hit_rate: float = 0.0
    avg_processing_time: float = 0.0
    total_tokens_saved: int = 0
    total_cost_saved: float = 0.0
    uptime_hours: float = 0.0
    error_rate: float = 0.0

class CostPerformanceMonitor:
    """Monitor and track cost optimization and performance metrics"""
    
    def __init__(self, log_file: str = "genbi_metrics.json"):
        self.log_file = log_file
        self.query_history: List[QueryMetrics] = []
        self.start_time = datetime.now()
        self.lock = threading.Lock()
        self.logger = logging.getLogger("genbi.monitor")
        
    def record_query(self, 
                    question: str,
                    complexity: str,
                    processing_time: float,
                    tokens_used: int = 0,
                    cache_hit: bool = False,
                    optimization_applied: bool = False,
                    cost_estimated: float = 0.0,
                    success: bool = True,
                    error_message: Optional[str] = None) -> None:
        """Record metrics for a single query"""
        
        metrics = QueryMetrics(
            timestamp=datetime.now(),
            question=question[:100],  # Truncate for privacy
            complexity=complexity,
            processing_time=processing_time,
            tokens_used=tokens_used,
            cache_hit=cache_hit,
            optimization_applied=optimization_applied,
            cost_estimated=cost_estimated,
            success=success,
            error_message=error_message
        )
        
        with self.lock:
            self.query_history.append(metrics)
            
            # Keep only last 1000 queries
            if len(self.query_history) > 1000:
                self.query_history = self.query_history[-1000:]
    
    def get_system_metrics(self, hours_back: int = 24) -> SystemMetrics:
        """Calculate system-wide metrics for the specified time period"""
        
        cutoff_time = datetime.now() - timedelta(hours=hours_back)
        
        with self.lock:
            recent_queries = [q for q in self.query_history if q.timestamp >= cutoff_time]
        
        if not recent_queries:
            return SystemMetrics()
        
        total_queries = len(recent_queries)
        successful_queries = sum(1 for q in recent_queries if q.success)
        cache_hits = sum(1 for q in recent_queries if q.cache_hit)
        
        # Calculate averages and rates
        cache_hit_rate = (cache_hits / total_queries) * 100 if total_queries > 0 else 0
        error_rate = ((total_queries - successful_queries) / total_queries) * 100 if total_queries > 0 else 0
        
        avg_processing_time = sum(q.processing_time for q in recent_queries) / total_queries
        total_tokens_saved = sum(q.tokens_used for q in recent_queries if q.optimization_applied)
        total_cost_saved = sum(q.cost_estimated for q in recent_queries if q.optimization_applied)
        
        uptime_hours = (datetime.now() - self.start_time).total_seconds() / 3600
        
        return SystemMetrics(
            total_queries=total_queries,
            successful_queries=successful_queries,
            cache_hit_rate=cache_hit_rate,
            avg_processing_time=avg_processing_time,
            total_tokens_saved=total_tokens_saved,
            total_cost_saved=total_cost_saved,
            uptime_hours=uptime_hours,
            error_rate=error_rate
        )
    
    def get_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report"""
        
        last_24h = self.get_system_metrics(24)
        
        return {
            "report_timestamp": datetime.now().isoformat(),
            "system_uptime_hours": last_24h.uptime_hours,
            "metrics": {
                "last_24_hours": asdict(last_24h)
            },
            "optimization_impact": {
                "cache_hit_rate": f"{last_24h.cache_hit_rate:.1f}%",
                "tokens_saved_24h": last_24h.total_tokens_saved,
                "cost_saved_24h": f"${last_24h.total_cost_saved:.4f}",
                "avg_response_time": f"{last_24h.avg_processing_time:.2f}s"
            }
        }

# Global monitor instance
performance_monitor = CostPerformanceMonitor()