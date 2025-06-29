from typing import Dict, Any, List, Optional
import logging
from datetime import datetime

from .base_agent import BaseAgent
from tools.analysis_tools import StatisticalAnalysisTool, TrendAnalysisTool, InsightGeneratorTool

class AnalysisAgent(BaseAgent):
    """Agent responsible for data analysis and insight generation"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("analysis_agent", config)
        
        # Register tools
        self.register_tool("statistical_analysis", StatisticalAnalysisTool(config))
        self.register_tool("trend_analysis", TrendAnalysisTool(config))
        self.register_tool("insight_generator", InsightGeneratorTool(config))
        
        # Agent configuration
        self.analysis_depth = self.config.get('analysis_depth', 'comprehensive')
        self.confidence_level = self.config.get('confidence_level', 0.95)
        self.include_trends = self.config.get('include_trends', True)
        self.business_context_enabled = self.config.get('business_context', True)
    
    def execute(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Execute analysis-related tasks"""
        if not self.validate_input(task):
            return {
                'status': 'error',
                'message': 'Invalid task input',
                'agent': self.name
            }
        
        task_type = task.get('type')
        
        try:
            if task_type == 'analyze_results':
                return self._analyze_results(task)
            elif task_type == 'statistical_analysis':
                return self._perform_statistical_analysis(task)
            elif task_type == 'trend_analysis':
                return self._perform_trend_analysis(task)
            elif task_type == 'generate_insights':
                return self._generate_insights(task)
            elif task_type == 'comprehensive_analysis':
                return self._comprehensive_analysis(task)
            else:
                return {
                    'status': 'error',
                    'message': f'Unknown task type: {task_type}',
                    'agent': self.name
                }
        
        except Exception as e:
            self.logger.error(f"Analysis agent task failed: {e}")
            return {
                'status': 'error',
                'message': str(e),
                'agent': self.name
            }
    
    def _analyze_results(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Analyze query results with appropriate analysis type"""
        data = task.get('data')
        question = task.get('question', '')
        analysis_type = task.get('analysis_type', self.analysis_depth)
        
        if not data:
            return {
                'status': 'error',
                'message': 'No data provided for analysis',
                'agent': self.name
            }
        
        self.logger.info(f"Analyzing results for question: {question[:100]}...")
        
        analysis_results = {}
        
        # Always perform statistical analysis
        statistical_result = self.use_tool('statistical_analysis',
                                         data=data,
                                         analysis_type=analysis_type,
                                         confidence_level=self.confidence_level)
        
        analysis_results['statistical'] = statistical_result
        
        # Perform trend analysis if data appears to be time-series
        if self.include_trends and self._has_time_dimension(data):
            trend_result = self.use_tool('trend_analysis',
                                       data=data,
                                       period=self._detect_time_period(data))
            analysis_results['trends'] = trend_result
        
        # Generate insights
        insight_result = self.use_tool('insight_generator',
                                     question=question,
                                     data_results=data,
                                     statistical_analysis=statistical_result,
                                     trend_analysis=analysis_results.get('trends'),
                                     business_context=task.get('business_context', ''))
        
        analysis_results['insights'] = insight_result
        
        # Store analysis in context
        self.update_context('last_analysis', {
            'question': question,
            'data_size': len(data) if isinstance(data, list) else 1,
            'analysis_results': analysis_results,
            'analysis_timestamp': datetime.now().isoformat()
        })
        
        return {
            'status': 'success',
            'message': 'Analysis completed successfully',
            'analysis_results': analysis_results,
            'summary': self._create_analysis_summary(analysis_results),
            'agent': self.name
        }
    
    def _perform_statistical_analysis(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Perform dedicated statistical analysis"""
        data = task.get('data')
        
        if not data:
            return {
                'status': 'error',
                'message': 'No data provided for statistical analysis',
                'agent': self.name
            }
        
        self.logger.info("Performing statistical analysis")
        
        statistical_result = self.use_tool('statistical_analysis',
                                         data=data,
                                         analysis_type=task.get('analysis_type', 'comprehensive'),
                                         confidence_level=task.get('confidence_level', self.confidence_level))
        
        return {
            'status': 'success',
            'message': 'Statistical analysis completed',
            'statistical_result': statistical_result,
            'agent': self.name
        }
    
    def _perform_trend_analysis(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Perform dedicated trend analysis"""
        data = task.get('data')
        
        if not data:
            return {
                'status': 'error',
                'message': 'No data provided for trend analysis',
                'agent': self.name
            }
        
        self.logger.info("Performing trend analysis")
        
        trend_result = self.use_tool('trend_analysis',
                                   data=data,
                                   date_column=task.get('date_column'),
                                   value_columns=task.get('value_columns'),
                                   period=task.get('period', 'daily'))
        
        return {
            'status': 'success',
            'message': 'Trend analysis completed',
            'trend_result': trend_result,
            'agent': self.name
        }
    
    def _generate_insights(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Generate insights from provided analysis"""
        question = task.get('question')
        data_results = task.get('data_results')
        
        if not question or not data_results:
            return {
                'status': 'error',
                'message': 'Question and data results are required for insight generation',
                'agent': self.name
            }
        
        self.logger.info("Generating insights")
        
        insight_result = self.use_tool('insight_generator',
                                     question=question,
                                     data_results=data_results,
                                     statistical_analysis=task.get('statistical_analysis'),
                                     trend_analysis=task.get('trend_analysis'),
                                     business_context=task.get('business_context', ''))
        
        return {
            'status': 'success',
            'message': 'Insights generated successfully',
            'insight_result': insight_result,
            'agent': self.name
        }
    
    def _comprehensive_analysis(self, task: Dict[str, Any]) -> Dict[str, Any]:
        """Perform comprehensive analysis including all available tools"""
        data = task.get('data')
        question = task.get('question', '')
        
        if not data:
            return {
                'status': 'error',
                'message': 'No data provided for comprehensive analysis',
                'agent': self.name
            }
        
        self.logger.info("Performing comprehensive analysis")
        
        comprehensive_results = {}
        
        # Step 1: Statistical Analysis
        statistical_task = {
            'type': 'statistical_analysis',
            'data': data,
            'analysis_type': 'comprehensive'
        }
        statistical_result = self._perform_statistical_analysis(statistical_task)
        comprehensive_results['statistical'] = statistical_result
        
        # Step 2: Trend Analysis (if applicable)
        if self._has_time_dimension(data):
            trend_task = {
                'type': 'trend_analysis',
                'data': data,
                'period': self._detect_time_period(data)
            }
            trend_result = self._perform_trend_analysis(trend_task)
            comprehensive_results['trends'] = trend_result
        
        # Step 3: Insight Generation
        insight_task = {
            'type': 'generate_insights',
            'question': question,
            'data_results': data,
            'statistical_analysis': statistical_result.get('statistical_result'),
            'trend_analysis': comprehensive_results.get('trends', {}).get('trend_result'),
            'business_context': task.get('business_context', '')
        }
        insight_result = self._generate_insights(insight_task)
        comprehensive_results['insights'] = insight_result
        
        return {
            'status': 'success',
            'message': 'Comprehensive analysis completed',
            'comprehensive_results': comprehensive_results,
            'executive_summary': self._create_executive_summary(comprehensive_results),
            'agent': self.name
        }
    
    def _has_time_dimension(self, data: List[Dict]) -> bool:
        """Check if data has time dimension for trend analysis"""
        if not data or not isinstance(data, list) or len(data) == 0:
            return False
        
        first_row = data[0]
        if not isinstance(first_row, dict):
            return False
        
        # Look for common date/time column patterns
        time_patterns = ['date', 'time', 'created', 'updated', 'timestamp', 'day', 'month', 'year']
        
        for key in first_row.keys():
            key_lower = key.lower()
            if any(pattern in key_lower for pattern in time_patterns):
                return True
        
        return False
    
    def _detect_time_period(self, data: List[Dict]) -> str:
        """Detect appropriate time period for trend analysis"""
        if len(data) <= 7:
            return 'daily'
        elif len(data) <= 31:
            return 'daily'
        elif len(data) <= 90:
            return 'weekly'
        elif len(data) <= 365:
            return 'monthly'
        else:
            return 'quarterly'
    
    def _create_analysis_summary(self, analysis_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create summary of analysis results"""
        summary = {
            'analyses_performed': list(analysis_results.keys()),
            'key_metrics': {},
            'main_insights': []
        }
        
        # Extract key metrics from statistical analysis
        if 'statistical' in analysis_results:
            stat_result = analysis_results['statistical']
            if stat_result.get('status') == 'success':
                analysis_data = stat_result.get('analysis', {})
                if 'descriptive' in analysis_data:
                    summary['key_metrics']['row_count'] = analysis_data['descriptive'].get('row_count', 0)
                    summary['key_metrics']['column_count'] = analysis_data['descriptive'].get('column_count', 0)
                    summary['key_metrics']['numeric_columns'] = len(analysis_data['descriptive'].get('numeric_columns', {}))
        
        # Extract main insights
        if 'insights' in analysis_results:
            insight_result = analysis_results['insights']
            if insight_result.get('status') == 'success':
                insights = insight_result.get('insight_result', {}).get('insights', '')
                if insights:
                    # Take first few sentences as main insights
                    insight_sentences = insights.split('.')[:3]
                    summary['main_insights'] = [s.strip() + '.' for s in insight_sentences if s.strip()]
        
        return summary
    
    def _create_executive_summary(self, comprehensive_results: Dict[str, Any]) -> Dict[str, Any]:
        """Create executive summary of comprehensive analysis"""
        summary = {
            'overview': 'Comprehensive analysis completed successfully',
            'data_quality': 'unknown',
            'key_findings': [],
            'recommendations': [],
            'analysis_completeness': 0
        }
        
        completed_analyses = 0
        total_possible = 3  # statistical, trends, insights
        
        # Check statistical analysis
        if 'statistical' in comprehensive_results:
            completed_analyses += 1
            stat_result = comprehensive_results['statistical']
            if stat_result.get('status') == 'success':
                # Extract data quality info
                analysis_data = stat_result.get('statistical_result', {}).get('analysis', {})
                if 'data_quality' in analysis_data:
                    quality_metrics = analysis_data['data_quality'].get('completeness', {})
                    avg_completeness = sum(q.get('completeness_ratio', 0) for q in quality_metrics.values()) / len(quality_metrics) if quality_metrics else 0
                    if avg_completeness > 0.95:
                        summary['data_quality'] = 'excellent'
                    elif avg_completeness > 0.85:
                        summary['data_quality'] = 'good'
                    else:
                        summary['data_quality'] = 'needs_attention'
        
        # Check trend analysis
        if 'trends' in comprehensive_results:
            completed_analyses += 1
        
        # Check insights
        if 'insights' in comprehensive_results:
            completed_analyses += 1
            insight_result = comprehensive_results['insights']
            if insight_result.get('status') == 'success':
                structured_insights = insight_result.get('insight_result', {}).get('structured_insights', {})
                summary['key_findings'] = structured_insights.get('key_findings', [])[:3]
                summary['recommendations'] = structured_insights.get('recommendations', [])[:3]
        
        summary['analysis_completeness'] = (completed_analyses / total_possible) * 100
        
        return summary
    
    def get_required_fields(self) -> List[str]:
        return ['type']
    
    def get_capabilities(self) -> List[str]:
        return [
            'statistical_data_analysis',
            'trend_pattern_detection',
            'business_insight_generation',
            'data_quality_assessment',
            'comprehensive_analysis_orchestration',
            'executive_summary_creation'
        ]
    
    def get_analysis_statistics(self) -> Dict[str, Any]:
        """Get analysis statistics"""
        last_analysis = self.get_context('last_analysis')
        
        stats = {
            'agent_name': self.name,
            'analysis_depth': self.analysis_depth,
            'confidence_level': self.confidence_level,
            'trends_enabled': self.include_trends,
            'business_context_enabled': self.business_context_enabled,
            'last_analysis_available': last_analysis is not None
        }
        
        if last_analysis:
            stats['last_data_size'] = last_analysis.get('data_size', 0)
            stats['last_analyses_performed'] = list(last_analysis.get('analysis_results', {}).keys())
            stats['last_analysis_timestamp'] = last_analysis.get('analysis_timestamp')
        
        return stats