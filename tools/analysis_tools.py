from typing import Dict, Any, List, Optional, Union
import statistics
import json
from datetime import datetime, timedelta
import pandas as pd
import numpy as np

from .base_tool import BaseTool
from llm_client import LLMClient

class StatisticalAnalysisTool(BaseTool):
    """Tool for performing statistical analysis on query results"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("statistical_analysis", config)
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Perform statistical analysis on query results"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for statistical analysis")
            
            data = kwargs.get('data')
            analysis_type = kwargs.get('analysis_type', 'comprehensive')
            confidence_level = kwargs.get('confidence_level', 0.95)
            
            # Convert to DataFrame for easier analysis
            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
            else:
                return {
                    'status': 'error',
                    'message': 'No data provided for analysis',
                    'analysis': None
                }
            
            analysis_result = {}
            
            # Basic descriptive statistics
            if analysis_type in ['basic', 'comprehensive']:
                analysis_result['descriptive'] = self._descriptive_analysis(df)
            
            # Advanced statistical analysis
            if analysis_type == 'comprehensive':
                analysis_result['advanced'] = self._advanced_analysis(df, confidence_level)
            
            # Data quality assessment
            analysis_result['data_quality'] = self._data_quality_analysis(df)
            
            # Generate insights
            insights = self._generate_statistical_insights(analysis_result, df)
            
            result = {
                'status': 'success',
                'data_shape': df.shape,
                'analysis': analysis_result,
                'insights': insights,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            return self._post_execute(result, **kwargs)
            
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _descriptive_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Perform descriptive statistical analysis"""
        analysis = {
            'row_count': len(df),
            'column_count': len(df.columns),
            'numeric_columns': {},
            'categorical_columns': {},
            'datetime_columns': {}
        }
        
        # Analyze numeric columns
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        for col in numeric_cols:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                analysis['numeric_columns'][col] = {
                    'count': len(col_data),
                    'mean': float(col_data.mean()),
                    'median': float(col_data.median()),
                    'std': float(col_data.std()) if len(col_data) > 1 else 0,
                    'min': float(col_data.min()),
                    'max': float(col_data.max()),
                    'q25': float(col_data.quantile(0.25)),
                    'q75': float(col_data.quantile(0.75)),
                    'null_count': df[col].isnull().sum(),
                    'unique_count': df[col].nunique()
                }
        
        # Analyze categorical columns
        categorical_cols = df.select_dtypes(include=['object', 'string']).columns
        for col in categorical_cols:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                value_counts = col_data.value_counts()
                analysis['categorical_columns'][col] = {
                    'count': len(col_data),
                    'unique_count': df[col].nunique(),
                    'null_count': df[col].isnull().sum(),
                    'most_common': value_counts.head(5).to_dict(),
                    'least_common': value_counts.tail(5).to_dict() if len(value_counts) > 5 else {}
                }
        
        # Analyze datetime columns
        datetime_cols = df.select_dtypes(include=['datetime64']).columns
        for col in datetime_cols:
            col_data = df[col].dropna()
            if len(col_data) > 0:
                analysis['datetime_columns'][col] = {
                    'count': len(col_data),
                    'min_date': col_data.min().isoformat(),
                    'max_date': col_data.max().isoformat(),
                    'date_range_days': (col_data.max() - col_data.min()).days,
                    'null_count': df[col].isnull().sum()
                }
        
        return analysis
    
    def _advanced_analysis(self, df: pd.DataFrame, confidence_level: float) -> Dict[str, Any]:
        """Perform advanced statistical analysis"""
        analysis = {
            'correlations': {},
            'outliers': {},
            'distributions': {},
            'trends': {}
        }
        
        numeric_cols = df.select_dtypes(include=[np.number]).columns
        
        # Correlation analysis
        if len(numeric_cols) > 1:
            correlation_matrix = df[numeric_cols].corr()
            
            # Find strong correlations
            strong_correlations = []
            for i in range(len(correlation_matrix.columns)):
                for j in range(i + 1, len(correlation_matrix.columns)):
                    corr_value = correlation_matrix.iloc[i, j]
                    if not pd.isna(corr_value) and abs(corr_value) > 0.7:
                        strong_correlations.append({
                            'column1': correlation_matrix.columns[i],
                            'column2': correlation_matrix.columns[j],
                            'correlation': float(corr_value),
                            'strength': 'strong positive' if corr_value > 0.7 else 'strong negative'
                        })
            
            analysis['correlations'] = {
                'matrix': correlation_matrix.to_dict(),
                'strong_correlations': strong_correlations
            }
        
        # Outlier detection using IQR method
        for col in numeric_cols:
            col_data = df[col].dropna()
            if len(col_data) > 4:  # Need at least 5 points for quartiles
                q1 = col_data.quantile(0.25)
                q3 = col_data.quantile(0.75)
                iqr = q3 - q1
                lower_bound = q1 - 1.5 * iqr
                upper_bound = q3 + 1.5 * iqr
                
                outliers = col_data[(col_data < lower_bound) | (col_data > upper_bound)]
                
                if len(outliers) > 0:
                    analysis['outliers'][col] = {
                        'count': len(outliers),
                        'percentage': (len(outliers) / len(col_data)) * 100,
                        'values': outliers.tolist()[:10],  # Limit to first 10
                        'bounds': {
                            'lower': float(lower_bound),
                            'upper': float(upper_bound)
                        }
                    }
        
        return analysis
    
    def _data_quality_analysis(self, df: pd.DataFrame) -> Dict[str, Any]:
        """Analyze data quality metrics"""
        quality = {
            'completeness': {},
            'consistency': {},
            'validity': {}
        }
        
        # Completeness analysis
        total_rows = len(df)
        for col in df.columns:
            null_count = df[col].isnull().sum()
            completeness_ratio = (total_rows - null_count) / total_rows if total_rows > 0 else 0
            
            quality['completeness'][col] = {
                'complete_count': total_rows - null_count,
                'null_count': null_count,
                'completeness_ratio': float(completeness_ratio),
                'quality_level': self._get_quality_level(completeness_ratio)
            }
        
        # Consistency analysis
        for col in df.select_dtypes(include=[np.number]).columns:
            col_data = df[col].dropna()
            if len(col_data) > 1:
                cv = col_data.std() / col_data.mean() if col_data.mean() != 0 else float('inf')
                quality['consistency'][col] = {
                    'coefficient_of_variation': float(cv),
                    'consistency_level': 'high' if cv < 0.5 else 'medium' if cv < 1.0 else 'low'
                }
        
        return quality
    
    def _get_quality_level(self, ratio: float) -> str:
        """Determine quality level based on completeness ratio"""
        if ratio >= 0.95:
            return 'excellent'
        elif ratio >= 0.90:
            return 'good'
        elif ratio >= 0.75:
            return 'acceptable'
        else:
            return 'poor'
    
    def _generate_statistical_insights(self, analysis: Dict[str, Any], df: pd.DataFrame) -> List[str]:
        """Generate insights from statistical analysis"""
        insights = []
        
        # Data size insights
        row_count = analysis['descriptive']['row_count']
        col_count = analysis['descriptive']['column_count']
        
        insights.append(f"Dataset contains {row_count:,} rows and {col_count} columns")
        
        # Numeric column insights
        numeric_cols = analysis['descriptive']['numeric_columns']
        if numeric_cols:
            insights.append(f"Found {len(numeric_cols)} numeric columns for quantitative analysis")
            
            # Find columns with high variation
            high_variation_cols = []
            for col, stats in numeric_cols.items():
                if stats['std'] > 0 and abs(stats['std'] / stats['mean']) > 1.0:
                    high_variation_cols.append(col)
            
            if high_variation_cols:
                insights.append(f"High variation detected in: {', '.join(high_variation_cols)}")
        
        # Data quality insights
        if 'data_quality' in analysis:
            poor_quality_cols = []
            for col, quality in analysis['data_quality']['completeness'].items():
                if quality['quality_level'] in ['poor', 'acceptable']:
                    poor_quality_cols.append(f"{col} ({quality['completeness_ratio']:.1%} complete)")
            
            if poor_quality_cols:
                insights.append(f"Data quality concerns in: {', '.join(poor_quality_cols)}")
        
        # Correlation insights
        if 'advanced' in analysis and analysis['advanced']['correlations'].get('strong_correlations'):
            strong_corrs = analysis['advanced']['correlations']['strong_correlations']
            insights.append(f"Found {len(strong_corrs)} strong correlations between numeric variables")
        
        return insights
    
    def get_required_parameters(self) -> List[str]:
        return ['data']
    
    def get_optional_parameters(self) -> List[str]:
        return ['analysis_type', 'confidence_level']
    
    def get_description(self) -> str:
        return "Performs comprehensive statistical analysis on query results"

class TrendAnalysisTool(BaseTool):
    """Tool for analyzing trends in time-series data"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("trend_analysis", config)
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Analyze trends in time-series data"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for trend analysis")
            
            data = kwargs.get('data')
            date_column = kwargs.get('date_column')
            value_columns = kwargs.get('value_columns', [])
            period = kwargs.get('period', 'daily')  # daily, weekly, monthly, quarterly
            
            if isinstance(data, list) and data:
                df = pd.DataFrame(data)
            else:
                return {
                    'status': 'error',
                    'message': 'No data provided for trend analysis'
                }
            
            # Auto-detect date and value columns if not specified
            if not date_column:
                date_column = self._detect_date_column(df)
            
            if not value_columns:
                value_columns = self._detect_value_columns(df)
            
            if not date_column or not value_columns:
                return {
                    'status': 'error',
                    'message': 'Could not identify date or value columns for trend analysis'
                }
            
            # Prepare data for trend analysis
            trend_data = self._prepare_trend_data(df, date_column, value_columns, period)
            
            # Perform trend analysis
            trends = {}
            for col in value_columns:
                if col in trend_data.columns:
                    trends[col] = self._analyze_column_trend(trend_data, col, period)
            
            # Generate trend insights
            insights = self._generate_trend_insights(trends, period)
            
            result = {
                'status': 'success',
                'date_column': date_column,
                'value_columns': value_columns,
                'period': period,
                'trends': trends,
                'insights': insights,
                'analysis_timestamp': datetime.now().isoformat()
            }
            
            return self._post_execute(result, **kwargs)
            
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _detect_date_column(self, df: pd.DataFrame) -> Optional[str]:
        """Auto-detect date column"""
        for col in df.columns:
            if df[col].dtype in ['datetime64[ns]', 'datetime64']:
                return col
            
            # Try to detect date-like strings
            if df[col].dtype == 'object':
                sample_values = df[col].dropna().head(5)
                try:
                    pd.to_datetime(sample_values)
                    return col
                except:
                    continue
        
        return None
    
    def _detect_value_columns(self, df: pd.DataFrame) -> List[str]:
        """Auto-detect numeric value columns"""
        numeric_cols = df.select_dtypes(include=[np.number]).columns.tolist()
        return numeric_cols
    
    def _prepare_trend_data(self, df: pd.DataFrame, date_col: str, value_cols: List[str], period: str) -> pd.DataFrame:
        """Prepare data for trend analysis"""
        # Convert date column to datetime
        df_copy = df.copy()
        df_copy[date_col] = pd.to_datetime(df_copy[date_col])
        
        # Sort by date
        df_copy = df_copy.sort_values(date_col)
        
        # Aggregate by period if needed
        if period == 'weekly':
            df_copy['period'] = df_copy[date_col].dt.to_period('W')
        elif period == 'monthly':
            df_copy['period'] = df_copy[date_col].dt.to_period('M')
        elif period == 'quarterly':
            df_copy['period'] = df_copy[date_col].dt.to_period('Q')
        else:  # daily
            df_copy['period'] = df_copy[date_col].dt.date
        
        # Group by period and aggregate
        agg_data = df_copy.groupby('period')[value_cols].agg(['sum', 'mean', 'count']).reset_index()
        
        return agg_data
    
    def _analyze_column_trend(self, df: pd.DataFrame, column: str, period: str) -> Dict[str, Any]:
        """Analyze trend for a specific column"""
        # Use sum aggregation for trend analysis
        values = df[(column, 'sum')].values
        periods = len(values)
        
        if periods < 2:
            return {
                'direction': 'insufficient_data',
                'strength': 'unknown',
                'statistics': {}
            }
        
        # Calculate trend statistics
        x = np.arange(periods)
        slope, intercept = np.polyfit(x, values, 1)
        
        # Calculate trend strength (R-squared)
        y_pred = slope * x + intercept
        ss_res = np.sum((values - y_pred) ** 2)
        ss_tot = np.sum((values - np.mean(values)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot != 0 else 0
        
        # Determine trend direction and strength
        if abs(slope) < 0.01 * np.mean(values):
            direction = 'stable'
        elif slope > 0:
            direction = 'increasing'
        else:
            direction = 'decreasing'
        
        if r_squared > 0.8:
            strength = 'strong'
        elif r_squared > 0.5:
            strength = 'moderate'
        else:
            strength = 'weak'
        
        # Calculate percentage change
        if len(values) > 1:
            total_change = ((values[-1] - values[0]) / values[0]) * 100 if values[0] != 0 else 0
            avg_period_change = total_change / (periods - 1) if periods > 1 else 0
        else:
            total_change = 0
            avg_period_change = 0
        
        return {
            'direction': direction,
            'strength': strength,
            'statistics': {
                'slope': float(slope),
                'r_squared': float(r_squared),
                'total_change_percent': float(total_change),
                'avg_period_change_percent': float(avg_period_change),
                'start_value': float(values[0]),
                'end_value': float(values[-1]),
                'min_value': float(np.min(values)),
                'max_value': float(np.max(values)),
                'periods_analyzed': periods
            }
        }
    
    def _generate_trend_insights(self, trends: Dict[str, Any], period: str) -> List[str]:
        """Generate insights from trend analysis"""
        insights = []
        
        for column, trend in trends.items():
            direction = trend['direction']
            strength = trend['strength']
            stats = trend['statistics']
            
            if direction == 'insufficient_data':
                insights.append(f"{column}: Insufficient data for trend analysis")
                continue
            
            # Main trend insight
            if direction == 'stable':
                insights.append(f"{column}: Shows stable pattern over the analyzed period")
            else:
                change_desc = f"{direction} with {strength} correlation"
                total_change = stats.get('total_change_percent', 0)
                insights.append(f"{column}: {change_desc} ({total_change:+.1f}% total change)")
            
            # Additional insights based on statistics
            if stats.get('avg_period_change_percent', 0) != 0:
                avg_change = stats['avg_period_change_percent']
                insights.append(f"{column}: Average {period} change of {avg_change:+.1f}%")
        
        return insights
    
    def get_required_parameters(self) -> List[str]:
        return ['data']
    
    def get_optional_parameters(self) -> List[str]:
        return ['date_column', 'value_columns', 'period']
    
    def get_description(self) -> str:
        return "Analyzes trends and patterns in time-series data"

class InsightGeneratorTool(BaseTool):
    """Tool for generating business insights from data analysis"""
    
    def __init__(self, config: Dict[str, Any] = None):
        super().__init__("insight_generator", config)
        self.llm_client = LLMClient()
    
    def execute(self, **kwargs) -> Dict[str, Any]:
        """Generate business insights from analysis results"""
        self._pre_execute(**kwargs)
        
        try:
            if not self.validate_inputs(**kwargs):
                raise ValueError("Invalid inputs for insight generation")
            
            question = kwargs.get('question')
            data_results = kwargs.get('data_results')
            statistical_analysis = kwargs.get('statistical_analysis')
            trend_analysis = kwargs.get('trend_analysis')
            business_context = kwargs.get('business_context', '')
            
            # Prepare comprehensive context for insight generation
            insight_context = self._prepare_insight_context(
                question, data_results, statistical_analysis, trend_analysis, business_context
            )
            
            # Generate insights using LLM
            insights = self.llm_client.analyze_query_results(question, insight_context)
            
            # Enhance insights with structured recommendations
            structured_insights = self._structure_insights(insights, statistical_analysis, trend_analysis)
            
            result = {
                'status': 'success',
                'original_question': question,
                'insights': insights,
                'structured_insights': structured_insights,
                'insight_timestamp': datetime.now().isoformat()
            }
            
            return self._post_execute(result, **kwargs)
            
        except Exception as e:
            return self._handle_error(e, **kwargs)
    
    def _prepare_insight_context(self, question: str, data_results: Any, 
                                statistical_analysis: Dict = None, 
                                trend_analysis: Dict = None,
                                business_context: str = '') -> str:
        """Prepare comprehensive context for insight generation"""
        context_parts = []
        
        # Add original question
        context_parts.append(f"ORIGINAL QUESTION: {question}")
        context_parts.append("")
        
        # Add business context if provided
        if business_context:
            context_parts.append("BUSINESS CONTEXT:")
            context_parts.append(business_context)
            context_parts.append("")
        
        # Add data results
        context_parts.append("QUERY RESULTS:")
        if isinstance(data_results, list):
            context_parts.append(json.dumps(data_results[:100], indent=2, default=str))  # Limit size
            if len(data_results) > 100:
                context_parts.append(f"... and {len(data_results) - 100} more records")
        else:
            context_parts.append(str(data_results))
        context_parts.append("")
        
        # Add statistical analysis if available
        if statistical_analysis:
            context_parts.append("STATISTICAL ANALYSIS:")
            context_parts.append(json.dumps(statistical_analysis, indent=2, default=str))
            context_parts.append("")
        
        # Add trend analysis if available
        if trend_analysis:
            context_parts.append("TREND ANALYSIS:")
            context_parts.append(json.dumps(trend_analysis, indent=2, default=str))
            context_parts.append("")
        
        return "\n".join(context_parts)
    
    def _structure_insights(self, insights: str, statistical_analysis: Dict = None, 
                           trend_analysis: Dict = None) -> Dict[str, Any]:
        """Structure insights into categories"""
        structured = {
            'key_findings': [],
            'trends': [],
            'anomalies': [],
            'recommendations': [],
            'data_quality_notes': []
        }
        
        # Extract key findings from statistical analysis
        if statistical_analysis and 'insights' in statistical_analysis:
            structured['key_findings'].extend(statistical_analysis['insights'])
        
        # Extract trends from trend analysis
        if trend_analysis and 'insights' in trend_analysis:
            structured['trends'].extend(trend_analysis['insights'])
        
        # Add data quality notes
        if statistical_analysis and 'data_quality' in statistical_analysis['analysis']:
            quality_issues = []
            for col, quality in statistical_analysis['analysis']['data_quality']['completeness'].items():
                if quality['quality_level'] in ['poor', 'acceptable']:
                    quality_issues.append(f"{col}: {quality['completeness_ratio']:.1%} complete")
            
            if quality_issues:
                structured['data_quality_notes'] = quality_issues
        
        # Parse insights for recommendations (simple keyword-based approach)
        if insights:
            insight_lines = insights.split('\n')
            for line in insight_lines:
                line = line.strip()
                if any(keyword in line.lower() for keyword in ['recommend', 'suggest', 'should', 'consider']):
                    structured['recommendations'].append(line)
                elif any(keyword in line.lower() for keyword in ['anomaly', 'unusual', 'outlier', 'unexpected']):
                    structured['anomalies'].append(line)
        
        return structured
    
    def get_required_parameters(self) -> List[str]:
        return ['question', 'data_results']
    
    def get_optional_parameters(self) -> List[str]:
        return ['statistical_analysis', 'trend_analysis', 'business_context']
    
    def get_description(self) -> str:
        return "Generates comprehensive business insights from data analysis results"