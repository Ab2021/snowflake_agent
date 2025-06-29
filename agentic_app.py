import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import json
from typing import Dict, List, Optional, Any

from agents.orchestrator_agent import OrchestratorAgent
from schema.catalog import SchemaCatalog
from utils import format_query_result, generate_chart_suggestions

# Page configuration
st.set_page_config(
    page_title="GenBI - Agentic Business Intelligence",
    page_icon="🤖",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Initialize session state
if 'messages' not in st.session_state:
    st.session_state.messages = []
if 'query_history' not in st.session_state:
    st.session_state.query_history = []
if 'semantic_context' not in st.session_state:
    st.session_state.semantic_context = ""
if 'current_results' not in st.session_state:
    st.session_state.current_results = None
if 'orchestrator' not in st.session_state:
    # Initialize orchestrator with configuration
    config = {
        'max_retries': 3,
        'auto_optimize': True,
        'include_analysis': True,
        'cache_schema': True,
        'schema_agent': {
            'auto_discovery': True,
            'discovery_frequency': 'daily',
            'include_system_tables': False
        },
        'sql_agent': {
            'optimization_level': 'moderate',
            'security_mode': 'strict',
            'complexity_threshold': 'medium'
        },
        'analysis_agent': {
            'analysis_depth': 'comprehensive',
            'confidence_level': 0.95,
            'include_trends': True,
            'business_context': True
        }
    }
    st.session_state.orchestrator = OrchestratorAgent(config)
if 'system_initialized' not in st.session_state:
    st.session_state.system_initialized = False

def main():
    st.title("🤖 GenBI - Agentic Business Intelligence")
    st.markdown("Advanced multi-agent BI system that understands your data and generates insights through natural language.")
    
    # Sidebar for system management
    with st.sidebar:
        st.header("🔧 System Management")
        
        # System Status
        st.subheader("System Status")
        if st.button("Check System Status"):
            check_system_status()
        
        # System Initialization
        st.subheader("Initialize System")
        if st.button("Initialize BI System"):
            initialize_system()
        
        # Schema Management
        st.subheader("Schema Management")
        if st.button("Refresh Schema Catalog"):
            refresh_schema()
        
        # Configuration
        st.subheader("Configuration")
        
        # Advanced Options
        with st.expander("Advanced Options"):
            auto_optimize = st.checkbox("Auto-optimize queries", value=True)
            include_analysis = st.checkbox("Include comprehensive analysis", value=True)
            analysis_depth = st.selectbox("Analysis depth", ["basic", "comprehensive"], index=1)
            security_mode = st.selectbox("Security mode", ["standard", "strict"], index=1)
            
            if st.button("Update Configuration"):
                update_configuration(auto_optimize, include_analysis, analysis_depth, security_mode)
        
        # Agent Statistics
        with st.expander("Agent Statistics"):
            if st.button("Show Agent Stats"):
                show_agent_statistics()
        
        # Query History
        st.subheader("Recent Queries")
        display_query_history()
    
    # Main interface
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("💬 Natural Language Query Interface")
        
        # Display chat messages
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
                
                # Display SQL query if available
                if message.get("sql_query"):
                    with st.expander("Generated SQL Query"):
                        st.code(message["sql_query"], language='sql')
                
                # Display workflow details if available
                if message.get("workflow_details"):
                    with st.expander("Workflow Details"):
                        display_workflow_details(message["workflow_details"])
                
                # Display data results if available
                if message.get("data_results"):
                    display_results(message["data_results"])
        
        # Chat input
        if prompt := st.chat_input("Ask any question about your data..."):
            # Check if system is initialized
            if not st.session_state.system_initialized:
                st.warning("System not initialized. Please initialize the BI system first.")
                return
            
            # Add user message to chat
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # Process the question through the orchestrator
            with st.chat_message("assistant"):
                with st.spinner("🤖 Agents are analyzing your question..."):
                    process_agentic_query(prompt)
    
    with col2:
        st.subheader("📊 Analysis & Insights")
        
        # Current Analysis Results
        if st.session_state.current_results:
            display_analysis_panel(st.session_state.current_results)
        else:
            st.info("Execute a query to see comprehensive analysis and insights here")
        
        # Quick Actions
        st.subheader("🚀 Quick Actions")
        if st.button("🔍 Explore Schema"):
            explore_schema()
        
        if st.button("📈 Performance Metrics"):
            show_performance_metrics()

def check_system_status():
    """Check and display system status"""
    with st.spinner("Checking system status..."):
        status_task = {'type': 'get_system_status'}
        status_result = st.session_state.orchestrator.execute(status_task)
        
        if status_result.get('status') == 'success':
            system_status = status_result.get('system_status', {})
            
            # Display status in organized format
            st.success("System Status Retrieved")
            
            # Orchestrator status
            orch_status = system_status.get('orchestrator', {})
            st.write(f"**Orchestrator**: {orch_status.get('status', 'unknown')}")
            
            # Agent statuses
            for agent_name in ['schema_agent', 'sql_agent', 'analysis_agent']:
                agent_status = system_status.get(agent_name, {})
                status_emoji = "✅" if agent_status.get('status') == 'active' else "❌"
                st.write(f"{status_emoji} **{agent_name.replace('_', ' ').title()}**: {agent_status.get('status', 'unknown')}")
            
            # Database connection
            db_status = system_status.get('database_connection', {})
            db_emoji = "✅" if db_status.get('status') == 'connected' else "❌"
            st.write(f"{db_emoji} **Database**: {db_status.get('status', 'unknown')}")
            
            # System initialization
            init_status = system_status.get('system_initialized', False)
            init_emoji = "✅" if init_status else "⚠️"
            st.write(f"{init_emoji} **System Initialized**: {init_status}")
            
        else:
            st.error(f"Failed to get system status: {status_result.get('message', 'Unknown error')}")

def initialize_system():
    """Initialize the BI system"""
    with st.spinner("Initializing BI system... This may take a few moments."):
        init_task = {
            'type': 'initialize_system',
            'database': 'postgres',  # Use PostgreSQL database
            'schema': 'public'       # Use public schema
        }
        
        init_result = st.session_state.orchestrator.execute(init_task)
        
        if init_result.get('status') == 'success':
            st.session_state.system_initialized = True
            st.success("✅ BI system initialized successfully!")
            
            # Display initialization details
            schema_result = init_result.get('schema_result', {})
            if 'catalog_result' in schema_result:
                catalog_stats = schema_result['catalog_result'].get('statistics', {})
                st.write(f"**Tables discovered**: {catalog_stats.get('table_count', 0)}")
                st.write(f"**Relationships mapped**: {catalog_stats.get('relationship_count', 0)}")
                st.write(f"**Business metrics**: {catalog_stats.get('business_metrics_count', 0)}")
        else:
            st.error(f"❌ Failed to initialize system: {init_result.get('message', 'Unknown error')}")

def refresh_schema():
    """Refresh the schema catalog"""
    with st.spinner("Refreshing schema catalog..."):
        refresh_result = st.session_state.orchestrator.refresh_system()
        
        if refresh_result.get('status') == 'success':
            st.success("✅ Schema catalog refreshed successfully!")
        else:
            st.error(f"❌ Failed to refresh schema: {refresh_result.get('message', 'Unknown error')}")

def update_configuration(auto_optimize: bool, include_analysis: bool, analysis_depth: str, security_mode: str):
    """Update orchestrator configuration"""
    # Update configuration in orchestrator
    st.session_state.orchestrator.auto_optimize = auto_optimize
    st.session_state.orchestrator.include_analysis = include_analysis
    st.session_state.orchestrator.analysis_agent.analysis_depth = analysis_depth
    st.session_state.orchestrator.sql_agent.security_mode = security_mode
    
    st.success("✅ Configuration updated successfully!")

def show_agent_statistics():
    """Display agent statistics"""
    orchestrator = st.session_state.orchestrator
    
    st.write("**Schema Agent Statistics**")
    schema_stats = orchestrator.schema_agent.get_schema_statistics()
    st.json(schema_stats)
    
    st.write("**SQL Agent Statistics**")
    sql_stats = orchestrator.sql_agent.get_generation_statistics()
    st.json(sql_stats)
    
    st.write("**Analysis Agent Statistics**")
    analysis_stats = orchestrator.analysis_agent.get_analysis_statistics()
    st.json(analysis_stats)

def process_agentic_query(question: str):
    """Process query through the agentic orchestrator"""
    try:
        # Execute complete BI workflow
        workflow_task = {
            'type': 'complete_bi_workflow',
            'question': question,
            'user_context': st.session_state.semantic_context,
            'database': 'postgres',  # Use PostgreSQL database
            'schema': 'public'       # Use public schema
        }
        
        workflow_result = st.session_state.orchestrator.execute(workflow_task)
        
        if workflow_result.get('status') == 'success':
            # Extract results
            sql_query = workflow_result.get('sql_query', '')
            results = workflow_result.get('results', [])
            insights = workflow_result.get('insights', '')
            analysis = workflow_result.get('analysis', {})
            workflow_log = workflow_result.get('workflow_log', [])
            
            # Display insights
            if insights:
                st.markdown("### 🧠 AI Insights")
                st.markdown(insights)
            else:
                st.markdown("### ✅ Query Executed Successfully")
                st.write(f"Found {len(results)} records")
            
            # Store results for visualization
            st.session_state.current_results = {
                'data': results,
                'analysis': analysis,
                'insights': insights,
                'sql_query': sql_query,
                'workflow_log': workflow_log
            }
            
            # Add to chat history
            st.session_state.messages.append({
                "role": "assistant",
                "content": insights if insights else f"Query executed successfully. Found {len(results)} records.",
                "sql_query": sql_query,
                "data_results": results,
                "workflow_details": workflow_log
            })
            
            # Add to query history
            st.session_state.query_history.append({
                "question": question,
                "sql": sql_query,
                "timestamp": datetime.now(),
                "success": True,
                "result_count": len(results)
            })
            
        else:
            error_message = workflow_result.get('message', 'Unknown error occurred')
            workflow_log = workflow_result.get('workflow_log', [])
            
            st.error(f"❌ {error_message}")
            
            # Show workflow log for debugging
            if workflow_log:
                with st.expander("Workflow Debug Information"):
                    for step_name, step_result in workflow_log:
                        st.write(f"**{step_name}**: {step_result.get('status', 'unknown')}")
                        if step_result.get('status') == 'error':
                            st.write(f"Error: {step_result.get('message', 'No details')}")
            
            # Add error to query history
            st.session_state.query_history.append({
                "question": question,
                "sql": workflow_result.get('sql_query', ''),
                "timestamp": datetime.now(),
                "success": False,
                "error": error_message
            })
    
    except Exception as e:
        st.error(f"❌ Unexpected error: {str(e)}")

def display_workflow_details(workflow_log: List):
    """Display workflow execution details"""
    for step_name, step_result in workflow_log:
        status = step_result.get('status', 'unknown')
        status_emoji = "✅" if status == 'success' else "❌" if status == 'error' else "⚠️"
        
        st.write(f"{status_emoji} **{step_name.replace('_', ' ').title()}**: {status}")
        
        if step_result.get('message'):
            st.write(f"   └ {step_result['message']}")

def display_results(results: List[Dict[str, Any]]):
    """Display query results in a formatted table"""
    if not results:
        st.info("No data returned from query")
        return
    
    # Convert to DataFrame for better display
    df = pd.DataFrame(results)
    
    st.subheader("📋 Query Results")
    st.dataframe(df, use_container_width=True)
    
    # Show summary statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        with st.expander("📊 Summary Statistics"):
            st.dataframe(df[numeric_cols].describe())

def display_analysis_panel(current_results: Dict[str, Any]):
    """Display comprehensive analysis panel"""
    analysis = current_results.get('analysis', {})
    data = current_results.get('data', [])
    
    # Analysis Summary
    if analysis:
        st.subheader("🔍 Analysis Summary")
        
        # Executive Summary
        if 'executive_summary' in analysis:
            exec_summary = analysis['executive_summary']
            st.metric("Analysis Completeness", f"{exec_summary.get('analysis_completeness', 0):.0f}%")
            st.metric("Data Quality", exec_summary.get('data_quality', 'unknown').title())
            
            # Key Findings
            if exec_summary.get('key_findings'):
                st.write("**Key Findings:**")
                for finding in exec_summary['key_findings'][:3]:
                    st.write(f"• {finding}")
            
            # Recommendations
            if exec_summary.get('recommendations'):
                st.write("**Recommendations:**")
                for rec in exec_summary['recommendations'][:3]:
                    st.write(f"• {rec}")
    
    # Visualizations
    if data:
        st.subheader("📈 Visualizations")
        create_automatic_visualizations(data)

def create_automatic_visualizations(results: List[Dict[str, Any]]):
    """Create automatic visualizations based on query results"""
    if not results:
        return
    
    df = pd.DataFrame(results)
    
    if df.empty:
        st.info("No data to visualize")
        return
    
    # Auto-detect chart types based on data
    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
    categorical_cols = df.select_dtypes(include=['object', 'string']).columns.tolist()
    date_cols = df.select_dtypes(include=['datetime']).columns.tolist()
    
    if len(df) == 1:
        # Single row - show as metrics
        st.subheader("Key Metrics")
        cols = st.columns(min(len(numeric_cols), 4))
        for i, col in enumerate(numeric_cols[:4]):
            with cols[i]:
                value = df[col].iloc[0]
                st.metric(col, f"{value:,.2f}" if isinstance(value, float) else f"{value:,}")
    
    elif len(numeric_cols) >= 1 and len(categorical_cols) >= 1:
        # Bar chart
        cat_col = categorical_cols[0]
        num_col = numeric_cols[0]
        
        if len(df) <= 20:  # Only for reasonable number of categories
            fig = px.bar(df, x=cat_col, y=num_col, 
                        title=f"{num_col} by {cat_col}")
            st.plotly_chart(fig, use_container_width=True)
    
    elif len(numeric_cols) >= 2:
        # Scatter plot for two numeric columns
        fig = px.scatter(df, x=numeric_cols[0], y=numeric_cols[1],
                        title=f"{numeric_cols[1]} vs {numeric_cols[0]}")
        st.plotly_chart(fig, use_container_width=True)
    
    # Time series if date column exists
    if date_cols and numeric_cols:
        date_col = date_cols[0]
        num_col = numeric_cols[0]
        
        # Convert to datetime if not already
        if not pd.api.types.is_datetime64_any_dtype(df[date_col]):
            df[date_col] = pd.to_datetime(df[date_col])
        
        df_sorted = df.sort_values(date_col)
        fig = px.line(df_sorted, x=date_col, y=num_col,
                     title=f"{num_col} over Time")
        st.plotly_chart(fig, use_container_width=True)

def display_query_history():
    """Display recent query history"""
    if st.session_state.query_history:
        for i, query_info in enumerate(reversed(st.session_state.query_history[-5:])):
            with st.expander(f"Query {len(st.session_state.query_history) - i}"):
                st.write(f"**Question:** {query_info['question']}")
                if query_info.get('sql'):
                    st.code(query_info['sql'], language='sql')
                
                if query_info.get('success'):
                    st.success(f"✅ Success ({query_info.get('result_count', 0)} records)")
                else:
                    st.error(f"❌ Failed: {query_info.get('error', 'Unknown error')}")
    else:
        st.info("No queries executed yet")

def explore_schema():
    """Explore the database schema"""
    orchestrator = st.session_state.orchestrator
    schema_stats = orchestrator.schema_agent.get_schema_statistics()
    
    st.subheader("🗄️ Database Schema")
    st.json(schema_stats)

def show_performance_metrics():
    """Show system performance metrics"""
    st.subheader("⚡ Performance Metrics")
    
    # Mock performance data - in real implementation, collect from agents
    metrics = {
        "Average Query Time": "2.3s",
        "Success Rate": "94%",
        "Queries Today": "47",
        "Cache Hit Rate": "78%"
    }
    
    cols = st.columns(len(metrics))
    for i, (metric, value) in enumerate(metrics.items()):
        with cols[i]:
            st.metric(metric, value)

if __name__ == "__main__":
    main()