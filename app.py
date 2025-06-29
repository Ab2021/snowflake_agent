import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, date
import json
from typing import Dict, List, Optional, Any

from llm_client import LLMClient
from database import SnowflakeConnector
from workflow import BIWorkflow
from utils import format_query_result, generate_chart_suggestions

# Page configuration
st.set_page_config(
    page_title="GenBI - Natural Language Business Intelligence",
    page_icon="📊",
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
if 'workflow' not in st.session_state:
    # Initialize workflow with current date
    current_date = date.today().strftime("%Y-%m-%d")
    st.session_state.workflow = BIWorkflow(current_date=current_date)

def main():
    st.title("📊 GenBI - Natural Language Business Intelligence")
    st.markdown("Ask questions about your data in plain English, and I'll generate SQL queries and provide insights.")
    
    # Sidebar for configuration
    with st.sidebar:
        st.header("⚙️ Configuration")
        
        # Semantic Context Configuration
        st.subheader("Database Schema Context")
        semantic_context = st.text_area(
            "Enter your semantic context (table schemas, relationships, etc.)",
            value=st.session_state.semantic_context,
            height=200,
            help="Provide table names, column definitions, relationships, and any business rules that will help generate accurate SQL queries."
        )
        
        if st.button("Update Context"):
            st.session_state.semantic_context = semantic_context
            st.success("Semantic context updated!")
        
        # Connection Status
        st.subheader("Connection Status")
        if st.button("Test Snowflake Connection"):
            try:
                connector = SnowflakeConnector()
                if connector.test_connection():
                    st.success("✅ Snowflake connection successful")
                else:
                    st.error("❌ Snowflake connection failed")
            except Exception as e:
                st.error(f"❌ Connection error: {str(e)}")
        
        # Query History
        st.subheader("Query History")
        if st.session_state.query_history:
            for i, query_info in enumerate(reversed(st.session_state.query_history[-10:])):
                with st.expander(f"Query {len(st.session_state.query_history) - i}"):
                    st.write(f"**Question:** {query_info['question']}")
                    st.code(query_info['sql'], language='sql')
                    if query_info.get('success'):
                        st.success("✅ Executed successfully")
                    else:
                        st.error("❌ Failed to execute")
        else:
            st.info("No queries executed yet")
    
    # Main chat interface
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.subheader("💬 Ask Your Question")
        
        # Display chat messages
        for message in st.session_state.messages:
            with st.chat_message(message["role"]):
                st.markdown(message["content"])
                
                # Display SQL query if available
                if message.get("sql_query"):
                    st.code(message["sql_query"], language='sql')
                
                # Display data results if available
                if message.get("data_results"):
                    display_results(message["data_results"])
        
        # Chat input
        if prompt := st.chat_input("Ask a question about your data..."):
            if not st.session_state.semantic_context.strip():
                st.error("Please configure your semantic context in the sidebar first.")
                return
            
            # Add user message to chat
            st.session_state.messages.append({"role": "user", "content": prompt})
            with st.chat_message("user"):
                st.markdown(prompt)
            
            # Process the question
            with st.chat_message("assistant"):
                with st.spinner("Generating SQL query..."):
                    process_question(prompt)
    
    with col2:
        st.subheader("📈 Data Visualization")
        if st.session_state.current_results is not None:
            create_visualizations(st.session_state.current_results)
        else:
            st.info("Execute a query to see visualizations here")

def process_question(question: str):
    """Process a user question through the BI workflow"""
    try:
        workflow = st.session_state.workflow
        
        # Step 1: Generate SQL
        sql_query = workflow.generate_sql(
            question=question,
            semantic_context=st.session_state.semantic_context
        )
        
        if not sql_query:
            st.error("Failed to generate SQL query")
            return
        
        st.code(sql_query, language='sql')
        
        # Step 2: Execute SQL
        connector = SnowflakeConnector()
        
        max_retries = 2
        current_sql = sql_query
        
        for attempt in range(max_retries + 1):
            try:
                with st.spinner(f"Executing query (attempt {attempt + 1})..."):
                    results = connector.execute_query(current_sql)
                
                if results is not None:
                    # Step 3: Analyze results
                    with st.spinner("Analyzing results..."):
                        analysis = workflow.analyze_data(
                            question=question,
                            query_result=results
                        )
                    
                    # Display analysis
                    st.markdown(analysis)
                    
                    # Store results for visualization
                    st.session_state.current_results = results
                    
                    # Add to chat history
                    st.session_state.messages.append({
                        "role": "assistant",
                        "content": analysis,
                        "sql_query": current_sql,
                        "data_results": results
                    })
                    
                    # Add to query history
                    st.session_state.query_history.append({
                        "question": question,
                        "sql": current_sql,
                        "timestamp": datetime.now(),
                        "success": True
                    })
                    
                    break
                else:
                    raise Exception("Query returned no results")
                    
            except Exception as e:
                error_msg = str(e)
                
                if attempt < max_retries:
                    # Try to fix the SQL
                    with st.spinner("Attempting to fix SQL query..."):
                        fixed_sql = workflow.fix_sql(
                            question=question,
                            semantic_context=st.session_state.semantic_context,
                            failed_sql_query=current_sql,
                            database_error=error_msg
                        )
                    
                    if fixed_sql and fixed_sql != current_sql:
                        current_sql = fixed_sql
                        st.warning(f"Query failed, attempting fix (attempt {attempt + 2})...")
                        st.code(current_sql, language='sql')
                        continue
                    else:
                        st.error(f"Unable to fix SQL query: {error_msg}")
                        break
                else:
                    st.error(f"Query failed after {max_retries + 1} attempts: {error_msg}")
                    
                    # Add failed query to history
                    st.session_state.query_history.append({
                        "question": question,
                        "sql": current_sql,
                        "timestamp": datetime.now(),
                        "success": False,
                        "error": error_msg
                    })
                    break
    
    except Exception as e:
        st.error(f"An unexpected error occurred: {str(e)}")

def display_results(results: List[Dict[str, Any]]):
    """Display query results in a formatted table"""
    if not results:
        st.info("No data returned from query")
        return
    
    # Convert to DataFrame for better display
    df = pd.DataFrame(results)
    
    # Display as dataframe
    st.dataframe(df, use_container_width=True)
    
    # Show summary statistics for numeric columns
    numeric_cols = df.select_dtypes(include=['number']).columns
    if len(numeric_cols) > 0:
        with st.expander("📊 Summary Statistics"):
            st.dataframe(df[numeric_cols].describe())

def create_visualizations(results: List[Dict[str, Any]]):
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

if __name__ == "__main__":
    main()
