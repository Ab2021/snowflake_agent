# GenBI - Natural Language Business Intelligence

## Overview

GenBI is a Streamlit-based business intelligence application that allows users to query Snowflake databases using natural language. The system leverages Anthropic's Claude LLM to translate natural language questions into SQL queries, execute them against a Snowflake data warehouse, and provide intelligent analysis of the results.

## System Architecture

The application follows a **multi-agent enterprise architecture** with specialized components:

### Agent-Based Framework
1. **Frontend Layer**: Streamlit web interface with agentic orchestration
2. **Agent Layer**: Specialized agents for different BI tasks
3. **Tool Layer**: Reusable tools for agent capabilities
4. **Schema Layer**: Semantic catalog and schema management
5. **Data Layer**: Snowflake database connector with advanced querying

### Core Agents
- **Schema Agent**: Discovers and manages database schemas with semantic understanding
- **SQL Agent**: Advanced natural language to SQL translation with optimization
- **Analysis Agent**: Intelligent data interpretation and insight generation
- **Orchestrator Agent**: Coordinates multi-agent workflows and task delegation

### Tool Ecosystem
Each agent leverages specialized tools:
- **Schema Tools**: Discovery, validation, relationship mapping, semantic cataloging
- **SQL Tools**: Generation, optimization, security validation, performance tuning
- **Analysis Tools**: Statistical analysis, trend detection, insight generation

## Key Components

### Frontend (`agentic_app.py`)
- **Technology**: Streamlit with Plotly for visualizations and multi-agent orchestration
- **Purpose**: Provides advanced web interface for agentic BI operations
- **Features**: Multi-agent controls, system monitoring, workflow visualization, comprehensive analysis
- **Port**: 5000 (configured for Replit deployment)

### LLM Integration (`llm_client.py`)
- **Technology**: OpenAI GPT-4o API
- **Purpose**: Natural language processing and SQL generation
- **Security**: Read-only operations only (SELECT queries)
- **Configuration**: Environment variable-based API key management

### Database Layer (`postgres_connector.py`, `database.py`)
- **Technology**: PostgreSQL (primary) with Snowflake fallback
- **Purpose**: Database connection and query execution with sample e-commerce data
- **Configuration**: Environment variables for connection parameters (auto-configured)
- **Features**: Connection testing, sample data creation, multi-database support

### Workflow Management (`workflow.py`)
- **Purpose**: Orchestrates the three-node BI workflow
- **Components**: SQL generation, data analysis, error correction
- **Design**: Stateful workflow with current date context

### Utilities (`utils.py`)
- **Purpose**: Data formatting and visualization suggestions
- **Features**: Query result formatting, chart type recommendations
- **Data Handling**: Pandas DataFrame integration for result processing

## Data Flow

1. **User Input**: Natural language question entered via Streamlit interface
2. **Context Processing**: Semantic context (database schema) combined with user question
3. **SQL Generation**: LLM generates Snowflake SQL query based on context
4. **Query Execution**: SQL executed against Snowflake database
5. **Result Analysis**: LLM analyzes results and generates insights
6. **Visualization**: Results displayed with suggested charts in Streamlit interface
7. **Error Handling**: Failed queries trigger SQL correction workflow

## External Dependencies

### Core Dependencies
- **Streamlit**: Web framework for UI (v1.46.0+)
- **OpenAI**: LLM API client (v1.0.0+)
- **Snowflake Connector**: Database connectivity (v3.15.0+)
- **Pandas**: Data manipulation (v2.3.0+)
- **Plotly**: Interactive visualizations (v6.1.2+)

### Environment Variables Required
- `OPENAI_API_KEY`: GPT-4o API access
- `SNOWFLAKE_USER`: Database username
- `SNOWFLAKE_PASSWORD`: Database password
- `SNOWFLAKE_ACCOUNT`: Snowflake account identifier
- `SNOWFLAKE_WAREHOUSE`: Compute warehouse
- `SNOWFLAKE_DATABASE`: Target database
- `SNOWFLAKE_SCHEMA`: Default schema
- `SNOWFLAKE_ROLE`: User role

## Deployment Strategy

- **Platform**: Replit with autoscale deployment target
- **Runtime**: Python 3.11 with Nix package management
- **Port**: 5000 (configured for external access)
- **Startup**: Streamlit server with headless configuration
- **Scaling**: Automatic scaling based on demand

The deployment uses Replit's workflow system with parallel task execution, ensuring reliable startup and port binding.

## Changelog

```
Changelog:
- June 22, 2025. Initial setup
- June 22, 2025. Switched from Anthropic Claude to OpenAI GPT-4o
- June 22, 2025. Implemented agentic framework with multi-agent architecture
- June 22, 2025. Added comprehensive schema catalog and semantic layer
- June 22, 2025. Created tool ecosystem for specialized agent capabilities
- June 22, 2025. Completed full system verification - all components operational
- June 22, 2025. Added PostgreSQL database with sample e-commerce data for immediate testing
- June 22, 2025. Completed database integration with multi-database support and schema discovery
- June 23, 2025. Implemented cost optimization features including prompt compression, result caching, and performance monitoring
```

## User Preferences

```
Preferred communication style: Simple, everyday language.
```