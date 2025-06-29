import os
import sys
import json
from openai import OpenAI
from typing import Optional

class LLMClient:
    """Client for interacting with OpenAI GPT LLM"""
    
    def __init__(self):
        # Initialize the client
        openai_key: str = os.environ.get('OPENAI_API_KEY', 'your-openai-api-key-here')
        if openai_key == 'your-openai-api-key-here':
            print("Warning: Using default API key. Please set OPENAI_API_KEY environment variable.")
        
        self.client = OpenAI(api_key=openai_key)
        
        # the newest OpenAI model is "gpt-4o" which was released May 13, 2024.
        # do not change this unless explicitly requested by the user
        self.DEFAULT_MODEL_STR = "gpt-4o"
        
        # System message for the BI agent
        self.SYSTEM_MESSAGE = """You are an expert-level data analyst and a master of Snowflake SQL, encapsulated within an automated BI agent.

**Your Primary Objective:** To accurately answer a user's natural language question by generating a valid Snowflake SQL query, interpreting the results, and providing a clear, insightful summary.

**Your Core Principles:**
1. **Context is King:** You MUST base your SQL queries exclusively on the provided **Semantic Context**. Do not invent table names, column names, or metrics that are not defined in the context.
2. **Precision and Accuracy:** Your generated SQL must be syntactically correct for Snowflake. Your analysis of the data must be factual and directly supported by the query results.
3. **Clarity:** Your final answer to the user should be in plain, easy-to-understand language. Avoid technical jargon where possible.
4. **Security:** You must never generate SQL that modifies the database (no `INSERT`, `UPDATE`, `DELETE`, `DROP`, etc.). Your role is strictly read-only (`SELECT`).

**Your Operational Flow:**
- First, you will be given a user's question and a relevant **Semantic Context**. Your task is to generate a SQL query.
- Next, if the query is successful, you will be given the original question and the data results. Your task is to analyze them and form a response.
- If the query fails, you will be given the error and asked to debug and fix your original SQL query."""

    def generate_sql_query(self, question: str, semantic_context: str, current_date: str) -> Optional[str]:
        """Generate SQL query from natural language question"""
        user_message = f"""Given the context and question below, generate a single, valid Snowflake SQL query to answer the question.

**Follow these strict instructions:**
- Output ONLY the raw SQL query and nothing else. Do not add explanations, comments, or any surrounding text.
- Use only the tables, columns, metrics, and relationships defined in the Semantic Context.
- If the question involves a time period (e.g., "last quarter", "this year"), use appropriate date functions in Snowflake. Assume the current date is {current_date}.
- Ensure all table and column names in the query are correctly quoted (e.g., "TableName"."ColumnName").

--- SEMANTIC CONTEXT ---
{semantic_context}

--- QUESTION ---
{question}

--- SNOWFLAKE SQL QUERY ---"""

        try:
            response = self.client.messages.create(
                model=self.DEFAULT_MODEL_STR,
                max_tokens=1000,
                system=self.SYSTEM_MESSAGE,
                messages=[
                    {"role": "user", "content": user_message}
                ]
            )
            
            sql_query = response.content[0].text.strip()
            
            # Clean up the response - remove any markdown formatting
            if sql_query.startswith('```sql'):
                sql_query = sql_query[6:]
            if sql_query.endswith('```'):
                sql_query = sql_query[:-3]
            
            return sql_query.strip()
            
        except Exception as e:
            print(f"Error generating SQL query: {e}")
            return None

    def analyze_query_results(self, question: str, query_result: list) -> Optional[str]:
        """Analyze query results and provide insights"""
        user_message = f"""You previously generated a SQL query to answer a user's question. The query was successful.

Now, analyze the provided data results and formulate a final, human-readable answer.

**Follow these strict instructions:**
- Begin by directly answering the user's original question.
- Summarize the key insights and trends found in the data. Do not just list the raw data.
- If the data contains numerical values, present them clearly.
- Your entire response should be a concise, well-written paragraph or a short list of bullet points.

--- ORIGINAL QUESTION ---
{question}

--- DATA RESULTS (in JSON format) ---
{query_result}

--- ANALYSIS & FINAL ANSWER ---"""

        try:
            response = self.client.chat.completions.create(
                model=self.DEFAULT_MODEL_STR,
                max_tokens=1500,
                messages=[
                    {"role": "system", "content": self.SYSTEM_MESSAGE},
                    {"role": "user", "content": user_message}
                ]
            )
            
            return response.choices[0].message.content.strip()
            
        except Exception as e:
            print(f"Error analyzing query results: {e}")
            return None

    def fix_sql_query(self, question: str, semantic_context: str, failed_sql_query: str, database_error: str) -> Optional[str]:
        """Fix a failed SQL query based on error message"""
        user_message = f"""The Snowflake SQL query you previously generated failed to execute. Analyze your failed query and the provided database error message to understand the problem.

Your task is to generate a corrected Snowflake SQL query.

**Follow these strict instructions:**
- Carefully review the error message. It often contains the key to the solution (e.g., "invalid identifier", "syntax error").
- Compare the failed query with the provided Semantic Context to ensure all table and column names were correct.
- Output ONLY the new, corrected SQL query and nothing else.

--- SEMANTIC CONTEXT ---
{semantic_context}

--- ORIGINAL QUESTION ---
{question}

--- FAILED SQL QUERY ---
{failed_sql_query}

--- DATABASE ERROR MESSAGE ---
{database_error}

--- CORRECTED SNOWFLAKE SQL QUERY ---"""

        try:
            response = self.client.messages.create(
                model=self.DEFAULT_MODEL_STR,
                max_tokens=1000,
                system=self.SYSTEM_MESSAGE,
                messages=[
                    {"role": "user", "content": user_message}
                ]
            )
            
            sql_query = response.content[0].text.strip()
            
            # Clean up the response - remove any markdown formatting
            if sql_query.startswith('```sql'):
                sql_query = sql_query[6:]
            if sql_query.endswith('```'):
                sql_query = sql_query[:-3]
            
            return sql_query.strip()
            
        except Exception as e:
            print(f"Error fixing SQL query: {e}")
            return None
