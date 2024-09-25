from re import L
from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import (
    InfoSQLDatabaseTool,
    QuerySQLDataBaseTool,
    ListSQLDatabaseTool,
    QuerySQLCheckerTool,
)

from langchain.chains import create_sql_query_chain

INFO_SQL_PREFIX = (
    "info_sql(query:str) -> str:\n"
    " -  Get the schema and sample rows for the specified SQL tables:{}.\n"
    " - `query` must be a correct sql query.\n"
)


LIST_SQL_PREFIX = (
    "ListSQLDatabaseTool(input:str) -> list:\n"
    " - Input is an empty string:'' \n"
    " - output is a comma-separated list of tables in the database .\n"
    " - the tool is to get database's table names.\n"
)


def get_info_sql_tool(db):
    table_names = db.get_usable_table_names()
    INFO_SQL_PREFIX = INFO_SQL_PREFIX.format(table_names)
    return InfoSQLDatabaseTool(db=db, description=INFO_SQL_PREFIX)


def get_list_sql_tool(db):
    return ListSQLDatabaseTool(
        db=db, description=LIST_SQL_PREFIX, name="ListSQLDatabaseTool"
    )


from langchain_community.agent_toolkits import SQLDatabaseToolkit
from langchain_core.messages import SystemMessage

SQL_PREFIX = """You are an agent designed to interact with a SQL database.
Given an input question, create a syntactically correct SQLite query to run, then look at the results of the query and return the answer.
Unless the user specifies a specific number of examples they wish to obtain, always limit your query to at most 5 results.
You can order the results by a relevant column to return the most interesting examples in the database.
Never query for all the columns from a specific table, only ask for the relevant columns given the question.
You have access to tools for interacting with the database.
Only use the below tools. Only use the information returned by the below tools to construct your final answer.
You MUST double check your query before executing it. If you get an error while executing a query, rewrite the query and try again.

DO NOT make any DML statements (INSERT, UPDATE, DELETE, DROP etc.) to the database.

To start you should ALWAYS look at the tables in the database to see what you can query.
Do NOT skip this step.
Then you should query the schema of the most relevant tables."""
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent

system_message = SystemMessage(content=SQL_PREFIX)


def get_sql_agent(db, llm):

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)

    tools = toolkit.get_tools()

    agent_executor = create_react_agent(llm, tools, state_modifier=system_message)
    return agent_executor
