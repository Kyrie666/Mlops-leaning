from langchain_community.utilities.sql_database import SQLDatabase
from langchain_core.messages import HumanMessage
from langgraph.prebuilt import create_react_agent
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

system_message = SystemMessage(content=SQL_PREFIX)


def get_sql_agent(db: SQLDatabase, llm):

    toolkit = SQLDatabaseToolkit(db=db, llm=llm)
    tools = toolkit.get_tools()
    agent_executor = create_react_agent(llm, tools, state_modifier=system_message)
    return agent_executor


def get_sql_agent_tool(db: SQLDatabase, llm):
    sql_agent = get_sql_agent(db, llm)
    # args_schema (Optional[Type[BaseModel]]) – The schema for the tool. Defaults to None.
    # name (Optional[str]) – The name of the tool. Defaults to None.
    # description (Optional[str]) – The description of the tool. Defaults to None.
    # arg_types (Optional[Dict[str, Type]]) – A dictionary of argument names to types. Defaults to None.

    description = f"""  
        "sql_agent_tool(messages: list) -> str:\n"
        " - Input is a string question  about database \n"
        " - output is an answer about some data in the database .\n"
        " - the tool is to solve problems about database. the database info:{db.table_info} \n"
        " - eg:how many data in the table?.  \n"
    """
    sql_agent_tool = sql_agent.as_tool(
        arg_types={"messages": list}, name="sql_agent_tool", description=description
    )

    return sql_agent_tool
