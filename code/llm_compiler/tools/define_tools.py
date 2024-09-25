from math_tools import get_math_tool
from sql_tools import get_sql_agent_tool
from python_tools import get_python_tool
from langchain_experimental.tools.python.tool import PythonREPLTool

from model import get_llm


def get_tools(llm, db):
    return [
        get_math_tool(llm),
        get_sql_agent_tool(db, llm),
        get_python_tool(llm, PythonREPLTool()),
    ]
