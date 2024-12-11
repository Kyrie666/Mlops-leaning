from langchain_community.utilities import SQLDatabase
from langchain_community.tools.sql_database.tool import InfoSQLDatabaseTool
import os


def get_db_schema(db_uri):
    try:
        db = SQLDatabase.from_uri(db_uri)
        tool = InfoSQLDatabaseTool(db=db)
        env_info = []
        for table in db.get_usable_table_names():
            env_info.append({"table_name": table, "table_info": tool.invoke(table)})

            return db, env_info
    except Exception as e:
        return None


db, env_info = get_db_schema(os.environ.get("DB_URI") or "sqlite:///chinook.db")
