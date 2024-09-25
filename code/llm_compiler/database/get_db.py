from langchain_community.utilities import SQLDatabase
from sqlalchemy import create_engine
import pandas as pd


class Database:
    def __init__(self, db_path):
        self.db = SQLDatabase.from_uri(db_path)

    def get_table_names(self):
        return self.db.get_usable_table_names()

    def get_table_schema(self, table_name):
        return self.db.get_table_info(table_name)

    def run_query(self, query):
        return self.db.run(query)


# example usage
# db = SQLDatabase.from_uri("sqlite:///../SQL_files/Chinook.db")
# print(db.dialect)
# print(db.get_usable_table_names())
# db.run("SELECT * FROM Artist LIMIT 10;")


class CSVDatabase(Database):
    def __init__(self, **kwargs):
        df = pd.read_csv(kwargs["csv_path"])
        db_path = f"sqlite:///../SQL_files/{kwargs['table_name']}.db"
        engine = create_engine(db_path)
        df.to_sql(kwargs["table_name"], engine, index=False)
        super().__init__(db_path)
