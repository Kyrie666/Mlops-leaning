import pymssql
import pandas as pd
from ..utils.log import logger
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.engine import URL

from .config import settings


# # 使用create_engine函数创建SQL Server引擎
# # 请将<YourServer>替换为您的SQL Server服务器名称
# # 请将<YourDatabase>替换为您要连接的数据库名称
# # 请将<YourUsername>和<YourPassword>替换为您的SQL Server凭据
# engine = create_engine('mssql+pyodbc://<YourUsername>:<YourPassword>@<YourServer>/<YourDatabase>?driver=ODBC+Driver+17+for+SQL+Server')

# # 连接到SQL Server数据库
# with engine.connect() as connection:
#     # 执行SQL查询或其他操作
#     result = connection.execute('SELECT * FROM your_table')
#     for row in result:
#         print(row)
# 请确保在Docker镜像中安装了SQLAlchemy和pyodbc库，
# 以便使用create_engine函数连接到SQL Server数据库。这样，您就可以在Docker镜像中创建SQL Server引擎并执行数据库操作了。


def truncate_table(table_name: str):
    """
    Truncates a specified table in the database.

    Args:
        table_name (str): The name of the table to be truncated.

    Returns:
        None
    """
    conn = get_conn(
        settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
    )
    logger.info("database connect success!")
    try:
        sql = f"TRUNCATE TABLE dbo.{table_name}"
        cursor = conn.cursor()
        cursor.execute(sql)
        conn.commit()
        conn.close()
    except Exception as err:
        logger.error(err)
        if conn:
            conn.close()


# 创建一个函数来动态创建dtype字典
def sqlcol(dfparam: pd.DataFrame) -> dict:
    """
    Generate a dictionary of SQLAlchemy data types based on the input DataFrame's column data types.

    :param dfparam: The input DataFrame
    :return: A dictionary mapping column names to their corresponding SQLAlchemy data types
    """
    dtypedict = {}
    for i, j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})
        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})
        if "str" in str(j):
            dtypedict.update({i: sqlalchemy.types.NVARCHAR(length=255)})
    return dtypedict


def get_engine(
    host: str, user: str, password: str, database: str
) -> sqlalchemy.engine.Engine:
    """
    Create and return a SQLAlchemy engine for the given host, user, password, and database.
    """
    url = URL.create(
        "mssql+pyodbc",
        username=user,
        password=password,
        host=host,
        database=database,
        query={
            "driver": "ODBC Driver 17 for SQL Server",  # k8s的驱动版本不一样
            "charset": "utf8",
            "echo": "True",
            "Encrypt": "YES",
            "TrustServerCertificate": "YES",
            # "SSLProtocol": "TLSv1.3",  # 指定支持的SSL协议版本
        },
    )
    return create_engine(url)


def check_prediction_if_exists(conn, today: str) -> pd.DataFrame:
    """
    Check if prediction data exists for the given date in the HR database.

    Args:
        conn: The database connection object.
        today: The date for which to check the prediction data.

    Returns:
        pd.DataFrame: The prediction data for the given date, or None if no data exists.
    """
    try:
        sql = f"select * from hr.dbo.prediction_data where start_date = '{today}'"
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        if len(data) == 0:
            return None
    except Exception as err:
        logger.error(err)
        return None

    return df


def add_prediction(data):
    """
    Function to add prediction data to the database.

    Args:
        data: The prediction data to be added to the database.

    Returns:
        None
    """
    try:
        engine = get_engine(
            settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
        )
        data.to_sql(
            name="prediction_data",
            con=engine,
            if_exists="append",
            index=False,
            dtype=sqlcol(data),
        )
        engine.dispose()
        return True
    except Exception as err:
        logger.error(err)
        logger.error("Failed to add prediction data")
        return False


def get_predition(conn, today):
    """
    Function to retrieve prediction data for a given date from the database.

    Args:
        conn: Connection object to the database.
        today: Date for which the prediction data is to be retrieved.

    Returns:
        List of tuples containing the prediction data for the given date, or None if no data is found.
    """
    try:
        sql = f"select * from hr.dbo.prediction_data where start_date = '{today}'"
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        if len(data) == 0:
            return None
    except Exception as err:
        logger.error(err)
        return None
    return data


def add_new_time_data(data):
    """
    A function to add new time data to the database.

    Args:
        data: the data to be added to the database.

    Returns:
        None
    """
    try:
        engine = get_engine(
            settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
        )
        logger.info(engine)
        # 使用动态创建的dtype字典将数据框写入数据库
        outputdict = sqlcol(data)
        data.to_sql(
            name="time_data",
            con=engine,
            if_exists="append",
            index=True,
            index_label="date",
            dtype=outputdict,
        )
        engine.dispose()
        return True
    except Exception as err:
        logger.error(err)
        return False


def get_last_date(conn):
    """
    Get the last date from the 'time_data' table using the provided database connection.

    Args:
        conn: The database connection object.

    Returns:
        datetime.date: The last date from the 'time_data' table, or None if the date is None or an error occurs.
    """
    try:
        sql = "select max(date) as last_date from dbo.time_data"
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        logger.info(f"last_date:{data}")
        if data[0]["last_date"] is None:
            return None
    except Exception as err:
        logger.error(err)
        return None
    return data[0]["last_date"].date()


def get_time_data(conn):
    """
    Retrieves time data from a database using the provided database connection.

    Args:
    conn: A database connection object.

    Returns:
    pd.DataFrame: A pandas DataFrame containing the retrieved time data, or None if an error occurs.
    """
    try:
        sql = "select * from hr.dbo.time_data order by date asc"
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        if len(data) == 0:
            return None

        df = pd.DataFrame(data)
    except Exception as err:
        logger.error(err)
        return None
    return df


def get_temp_data(conn, start_time, end_time):
    """
    Retrieves temporary data from a SQL Server database.

    Parameters:
        conn (pymssql.Connection): A connection object representing the connection to the database.
        start_time (str): The start time for the query in the format 'YYYY-MM-DD'.
        end_time (str): The end time for the query in the format 'YYYY-MM-DD'.
    Returns:
        df (pandas.DataFrame): A DataFrame containing the retrieved temporary data.
    """
    try:
        sql = f"""SELECT per_outrq,per_lzrq
                FROM [hr].[dbo].[t_lzpersonal_bs]
                WHERE zhibie='员工'
                AND per_outrq between '{start_time}' and '{end_time}'
                AND per_outfs='离厂' """
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        df.columns = ["离职日期", "入职日期"]
    except Exception as err:
        logger.error(err)
        return None
    return df


def get_depart_data(conn, start_time, end_time, department="bs"):
    """
    Retrieves department data from a SQL Server database.

    Parameters:
        conn (pymssql.Connection): A connection object representing the connection to the database.
        start_time (str): The start time for the query in the format 'YYYY-MM-DD'.
        end_time (str): The end time for the query in the format 'YYYY-MM-DD'.
    Returns:
        df (pandas.DataFrame): A DataFrame containing the retrieved department data.
    """
    try:
        sql = f""" SELECT per_outrq,per_lzrq   
                FROM [hr].[dbo].[t_lzpersonal_{department}]
                WHERE zhibie='员工'
                AND per_outrq between '{start_time}' and '{end_time}'
                AND per_outfs IN ('辞职','辞职1''急辞','自离','自离1') """
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        df.columns = ["离职日期", "入职日期"]
        df["入职日期"] = pd.to_datetime(df["入职日期"])
        df["离职日期"] = pd.to_datetime(df["离职日期"])
    except Exception as err:
        logger.error(err)
        return None
    return df


def get_employee_data(conn, department="bs"):
    """
    Retrieves employee data from the database.

    Parameters:
    - conn: The database connection object.

    Returns:
    - df: A DataFrame containing the employee data, including the "入职日期" (entry date) column.
    """
    try:
        sql = f"""SELECT per_rzrq 
                FROM [hr].[dbo].[t_personal_{department}]
                where zhibie='员工'
                AND per_jxfs='日薪'
                """
        # print(sql)
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        df.columns = ["入职日期"]
        df["入职日期"] = pd.to_datetime(df["入职日期"])
        df["离职日期"] = pd.to_datetime("2045-12-31")
    except Exception as err:
        logger.error(err)
        return None
    return df


def get_temp_employee_data(conn):
    """
    Retrieves employee data from the database.

    Parameters:
    - conn: The database connection object.

    Returns:
    - df: A DataFrame containing the employee data, including the "入职日期" (entry date) column.
    """
    try:
        sql = f"""SELECT per_rzrq 
                FROM [hr].[dbo].[t_personal_bs]
                where zhibie='员工'
                AND per_jxfs='派遣'
                """
        cursor = conn.cursor()
        cursor.execute(sql)
        data = cursor.fetchall()
        df = pd.DataFrame(data)
        df.columns = ["入职日期"]
        df["入职日期"] = pd.to_datetime(df["入职日期"])
        df["离职日期"] = pd.to_datetime("2045-12-31")
    except Exception as err:
        logger.error(err)
        return None
    return df


def get_conn(host, user, password, database):
    """
    Creates a connection to a database using the provided host, username, password, and database name.

    Parameters:
        host (str): The host name or IP address of the database server.
        user (str): The username used to authenticate with the database server.
        password (str): The password used to authenticate with the database server.
        database (str): The name of the database to connect to.

    Returns:
        conn (pymssql.Connection): A connection object representing the connection to the database.
    """
    try:
        conn = pymssql.connect(
            server=host,
            user=user,
            password=password,
            database=database,
            as_dict=True,
            tds_version="7.0",  # TDS 协议的版本号，用于指定与数据库服务器通信所使用的 TDS 协议版本。
        )
    # 捕获数据库连接错误
    except Exception as err:
        logger.error(err)
        return None
    return conn
