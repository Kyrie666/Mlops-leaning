import pymssql
import pandas as pd
import json
import requests
from ..core.log import logger
import time


def get_machine_ip(conn, equipment_id):
    """
    Get the IP address of a machine based on its equipment ID.

    Parameters:
    - conn: A connection object to the database.
    - equipment_id: The ID of the equipment.

    Returns:
    - The IP address of the machine.
    """
    try:
        sql = f"SELECT ip FROM IAData.dbo.Station where Station_ID= '{equipment_id}'"
        cursor = conn.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
    except Exception as e:
        logger.error(e)
        return None
    if len(result) == 0:
        return None
    return result[0]["ip"]


def get_pieces(conn, machine_ip, start_time, end_time):
    sql = f"SELECT * FROM IAData.dbo.pieces WHERE (CreateTime between '{start_time}' and '{end_time}') and ip = '{machine_ip}'"
    cursor = conn.cursor()
    cursor.execute(sql)
    pieces = cursor.fetchall()
    pieces = pd.DataFrame(pieces)
    logger.info(f"pieces的数量: {pieces.shape[0]}")
    return pieces


def get_defects_from_pieces(conn, data):
    if len(data) == 0:
        return pd.DataFrame()

    sql = f"""SELECT frameid,defectid,qualityid,attribute,FVBucketName,FVFileName
                 FROM IAData.dbo.defect
                 WHERE frameid IN {tuple(data["frameid"].values)} """

    cursor = conn.cursor()
    cursor.execute(sql)
    defects_pieces = cursor.fetchall()

    return defects_pieces


def get_pieces_from_defect(conn, data):
    """
    Retrieves the pieces and frame IDs from the IAData.dbo.pieces table based on the given data.

    Parameters:
        conn (Connection): The database connection object.
        data (DataFrame): The input data containing frame IDs.

    Returns:
        result_pieces (DataFrame): A DataFrame containing the retrieved pieces and frame IDs.
    """
    if len(data) == 0:
        return pd.DataFrame()
    sql = f"""SELECT pieceid,frameid,qualityid
                 FROM IAData.dbo.pieces
                 WHERE frameid IN {tuple(data["frameid"].values)} """

    cursor = conn.cursor()
    cursor.execute(sql)
    result_pieces = cursor.fetchall()
    result_pieces = pd.DataFrame(result_pieces)
    result_pieces.drop_duplicates(subset="pieceid", inplace=True, ignore_index=True)
    return result_pieces


def get_feature(conn, begintime, endtime, factory_ip, attribute):
    """
    Retrieves feature data from the database based on specified criteria.

    Args:
        engine (sqlalchemy.engine.Engine): The database engine to use.
        begintime (str): The start time for the query.
        endtime (str): The end time for the query.
        factory_ip (list): A list of factory IP addresses.

    Returns:
        list: A list of feature data retrieved from the database.
    """
    sql = f"""SELECT
            frameid,
			time,
			defectid,
			attribute,
    	    FVBucketName,
    		FVFileName,
    		Defect_RE_result
    FROM defect_detail_whm
    WHERE (time BETWEEN '{begintime}' AND '{endtime}')  
    and  (ip = '{factory_ip}' ) 
    and (Defect_RE_result IS NOT NULL) 
    AND FVBucketName IS NOT NULL 
	AND FVFileName IS NOT NULL 
    AND attribute = '{attribute}' """
    cursor = conn.cursor()
    cursor.execute(sql)
    result_pieces = cursor.fetchall()

    return result_pieces


def get_feature_id(conn):
    """
    Get the feature ID from the IAData database.

    Parameters:
    - engine: The database engine used to connect to the IAData database.

    Returns:
    - pandas.DataFrame: A DataFrame containing the result pieces of the SQL query.
    """

    sql = "SELECT id,name,display_name FROM IAData.dbo.dim_feature"
    cursor = conn.cursor()
    cursor.execute(sql)
    result_pieces = cursor.fetchall()
    return pd.DataFrame(result_pieces)


def get_classifier_name(conn, classifier_id):
    """
    Retrieves the name of a classifier with the given ID from the IAData.dbo.dim_aoi_classifier table.

    Parameters:
    - conn (Connection): The connection object to the database.
    - classifier_id (str): The ID of the classifier.

    Returns:
    - str: The name of the classifier.

    Example usage:
    >>> conn = create_connection()
    >>> classifier_name = get_classifier_name(conn, "12345")
    """
    try:
        sql = f"""SELECT name
        FROM IAData.dbo.dim_aoi_classifier
        WHERE id = '{classifier_id}' """
        cursor = conn.cursor()
        cursor.execute(sql)
        get_name_data = cursor.fetchall()
    except Exception as e:
        logger.error(e)
        return None
    if len(get_name_data) == 0:
        return None
    return get_name_data[0]["name"]


def get_json(defects):
    """
    Retrieves JSON data from a list of defects.

    Args:
        defects (list): A list of dictionaries representing defects.

    Returns:
        pandas.DataFrame: A DataFrame containing the JSON data from the defects.
    """
    logger.info(f"defects点列表的长度：{len(defects)}")

    json_data_list = []
    start = time.time()

    for d in defects:
        try:
            FVBucketName = d["FVBucketName"]
            FVFileName = d["FVFileName"]

            url = f"http://10.211.89.15:9008/SAS3API/api/Storage/sa/{FVBucketName}/{FVFileName}?filetype=json"
            response = requests.get(url, timeout=10)
            json_data = json.loads(response.text)
            json_data["frameid"] = d["frameid"]
            json_data["defectid"] = d["defectid"]
            json_data["label"] = d["Defect_RE_result"]
            json_data_list.append(json_data)
        except requests.exceptions.Timeout:
            logger.error("请求超时")
        except json.decoder.JSONDecodeError:
            logger.error("JSON解码错误")
            logger.info(f"FVBucketName, FVFileName: {FVBucketName}, {FVFileName}")
            logger.info(response.text)
        except Exception as e:
            logger.error(e)

    df = pd.DataFrame(json_data_list)

    logger.info(f"程序运行时间：{time.time() - start} s")

    return df


def get_channelkeys(conn, defects):
    sql = f""" SELECT defectid,channelkey
                FROM dbo.defectimages
                WHERE defectid in {tuple(defects["defectid"].values)}"""

    cursor = conn.cursor()
    cursor.execute(sql)
    channelkeys = cursor.fetchall()
    channelkeys = pd.DataFrame(channelkeys)
    return channelkeys


def get_json_simulate(defects):
    """
    Retrieves JSON data from a list of defects.

    Args:
        defects (list): A list of dictionaries representing defects.

    Returns:
        pandas.DataFrame: A DataFrame containing the JSON data from the defects.
    """
    logger.info(f"defects点列表的长度：{len(defects)}")

    json_data_list = []
    start = time.time()
    lack_data = []
    for d in defects:
        try:
            FVBucketName = d["FVBucketName"]
            FVFileName = d["FVFileName"]

            url = f"http://10.211.89.15:9008/SAS3API/api/Storage/sa/{FVBucketName}/{FVFileName}?filetype=json"
            response = requests.get(url, timeout=10)
            json_data = json.loads(response.text)
            json_data["frameid"] = d["frameid"]
            json_data["defectid"] = d["defectid"]
            json_data["qualityid"] = d["qualityid"]
            json_data["attribute"] = d["attribute"]
            json_data_list.append(json_data)
        except requests.exceptions.Timeout:
            logger.error("请求超时")
        except json.decoder.JSONDecodeError:
            logger.error("JSON解码错误")
            logger.info(f"FVBucketName, FVFileName: {FVBucketName}, {FVFileName}")
            lack_data.append(
                {
                    "defectid": d["defectid"],
                    "FVBucketName": FVBucketName,
                    "FVFileName": FVFileName,
                }
            )
            logger.info(response.text)
        except Exception as e:
            logger.error(e)

    df = pd.DataFrame(json_data_list)

    logger.info(f"get json time:{time.time() - start} s")
    logger.info(f"lack data number: {len(lack_data)}")

    return df


def get_data(conn, begintime, endtime, factory_ip, attribute):
    """
    Retrieves data from the specified engine within the given time range and factory IP.

    Parameters:
        engine (Engine): The engine from which to retrieve the data.
        begintime (datetime): The start time of the data retrieval.
        endtime (datetime): The end time of the data retrieval.
        factory_ip (str): The IP address of the factory.

    Returns:
        DataFrame: The retrieved data in the form of a DataFrame.
    """
    feature = get_feature(conn, begintime, endtime, factory_ip, attribute)
    df = get_json(feature)
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
        )
    # 捕获数据库连接错误
    except Exception as e:
        logger.error(e)

    return conn
