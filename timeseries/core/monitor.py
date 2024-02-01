# monitor prediction result
from db.getdata import get_conn, get_engine, sqlcol
from db.config import settings
from utils.log import logger
from utils.metrics import mae
import pandas as pd


def monitor_get_prediction(conn, date):
    """
    Retrieves prediction data from the database for a given date.

    Args:
        conn: A connection to the database.
        date: The date for which prediction data is requested.

    Returns:
        The prediction data for the given date, or None if no data is found.
    """
    try:
        sql = f""" select number,
                department 
                from hr.dbo.prediction_data 
                where start_date = '{date}' and end_date = '{date}'"""
        cursor = conn.cursor()
        cursor.execute(sql)
        predict_data = cursor.fetchall()
        if len(predict_data) == 0:
            logger.info("no prediction data")
            return None
    except Exception as err:
        logger.error(err)
        return None
    return predict_data


def monitor_get_real(conn, date):
    """
    Get real data from the database for a specific date.

    Args:
        conn: The database connection object.
        date: The specific date for which to retrieve the real data.

    Returns:
        The real data fetched from the database for the specified date, or None if no data was found.
    """
    try:
        sql = f"select department, 离职人数 from hr.dbo.time_data where date = '{date}'"
        cursor = conn.cursor()
        cursor.execute(sql)
        real_data = cursor.fetchall()
        if len(real_data) == 0:
            logger.info("no real data")
            return None
    except Exception as err:
        logger.error(err)
        return None
    return real_data


def monitor_prediction(date):
    """
    Function to monitor prediction based on the given date.

    Args:
        date: The date for which the prediction is to be monitored.

    Returns:
        pandas DataFrame: Merged data with calculated mean absolute error (MAE) and MAE rate.
    """
    conn = get_conn(
        settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
    )
    logger.info("database connect success!")
    predict_data = monitor_get_prediction(conn, date)
    predict_data = pd.DataFrame(predict_data)
    real_data = monitor_get_real(conn, date)
    real_data = pd.DataFrame(real_data)
    all_data = pd.merge(real_data, predict_data, on="department")
    all_data["mae"] = abs(all_data["离职人数"] - all_data["number"])
    all_data["mae_rate"] = all_data["mae"] / (all_data["离职人数"] + 1e-5)
    return all_data


def upload_monitor_data(date):
    """
    Uploads monitor data for a specific date to the database.

    :param date: The date for which the monitor data is being uploaded.
    :return: None
    """
    monitor_data = monitor_prediction(date)
    monitor_data["date"] = date.strftime("%Y-%m-%d")
    try:
        engine = get_engine(
            settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
        )
        monitor_data.to_sql(
            name="monitor_data",
            con=engine,
            if_exists="append",
            index=False,
            dtype=sqlcol(monitor_data),
        )
        engine.dispose()
    except Exception as err:
        logger.error(err)
