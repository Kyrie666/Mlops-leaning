import pandas as pd
import warnings
import datetime

warnings.filterwarnings("ignore")
import xgboost as xgb
import lightgbm as lgb
from .train import generate_new_data
from utils.log import logger
from .data_operate import get_all_data, create_time_features
from db.getdata import (
    get_employee_data,
    get_depart_data,
    get_conn,
    get_time_data,
    get_last_date,
    add_new_time_data,
    add_prediction,
)
from db.config import settings


def lightGBM_infer(X_train_df, y_train, X_test_df, days):
    """
    Perform inference using a LightGBM model.

    Args:
        X_train_df (DataFrame): The input training data.
        y_train (array-like): The target values for training.
        X_test_df (DataFrame): The input test data.
        days (int): The number of days for which to make predictions.

    Returns:
        int: The rounded sum of the predicted values, or 0 if the sum is less than or equal to 0.
    """

    res = []
    lightGBM = lgb.LGBMRegressor(verbose=-1)
    lightGBM.fit(X_train_df.values, y_train)
    logger.info(X_test_df.shape)
    yhat = lightGBM.predict(X_test_df)
    logger.info(yhat)
    res.extend(yhat.tolist())
    temp_data = X_test_df.iloc[0]
    for j in range(1, days):
        new_data = generate_new_data(temp_data, yhat[0])
        yhat = lightGBM.predict(new_data.values.reshape(1, -1))
        res.extend(yhat.tolist())
        temp_data = new_data

    return round(sum(res), 0) if sum(res) > 0 else 0


def xgboost_infer(X_train_df, y_train, X_test_df, days):
    """
    Perform XGBoost inference using the trained model to predict values for the given test data.

    Args:
        X_train_df: DataFrame, the training data features
        y_train: array-like, the training data target values
        X_test_df: DataFrame, the test data features
        days: int, the number of days to predict

    Returns:
        int, the rounded sum of the predicted values if the sum is positive, else 0
    """
    res = []
    xgb_model = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=1000)
    xgb_model.fit(X_train_df.values, y_train)
    yhat = xgb_model.predict(X_test_df)
    res.extend(yhat.tolist())
    temp_data = X_test_df.iloc[0]
    for j in range(1, days):
        new_data = generate_new_data(temp_data, yhat)
        yhat = xgb_model.predict(new_data)
        res.extend(yhat.tolist())
        temp_data = new_data

    return round(sum(res), 0) if sum(res) > 0 else 0


def train_and_infer(X_train_df, y_train, X_test_df, days, model_type):
    if model_type == "lgbm":
        return lightGBM_infer(X_train_df, y_train, X_test_df, days)
    else:
        return xgboost_infer(X_train_df, y_train, X_test_df, days)


def predict_data():
    """
    Predict data using the specified database connection and time series analysis.
    This function does not take any parameters and does not return any value.
    """
    conn = get_conn(
        settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
    )
    logger.info("database connect success!")
    time_data = get_time_data(conn)
    time_data.index = pd.to_datetime(time_data["date"])
    time_data.drop(["date"], axis=1, inplace=True)
    logger.info(time_data.iloc[:2, :2])
    last_day = datetime.date.today()
    result = {"department": [], "start_date": [], "end_date": [], "number": []}
    for department in ["bs", "jm", "sz", "gx"]:
        cur_data = time_data[time_data["department"] == department]
        cur_data.drop(["department"], axis=1, inplace=True)
        logger.info(cur_data.iloc[:2, :2])
        df_training = cur_data.loc[cur_data.index < pd.Timestamp(last_day)]
        df_test = cur_data.loc[cur_data.index == pd.Timestamp(last_day)]
        X_train_df, y_train = create_time_features(df_training, target="离职人数")
        X_test_df, y_test = create_time_features(df_test, target="离职人数")

        logger.info("split data success!")
        logger.info("train and infer")

        for days in [1, 3, 7]:
            if days == 1:
                number = train_and_infer(
                    X_train_df, y_train, X_test_df, days, "xgboost"
                )
            else:
                number = train_and_infer(X_train_df, y_train, X_test_df, days, "lgbm")
            logger.info(f"{department}-{days} train and infer success!")

            result["department"].append(department)
            result["start_date"].append(last_day)
            result["end_date"].append(last_day + datetime.timedelta(days=days - 1))
            result["number"].append(number)
    result = pd.DataFrame(result)
    add_prediction(result)
    logger.info("add predict data success!")
    conn.close()
    return


def synchronize_data():
    """
    Synchronize data from various sources and update the database accordingly.
    """
    conn = get_conn(
        settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
    )
    logger.info("database connect success!")
    time_data_lastday = get_last_date(conn)

    logger.info(time_data_lastday)
    today = datetime.date.today()
    if time_data_lastday is None:
        start_time = settings.TRAIN_START_DATE
        end_time = today
    else:
        logger.info("data is up to date!")
        conn.close()
        return

    logger.info(f"start_time:{start_time},end_time:{end_time}")
    for department in ["bs", "jm", "sz", "gx"]:
        employee_data = get_employee_data(conn, department)
        depart_data = get_depart_data(conn, start_time, end_time, department)
        new_data = get_all_data(employee_data, depart_data, start_time, end_time)

        logger.info(new_data.head(2))
        logger.info(new_data.tail(2))

        # 增加园区特征
        new_data["department"] = department
        add_new_time_data(new_data)
        logger.info(f"{department} add new data success!")

    conn.close()
    logger.info("synchronize data success!")
    return
