import warnings

import numpy as np
import pandas as pd  # Basic library for all of our dataset operations
import lightgbm as lgb
import xgboost as xgb
from ..utils.metrics import evaluate
from datetime import datetime, timedelta
from .data_operate import is_spring_festival

# We will use deprecated models of statmodels which throw a lot of warnings to use more modern ones
warnings.filterwarnings("ignore")

# Extra settings
seed = 42
np.random.seed(seed)
from ..utils.log import logger
import lunardate


def generate_new_data(data, y_hat):
    """
    Generate new data based on the input data and predicted values.

    Args:
        data: The input data to be modified.
        y_hat: The predicted values for the next day.

    Returns:
        data: The modified data with new calculated features.
    """
    date_today = pd.to_datetime(data.name)
    next_day = pd.to_datetime(date_today + timedelta(days=1), format="%Y-%m-%D")

    data.name = next_day
    data["前一天离职人数"] = y_hat
    data["前一天在职人数"] -= y_hat
    data["离职人数比"] = data["前一天离职人数"] / data["前一天在职人数"]

    data["hour"] = next_day.hour
    data["dayofweek"] = next_day.dayofweek
    data["quarter"] = next_day.quarter
    data["month"] = next_day.month
    data["year"] = next_day.year
    data["dayofyear"] = next_day.dayofyear
    data["sin_day"] = np.sin(data["dayofyear"])
    data["cos_day"] = np.cos(data["dayofyear"])
    data["dayofmonth"] = next_day.day
    data["weekofyear"] = next_day.isocalendar().week

    lunar_date = lunardate.LunarDate.fromSolarDate(
        next_day.year, next_day.month, next_day.day
    )
    data["lunar_year"] = lunar_date.year
    data["lunar_month"] = lunar_date.month
    data["lunar_day"] = lunar_date.day

    data["IsSpringFestival"] = is_spring_festival(next_day)
    return data


def lightGBM_train(X_train_df, y_train, X_test_df, y_test, days):
    """
    Train a LightGBM model using the given training and test data, and predict the target variable values.

    Parameters:
    - X_train_df: the training feature data in DataFrame format
    - y_train: the training target variable data
    - X_test_df: the test feature data in DataFrame format
    - y_test: the test target variable data
    - days: interval for predicting target variable values

    Returns:
    - res: list of predicted target variable values
    """
    res = []
    for i in range(0, len(y_test), days):
        lightGBM = lgb.LGBMRegressor()
        X_train_data = pd.concat([X_train_df, X_test_df[:i]], ignore_index=True)
        y_train_data = pd.concat([y_train, y_test[:i]])
        lightGBM.fit(X_train_data.values, y_train_data)
        yhat = lightGBM.predict(X_test_df.values[i : i + 1])
        # yhat = np.round(yhat)
        # yhat[yhat < 0] = 0
        # resultsDict[f'Lightgbm_{i}'] = evaluate(df_test["离职人数"][i:i+1], yhat)
        res.extend(yhat.tolist())
        day_i = i
        for j in range(1, days):
            if day_i >= X_test_df.shape[0]:
                break
            yhat = lightGBM.predict(
                generate_new_data(X_test_df.iloc[day_i], yhat).values.reshape(1, -1)
            )
            res.extend(yhat.tolist())
            day_i += 1

    return res


def xgboost_train(X_train_df, y_train, X_test_df, y_test, days):
    """
    Train an XGBoost model using the given training and test data, with a specified interval for prediction.

    Args:
    X_train_df: DataFrame containing the features for training.
    y_train: Series containing the target values for training.
    X_test_df: DataFrame containing the features for testing.
    y_test: Series containing the target values for testing.
    days: Integer specifying the interval for prediction.
    """
    res = []
    for i in range(0, len(y_test), days):
        xgb_model = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=1000)
        X_train_data = pd.concat([X_train_df, X_test_df[:i]], ignore_index=True)
        y_train_data = pd.concat([y_train, y_test[:i]])
        xgb_model.fit(X_train_data.values, y_train_data)
        yhat = xgb_model.predict(X_test_df.values[i : i + 1])
        # yhat = np.round(yhat)
        # yhat[yhat < 0] = 0
        # yhat[yhat>df_training["离职人数"].max()] = df_training["离职人数"].max()
        res.extend(yhat.tolist())
        day_i = i
        for j in range(1, days):
            if day_i >= X_test_df.shape[0]:
                break
            yhat = xgb_model.predict(
                generate_new_data(X_test_df.iloc[day_i], yhat).values.reshape(1, -1)
            )
            # yhat = np.round(yhat)
            # yhat[yhat < 0] = 0
            # yhat[yhat>df_training["离职人数"].max()] = df_training["离职人数"].max()
            res.extend(yhat.tolist())
            day_i += 1


def train_and_predict(X_train_df, y_train, X_test_df, y_test, days, model_type):
    """
    Train and predict using the specified model type on the given training and test data.

    Args:
        X_train_df: The training data features.
        y_train: The training data labels.
        X_test_df: The test data features.
        y_test: The test data labels.
        days: The number of days for training.
        model_type: The type of model to use for training and prediction.

    Returns:
        The predictions made by the trained model.
    """
    if model_type == "lgbm":
        return lightGBM_train(X_train_df, y_train, X_test_df, y_test, days)
    else:
        return xgboost_train(X_train_df, y_train, X_test_df, y_test, days)


def evaluate_all(actual: pd.DataFrame, predicted: np.ndarray, days: int):
    """
    Function to evaluate actual and predicted values over a specified number of days.

    Args:
        actual (pd.DataFrame): The actual values to be evaluated.
        predicted (np.ndarray): The predicted values to be evaluated.
        days (int): The number of days over which to evaluate the values.

    Returns:
        The evaluation result from the evaluate function.
    """
    if len(actual) % 3 and days != 1:
        temp = pd.Series([actual.rolling(window=days).sum()[-1]])
    else:
        temp = pd.Series([])
    actual = actual.rolling(window=days).sum()[days - 1 :: days]
    new_actual = pd.concat([actual, temp], ignore_index=True)
    predicted = [sum(predicted[i : i + days]) for i in range(0, len(predicted), days)]
    predicted = np.array(predicted).round(0)
    predicted[predicted < 0] = 0

    return evaluate(new_actual, predicted)
