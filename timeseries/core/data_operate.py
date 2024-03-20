# dimission-rate-analysis/timeseries/core/data_operate.py
import pandas as pd
import numpy as np
import warnings

warnings.filterwarnings("ignore")

from ..db.config import settings

from lunarcalendar import Converter
import lunardate

import holidays
from ..utils.log import logger


def cal_exist_count(data: pd.DataFrame, current_date: pd.Timestamp) -> int:
    """
    Calculates the number of employees who are currently employed on a given date.

    Args:
        data (pandas.DataFrame): A DataFrame containing employee data, including "入职日期" (date of joining) and "离开日期" (date of leaving).
        current_date (datetime.date): The date for which the count of employed employees should be calculated.

    Returns:
        int: The count of employees who are currently employed on the given date.

    Raises:
        Exception: If there is an error in calculating the count.

    """
    try:
        on_job = data[
            (data["入职日期"] <= current_date) & (data["离开日期"] > current_date)
        ]
    except Exception as err:
        logger.error(err)
        return -1
    return on_job.shape[0]


# 正式上线需要废弃
def add_ill_feature(data):
    data["疫情等级"] = 0
    data.loc[
        (data.index >= pd.to_datetime("2020-05-01"))
        & (data.index < pd.to_datetime("2023-01-01")),
        "疫情等级",
    ] = 1
    data.loc[
        (data.index >= pd.to_datetime("2020-02-01"))
        & (data.index < pd.to_datetime("2020-05-01")),
        "疫情等级",
    ] = 2


def add_lunar_feature(df: pd.DataFrame):
    """
    Converts the given DataFrame's index from the Gregorian calendar to the lunar calendar.

    Parameters:
        df (DataFrame): The DataFrame to be modified.

    Returns:
        None
    """
    # 将公历日期转换为农历日期
    lunar_dates = [Converter.Solar2Lunar(date) for date in df.index]

    # 提取农历年、月、日
    lunar_years = [date.year for date in lunar_dates]
    lunar_months = [date.month for date in lunar_dates]
    lunar_days = [date.day for date in lunar_dates]

    # 将农历年、月、日作为新的特征列添加到DataFrame中
    df["lunar_year"] = lunar_years
    df["lunar_month"] = lunar_months
    df["lunar_day"] = lunar_days


def add_exist_yesterday_feature(data: pd.DataFrame):
    """
    Add a feature to the given data that represents the number of employees in the previous day.

    Parameters:
        data (pandas.DataFrame): The input data containing the "在职人数" column.

    Returns:
        None
    """
    tt = data["在职人数"].to_list()
    tt.pop()
    tt.insert(0, tt[0])
    data["前一天在职人数"] = tt
    data.drop(["在职人数"], axis=1, inplace=True)


def add_depart_yesterday_feature(data: pd.DataFrame):
    """
    Add a feature to the given DataFrame indicating the number of employees who resigned the previous day.

    Parameters:
    - data (pd.DataFrame): The DataFrame to modify.

    Returns:
    - None: This function does not return anything.
    """
    tt = data["离职人数"].to_list()
    tt.pop()
    tt.insert(0, tt[0])
    data["前一天离职人数"] = tt


def add_dimission_feature(data: pd.DataFrame):
    """
    Calculate the ratio of the number of employees who resigned on the previous day to the number of employees who were employed on the previous day.

    Parameters:
        data (pd.DataFrame): The DataFrame containing the employee data.

    Returns:
        None
    """
    data["离职人数比"] = data["前一天离职人数"] / data["前一天在职人数"]


def add_holiday_feature(data: pd.DataFrame):
    """
    Adds a holiday feature to the given DataFrame.

    Parameters:
    - data: pd.DataFrame
        The DataFrame to which the holiday feature will be added.

    Returns:
    None
    """
    # 选择中国的节假日
    chinese_holidays = holidays.China()
    data["Date"] = pd.to_datetime(data.index)
    data["IsHoliday"] = data["Date"].apply(lambda x: 1 if x in chinese_holidays else 0)
    # 设置农历腊月24 到正月初7为春节
    data["IsSpringFestival"] = data["Date"].apply(is_spring_festival)
    data.drop("Date", axis=1, inplace=True)


def is_spring_festival(date) -> int:
    """
    Check if a given date is the Spring Festival (Chinese New Year).

    Args:
        date (date): The date to be checked.

    Returns:
        int: 1 if the date is the Spring Festival, 0 otherwise.
    """
    lunar_date = lunardate.LunarDate.fromSolarDate(date.year, date.month, date.day)
    if lunar_date.month == 1 and (lunar_date.day >= 1 and lunar_date.day <= 7):
        return 1
    if lunar_date.month == 12 and (lunar_date.day >= 26 and lunar_date.day <= 30):
        return 1
    return 0


def add_feature(data: pd.DataFrame):
    """
    Generates a set of features for the given data.

    Parameters:
        data (list): The input data for generating features.

    Returns:
        None
    """
    # add_ill_feature(data)
    try:
        add_lunar_feature(data)
        add_exist_yesterday_feature(data)
        add_depart_yesterday_feature(data)
        add_dimission_feature(data)
        add_holiday_feature(data)
    except Exception as err:
        logger.error(err)


def get_all_data(employee_data, depart_data, start_time, end_time):
    all_data = pd.concat([employee_data, depart_data], axis=0)
    all_data.reset_index(inplace=True, drop=True)
    all_data.columns = ["入职日期", "离开日期"]
    on_job_count = []
    for d in pd.date_range(start=start_time, end=end_time):
        on_job_count.append(cal_exist_count(all_data, d))
    all_data["离开日期"] = pd.to_datetime(all_data["离开日期"])
    # 按照日期进行分组，并统计每个日期的人数
    daily_resignations = all_data.groupby([all_data["离开日期"].dt.date]).size()
    daily_resignations = pd.DataFrame(daily_resignations)
    daily_resignations.columns = ["离职人数"]
    data = daily_resignations[
        (pd.to_datetime(daily_resignations.index) >= pd.to_datetime(start_time))
        & (pd.to_datetime(daily_resignations.index) <= pd.to_datetime(end_time))
    ]
    temp = pd.date_range(start=start_time, end=end_time)
    data = data.reindex(temp)
    data["离职人数"] = data["离职人数"].fillna(0)
    data["在职人数"] = on_job_count

    add_feature(data)
    return data


# ADD time features to our model
def create_time_features(df, target=None):
    """
    Creates time series features from datetime index
    """
    df["date"] = pd.to_datetime(df.index)
    df["hour"] = df["date"].dt.hour
    df["dayofweek"] = df["date"].dt.dayofweek
    df["quarter"] = df["date"].dt.quarter
    df["month"] = df["date"].dt.month
    df["year"] = df["date"].dt.year
    df["dayofyear"] = df["date"].dt.dayofyear
    df["sin_day"] = np.sin(df["dayofyear"])
    df["cos_day"] = np.cos(df["dayofyear"])
    df["dayofmonth"] = df["date"].dt.day
    df["weekofyear"] = df["date"].dt.isocalendar().week
    X = df.drop(["date"], axis=1)
    if target:
        y = df[target]
        X = X.drop([target], axis=1)
        return X, y

    return X
