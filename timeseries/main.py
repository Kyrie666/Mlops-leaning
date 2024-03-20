import datetime
from .db.config import settings
from fastapi import FastAPI, APIRouter, Response
import io
import matplotlib.pyplot as plt
import matplotlib as mpl
from .db.getdata import get_predition, get_conn, truncate_table
from .core.infer import predict_data, synchronize_data
from .core.monitor import upload_monitor_data
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler

from .utils.log import logger
import time
from pytz import timezone

try:
    # 获取所有可用字体的列表
    font_list = mpl.font_manager.findSystemFonts()
    # 打印所有可用字体
    for font_path in font_list:
        font = mpl.font_manager.FontProperties(fname=font_path)
        logger.info(font.get_name())
except Exception as err:
    pass


mpl.rcParams["font.sans-serif"] = [
    "WenQuanYi Zen Hei"
]  # 设置中文显示的字体为文泉驿正黑


def scheduled_task():
    # 设置全局时区为北京时间
    beijing_tz = timezone("Asia/Shanghai")
    today = datetime.datetime.now(beijing_tz).date()
    logger.info(f"{today} scheduled task start!")

    truncate_table("time_data")
    logger.info(f"{today} truncate table success!")
    time.sleep(2)

    synchronize_data(today)
    logger.info(f"{today} synchronize data success!")
    time.sleep(10)

    predict_data(today)
    logger.info(f"{today} scheduled task success!")


def monitor_task():
    # 设置全局时区为北京时间
    beijing_tz = timezone("Asia/Shanghai")
    today = datetime.datetime.now(beijing_tz).date()
    upload_monitor_data(today - datetime.timedelta(days=1))
    logger.info(f"monitor date: {today},monitor task success!")


api_router = APIRouter(prefix="/v1", tags=["Predicting the Number of Resignations"])


def create_application() -> FastAPI:
    app = FastAPI(
        title="Predicting the Number of Resignations",
        description="离职人数预测",
        openapi_url=f"/{settings.PROJECT_NAME}/openapi.json",
        docs_url=f"/{settings.PROJECT_NAME}/docs",
    )
    app.include_router(api_router, prefix=f"/{settings.PROJECT_NAME}")

    return app


# @api_router.put("/synchronize")
# def synchronize():
#     scheduled_task()
#     monitor_task()


def get_prediction_result():
    try:
        conn = get_conn(
            settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
        )
        logger.info("database connect success!")

        beijing_tz = timezone("Asia/Shanghai")
        today = datetime.datetime.now(beijing_tz).date()
        data = get_predition(conn, today)
        conn.close()

        # 创建Pandas数据框
        df = pd.DataFrame(data)
        df["number"] = df["number"].astype(int)
        # Add Chinese name dictionary
        chinese_name_dict = {
            "bs": "白石园区",
            "jm": "精密园区",
            "sz": "深圳园区",
            "gx": "高新园区",
        }
        df["department"] = df["department"].map(chinese_name_dict)
        return df
    except Exception as err:
        logger.error(err)
        if conn:
            conn.close()
        return None


@api_router.get("/table")
def get_prediction_result_table():
    try:
        df = get_prediction_result()
        # 创建表格
        fig, ax = plt.subplots()
        ax.axis("tight")
        ax.axis("off")
        ax.set_title("普工离职人数预测（非派遣）", loc="center")
        table = ax.table(cellText=df.values, colLabels=df.columns, loc="center")

        # 将表格保存到内存中
        img = io.BytesIO()
        plt.savefig(img, format="png")
        img.seek(0)

        # 将表格作为响应返回给客户端
        return Response(content=img.getvalue(), media_type="image/png")

    except Exception as err:
        logger.error(err)
        return {"message": "error"}


@api_router.get("/json")
def get_prediction_result_json():
    try:
        df = get_prediction_result()
        return df.to_dict("records")

    except Exception as err:
        logger.error(err)
        return {"message": "error"}


app = create_application()


@app.on_event("startup")
async def startup_event():
    scheduler = BackgroundScheduler()
    # 设置时区为北京时间
    scheduler.configure(timezone=timezone("Asia/Shanghai"))
    scheduler.add_job(scheduled_task, "cron", hour=7, minute=10)
    scheduler.add_job(monitor_task, "cron", hour=7, minute=50)
    scheduler.start()
    logger.info("scheduler startup success!")
