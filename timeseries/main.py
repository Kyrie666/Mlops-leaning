import datetime
from db.config import settings
from fastapi import FastAPI, APIRouter, Response
import io
import matplotlib.pyplot as plt
import matplotlib as mpl

mpl.rcParams["font.family"] = "simsun"
# 设置负号显示
mpl.rcParams["axes.unicode_minus"] = False

from db.getdata import get_predition, get_conn, truncate_table
from core.infer import predict_data, synchronize_data
from core.monitor import upload_monitor_data
import pandas as pd

from apscheduler.schedulers.background import BackgroundScheduler

# from contextlib import asynccontextmanager
from utils.log import logger
import time


# apscheduler 定时任务
# lifespan 生命周期
# async context manager 上下文管理器


def scheduled_task():
    logger.info(f"{datetime.date.today()} scheduled task start!")

    truncate_table("time_data")
    logger.info(f"{datetime.date.today()} truncate table success!")
    time.sleep(2)

    synchronize_data()
    logger.info(f"{datetime.date.today()} synchronize data success!")
    time.sleep(10)

    predict_data()
    logger.info(f"{datetime.date.today()} scheduled task success!")


def monitor_task():
    upload_monitor_data(datetime.date.today() - datetime.timedelta(days=1))
    logger.info(
        f"monitor date: {datetime.date.today() - datetime.timedelta(days=1)},monitor task success!"
    )


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


@api_router.put("/synchronize")
def synchronize():
    scheduled_task()
    monitor_task()


@api_router.get("/")
def get_prediction_result():
    try:
        conn = get_conn(
            settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
        )
        logger.info("database connect success!")

        data = get_predition(conn, datetime.date.today())
        conn.close()

        # 创建Pandas数据框
        df = pd.DataFrame(data)
        logger.info(df.head(2))
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
        if conn:
            conn.close()
        return {"message": "error"}


app = create_application()


@app.on_event("startup")
async def startup_event():
    scheduler = BackgroundScheduler()
    scheduler.add_job(scheduled_task, "cron", hour=7, minute=10)
    scheduler.add_job(monitor_task, "cron", hour=7, minute=50)
    scheduler.start()
    logger.info("scheduler startup success!")
