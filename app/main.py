from fastapi import FastAPI, BackgroundTasks, APIRouter
from pydantic import BaseModel
from datetime import datetime

from .core.log import logger
from .db.getdata import (
    get_conn,
    get_data,
    get_feature_id,
    get_classifier_name,
    get_pieces_from_defect,
    get_machine_ip,
    get_json_simulate,
    get_defects_from_pieces,
    get_pieces,
    get_channelkeys,
)
from .core.config import settings
from .core.read_json import get_rule_json, dic_feature_name_en_cn, parse_csv
from .core.search_value import (
    adjust_value,
    match_cn_en_name,
    analy_clf_factor,
    check_if_adjust,
    operate_data,
)
from .core.simulate import analy_rule, simulate_aoi
import pandas as pd
import time
import httpx

api_router = APIRouter(prefix="/v1", tags=["recommend threshold"])
api_router_simulate = APIRouter(prefix="/v1", tags=["aoi simulate"])


def create_application() -> FastAPI:
    app = FastAPI(
        title="Search Value Service",
        description="阈值推荐服务",
        openapi_url=f"/{settings.PROJECT_NAME}/openapi.json",
        docs_url=f"/{settings.PROJECT_NAME}/docs",
    )
    app.include_router(api_router, prefix=f"/{settings.PROJECT_NAME}")
    app.include_router(api_router_simulate, prefix=f"/{settings.PROJECT_NAME}")
    return app


class CalculationDto(BaseModel):
    task_id: str
    classifier_id: str
    start_time: datetime
    end_time: datetime
    equipment_id: str


class SimulateDto(BaseModel):
    start_time: str = "2024.3.20 13:45:00"
    end_time: str = "2024.3.20 14:24:00"
    machine_ip: str = "10.200.211.19"


NUM_TRANS_PARAM = 0.012


@api_router_simulate.post("/simulate")
def get_aoi_simulate_result(dto: SimulateDto):
    conn = get_conn(
        settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
    )
    logger.info("simulate:database connected")
    pieces = get_pieces(conn, dto.machine_ip, dto.start_time, dto.end_time)
    defects = get_defects_from_pieces(conn, pieces)
    defects_val = get_json_simulate(defects)
    conn.close()
    defects_val.rename(columns=dic_feature_name_en_cn, inplace=True)
    defects_val.drop(["子类型"], axis=1, inplace=True)

    # 求交集
    intersection = set(defects_val.columns.to_list()) & set(
        list(dic_feature_name_en_cn.values())
    )
    # 将交集转换回列表（如果需要）
    intersection_list = list(intersection)
    defects_val[intersection_list] = defects_val[intersection_list].astype(float)
    defects_val["缺陷长宽比"] = defects_val["缺陷长"] / defects_val["缺陷宽(深)"]
    defects_val.fillna(0, inplace=True)

    # add channel
    conn_image = get_conn(
        settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE_IMAGE
    )
    channelkeys = get_channelkeys(conn_image, defects_val)
    conn_image.close()

    defects_val = pd.merge(defects_val, channelkeys, on="defectid")

    logger.info(f"defects_val: {defects_val.head()}")
    logger.info(defects_val["attribute"].value_counts())

    rule_S1 = get_rule_json("./json/rule_file_A15-2023-07-31.xml.json")
    rule_S2 = get_rule_json("./json/rule_file_A15-2023-10-13.xml.json")
    rule = {**rule_S1, **rule_S2}
    rule = analy_rule(rule)

    cm, diff_data = simulate_aoi(defects_val, rule)
    logger.info(f"cm: {cm}")
    return parse_csv(diff_data)


def start_calculation(dto: CalculationDto):
    start = time.time()
    logger.info("params: %s", dto)

    headers = {"Content-Type": "application/json"}
    url = settings.POST_URL + "v1/equipments/classifiers/tasks/callback"

    rule = get_rule_json("./json/rule_all.json")
    adjust_rule = get_rule_json("./json/adjust_rule_d3.json")

    conn = get_conn(
        settings.HOST, settings.DBUSER, settings.PASSWORD, settings.DATABASE
    )

    logger.info(f"start_time: {dto.start_time}")
    logger.info(f"end_time: {dto.end_time}")
    logger.info(f"equipment_id: {dto.equipment_id}")
    logger.info("database connect success!")
    classifier_name = get_classifier_name(conn, dto.classifier_id)
    if classifier_name not in rule.keys():
        return
    if classifier_name not in adjust_rule.keys() or not check_if_adjust(
        classifier_name, adjust_rule
    ):
        ex_message = {
            "task_id": dto.task_id,
            "initial_false_discovery_rate": -1,
            "proposals": [],
        }
        logger.info(ex_message)
        response = httpx.post(url=url, json=ex_message, headers=headers)
        logger.info(response.text)
        return
    machine_ip = get_machine_ip(conn, dto.equipment_id)
    df = get_data(conn, dto.start_time, dto.end_time, machine_ip, classifier_name)
    if len(df) == 0:
        logger.info("no data")
        return

    orign_pieces = get_pieces_from_defect(conn, df)
    logger.info(orign_pieces.shape)

    feature_id_df = get_feature_id(conn)
    logger.info("get data success!\n")

    classifier_rule = rule[classifier_name]
    factor = analy_clf_factor(classifier_rule["生效条件"])
    classifier_rule.pop("生效条件")

    fea_en_list, feature_name_cn_en = match_cn_en_name(
        dic_feature_name_en_cn, classifier_rule
    )

    select_data = operate_data(df, fea_en_list, classifier_rule)

    logger.info(factor)
    logger.info(classifier_name)
    logger.info(f"cur_data.shape:{select_data.shape}")

    logger.info(select_data.head())

    try:
        result_top, adjusted_data_filter = adjust_value(
            select_data,
            classifier_rule,
            feature_name_cn_en,
            adjust_rule[classifier_name],
            factor,
            dto.task_id,
            feature_id_df,
        )

        logger.info(result_top)
        # logger.info(adjusted_data_filter)
        response = httpx.post(url=url, json=result_top, headers=headers)
        logger.info(response.text)
        logger.info(f"total time: {time.time() - start} s")

        logger.info(f"adjusted_data_filter:{adjusted_data_filter}")

        for f_index in adjusted_data_filter:
            logger.info(f_index)
            pieces = get_pieces_from_defect(conn, df.loc[f_index])
            logger.info(pieces.shape)
        return
    except Exception as e:
        logger.error(e)

        ex_message = {
            "task_id": dto.task_id,
            "initial_false_discovery_rate": -1,
            "proposals": [],
        }

        logger.error(ex_message)
        response = httpx.post(url=url, json=ex_message, headers=headers)
        logger.error(response.text)
        return
    #


@api_router.post("/calculation")
async def send_task(dto: CalculationDto, background_tasks: BackgroundTasks):
    logger.info(f"request received:{dto.task_id}")
    background_tasks.add_task(start_calculation, dto)
    logger.info("task added")
    return {"message": f"task {dto.task_id} received and will be calculated"}


app = create_application()
