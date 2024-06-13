import warnings
import math
import time

import logging
import random

import numpy as np
import pandas as pd

from pyparsing import Word, nums, infixNotation, opAssoc, Literal
from .config import settings
from .evaluation import get_confusion_matrix

warnings.filterwarnings("ignore")

SEARCH_CNT = settings.SEARCH_CNT
NUM_TRANS_PARAM = 0.012


def check_if_adjust(sub_classifier, adjust_rule):
    """
    Check if the given sub_classifier needs adjustment based on the adjust_rule.

    Parameters:
    - sub_classifier (str): The sub_classifier to check.
    - adjust_rule (dict): The adjustment rule for the sub_classifier.

    Returns:
    - bool: True if the sub_classifier needs adjustment, False otherwise.
    """
    for feature in adjust_rule[sub_classifier].keys():
        if feature == "生效条件":
            continue
        feature_range = adjust_rule[sub_classifier][feature]
        floor_min, floor_max = (
            feature_range["floorValue"][0],
            feature_range["floorValue"][1],
        )
        if not math.isclose(floor_min, floor_max):
            return True
        upper_min, upper_max = (
            feature_range["upperValue"][0],
            feature_range["upperValue"][1],
        )
        if not math.isclose(upper_min, upper_max):
            return True
    return False


def transfomer_display_range(feature_range, feature_id):
    result = []
    for name in feature_range.keys():
        feature_dict = {}
        feature_dict["number"] = feature_range[name]["No"]
        feature_dict["feature_id"] = str(
            feature_id[feature_id["display_name"] == name]["id"].values[0]
        )

        feature_dict["minimum"] = feature_range[name]["Value"][0]
        feature_dict["maximum"] = feature_range[name]["Value"][1]
        result.append(feature_dict)
    return result


def adjust_value(
    data,
    feature_rule,
    feature_name,
    feature_range,
    logic_operators,
    task_id,
    feature_id,
):
    """
    需要两个字典：一个字典写入当前值域，一个字典是需要 调整的特征值
    根据不同的子分类器调整对应的特征值的范围，减少调整值的个数和范围；

    假设 字典是rule_dict，对应的特征值是feature_name，对应的特征值的范围是feature_range
    其中下限值有个范围，上限值暂时不考虑
    1.遍历调整特征字典
    2.根据分度值对特征值进行调整，并写入新字典，
    3.进行相关判别标准的判别，记录当前混淆矩阵，以及pr
    4.返回调整后的字典和对应的结果
    """

    start_time = time.time()

    # 这里放置你想要统计时间的代码
    cnt = 0
    cal_res = {}
    adjusted_data = {}
    while cnt < SEARCH_CNT:
        for feature in feature_rule.keys():
            if feature == "缺陷类型":
                continue
            floor_min, floor_max = (
                feature_range[feature]["floorValue"][0],
                feature_range[feature]["floorValue"][1],
            )
            if not math.isclose(floor_min, floor_max):
                new_floor = random.uniform(floor_min, floor_max)
                feature_rule[feature]["Value"][0] = round(new_floor, 1)

            upper_min, upper_max = (
                feature_range[feature]["upperValue"][0],
                feature_range[feature]["upperValue"][1],
            )
            if not math.isclose(upper_min, upper_max):
                new_upper = random.uniform(upper_min, upper_max)
                feature_rule[feature]["Value"][1] = new_upper

        res = classifier(data, feature_rule, feature_name, logic_operators)
        cm, orgin_change, new_change, forget_kill = get_confusion_matrix(res)
        cal_res[(forget_kill, new_change.round(2))] = feature_rule
        adjusted_data[(forget_kill, new_change.round(2))] = res[
            (res["label"] == "NG") & (res["new_label"] == "OK")
        ].index
        cnt += 1

    end_time = time.time()

    execution_time = end_time - start_time
    logging.info("执行时间：%s 秒", execution_time)

    result_top = {
        "task_id": task_id,
        "initial_false_discovery_rate": orgin_change.round(2),
        "proposals": [],
    }
    adjusted_data_filter = []
    filtered_points = operate_points(cal_res.keys())
    for f, n in filtered_points:
        cur_proposal = {
            "feature_ranges": transfomer_display_range(cal_res[(f, n)], feature_id),
            "false_positive_count": int(f),
            "false_discovery_rate": n,
        }
        result_top["proposals"].append(cur_proposal)
        adjusted_data_filter.append(adjusted_data[(f, n)])

    logging.info("最佳:%s\n", result_top)
    return result_top, adjusted_data_filter


def operate_points(points):
    """
    Given a list of points, this function finds the points with the minimum x-coordinate for each distinct x-coordinate.
    It then sorts these points by their x-coordinate and returns the three points with the smallest x-coordinate that
    have a greater x-coordinate and a smaller y-coordinate than the previous point in the sorted list.

    :param points: A list of points represented as tuples (x, y).
    :return: A list of tuples representing the three points with the smallest x-coordinate
        that have a greater x-coordinate and a smaller y-coordinate than the previous point in the sorted list.
    """
    min_x = {}
    for x_, y_ in points:
        if x_ not in min_x or y_ < min_x[x_]:
            min_x[x_] = y_

    sorted_dict = list(sorted(min_x.items(), key=lambda item: item[0]))
    res = [sorted_dict[0]]
    for p in sorted_dict[1:]:
        if len(res) == 3:
            break
        if p[0] > res[-1][0] and p[1] < res[-1][1]:
            res.append(p)
    return res


def classifier(data, feature_rules, feature_name, logic_operators):
    # 初始化新的label
    data["new_label"] = "1"

    for i in range(data.shape[0]):
        for j, rules in enumerate(feature_rules.keys()):
            if rules == "缺陷类型":
                continue
            ruleid = feature_rules[rules]["No"]
            op = logic_operators[logic_operators.find(ruleid) - 1]

            val = data.loc[i, feature_name[rules]]
            rule_val = feature_rules[rules]["Value"]

            if op == "&":
                if val < rule_val[0] or val > rule_val[1]:
                    data.loc[i, "new_label"] = "OK"
                    break
            elif op == "!":
                if val >= rule_val[0] and val <= rule_val[1]:
                    data.loc[i, "new_label"] = "OK"
                    break

        if data.loc[i, "new_label"] != "OK":
            data.loc[i, "new_label"] = "NG"
    return data


def analy_clf_factor(factor):
    """
    Generates a function comment for the given function body.

    Parameters:
        factor (str): The function body to generate the comment for.

    Returns:
        str: The function comment for the given function body.
    """
    # 定义逻辑运算符
    and_operator = Literal("&&")
    or_operator = Literal("||")
    not_operator = Literal("!")

    # 定义语法规则
    operand = Word(nums)
    expression = infixNotation(
        operand,
        [
            (not_operator, 1, opAssoc.RIGHT),
            (and_operator, 2, opAssoc.LEFT),
            (or_operator, 2, opAssoc.LEFT),
        ],
    )

    # 解析逻辑表达式
    parsed_expression = expression.parseString(factor)
    parsed_expression = parsed_expression.as_list()[0]
    parsed_expression.insert(0, "&&")

    # 去除括号并转换为字符串
    def remove_parentheses(parsed_expression):
        """
        Recursively removes parentheses from a parsed expression.

        Parameters:
            parsed_expression (list or any): The parsed expression to remove parentheses from.

        Returns:
            str: The expression with parentheses removed.
        """
        if isinstance(parsed_expression, list):
            if len(parsed_expression) == 1:
                return remove_parentheses(parsed_expression[0])
            else:
                return "".join(remove_parentheses(item) for item in parsed_expression)
        else:
            return str(parsed_expression)

    expression_without_parentheses = remove_parentheses(parsed_expression)

    # 修正一下
    not_pos = expression_without_parentheses.find("!")
    if not_pos != -1:
        expression_without_parentheses = expression_without_parentheses[
            :not_pos
        ] + expression_without_parentheses[not_pos:].replace("&", "!")

    return expression_without_parentheses


def operate_data(data, feature_list, feature_name):
    """
    A function that operates on data by modifying the feature list and feature name.

    Args:
        data (pandas.DataFrame): The input data to be operated on.
        feature_list (list): The list of features.
        feature_name (str): The name of the feature.

    Returns:
        pandas.DataFrame: The modified data after the operations.
    """

    temp_feature = feature_list[:-1].copy()

    if "flawLength" not in temp_feature:
        temp_feature.append("flawLength")
    if "flawWidth" not in temp_feature:
        temp_feature.append("flawWidth")

    temp_data = data[temp_feature].astype(np.float64)

    sub_data = data[feature_list[:-1]].astype(np.float64)

    if "缺陷长宽比" in feature_name:
        temp_feature.append("regionLWRatio")
        sub_data["regionLWRatio"] = temp_data["flawLength"] / temp_data["flawWidth"]
    res_data = pd.concat([sub_data, data[feature_list[-1:]]], axis=1)
    # data = data[temp_feature]

    # if "缺陷长宽比" in feature_name:
    #     data.iloc[:, :-3] = data.iloc[:, :-3].astype(np.float)
    # else:
    #     data.iloc[:, :-2] = data.iloc[:, :-2].astype(np.float)
    if "flawLength" in feature_list:
        res_data["flawLength"] = res_data["flawLength"] / NUM_TRANS_PARAM
    if "flawWidth" in feature_list:
        res_data["flawWidth"] = res_data["flawWidth"] / NUM_TRANS_PARAM
    if "flawArea" in feature_list:
        res_data["flawArea"] = res_data["flawArea"] / (pow(NUM_TRANS_PARAM, 2))

    return res_data


def match_cn_en_name(feature_name_en_cn, feature_name_cn):
    """
    This function takes in two dictionaries, feature_name_en_cn and feature_name_cn, and returns two dictionaries. The first dictionary contains English feature names as keys and their corresponding Chinese names as values. The second dictionary contains Chinese feature names as keys and their corresponding English names as values. The function also adds the string "label" to the list of English feature names. If the key "缺陷长宽比" exists in the feature_name_cn dictionary, the value of this key in the feature_name_cn_en dictionary is changed to "regionLWRatio".

    Parameters:
    feature_name_en_cn (dict): A dictionary containing English feature names as keys and their corresponding Chinese names as values.
    feature_name_cn (dict): A dictionary containing Chinese feature names as keys and their corresponding English names as values.

    Returns:
    tuple: A tuple containing two dictionaries. The first dictionary contains English feature names as keys and their corresponding Chinese names as values. The second dictionary contains Chinese feature names as keys and their corresponding English names as values.
    """
    feature_name_en = [
        k for k, v in feature_name_en_cn.items() if v in feature_name_cn.keys()
    ]
    feature_name_cn_en = {
        v: k for k, v in feature_name_en_cn.items() if v in feature_name_cn.keys()
    }
    feature_name_en.extend(["label"])
    if "缺陷长宽比" in feature_name_cn.keys():
        feature_name_cn_en["缺陷长宽比"] = "regionLWRatio"
    return feature_name_en, feature_name_cn_en


def rule_transfomer(data):
    # 用于对数据中获取的规则进行转换
    # 转换目标是目前已有的json字典
    pass
