# simulate aoi to test
from .log import logger
from sklearn.metrics import confusion_matrix
import time

NUM_TRANS_PARAM = 0.012


def analy_rule(rule):
    for sub_classifier in rule.keys():
        factor_str = rule[sub_classifier].pop()
        factor = factor_str.split(":")[1]
        if "||" in factor:
            temp = []
            for s in factor.split("||"):
                temp.append(s[1:-1].split("&&"))
            rule[sub_classifier].append(temp)
        else:
            rule[sub_classifier].append(factor.split("&&"))
    return rule


def simulate_aoi(defects, rule):
    start = time.time()

    defects["simulated_aoi"] = "1"
    defects["simulated_aoi_att"] = "1"
    for index, row in defects.iterrows():
        defects.loc[index, ["simulated_aoi", "simulated_aoi_att"]] = single_aoi_test(
            row, rule
        )

    cm = confusion_matrix(
        defects["qualityid"], defects["simulated_aoi"], labels=["OK", "NG"]
    )

    logger.info(f"simulate_aoi time:{time.time() - start} s")

    return cm, defects[defects["simulated_aoi"] != defects["qualityid"]]


def single_clf(value, classifier_name, rule, factor):

    sub_classifier = rule[classifier_name]

    if isinstance(factor[0], list):
        return single_clf(value, classifier_name, rule, factor[0]) or single_clf(
            value, classifier_name, rule, factor[1]
        )
    else:
        for i, feature in enumerate(sub_classifier):
            if i == len(sub_classifier) - 1:
                break

            if feature["Name"] not in value.index.to_list():
                return [True, classifier_name]
            elif feature["No"] in factor:
                if feature["Name"] == "缺陷面积":
                    val = value[feature["Name"]] / (pow(NUM_TRANS_PARAM, 2))
                elif feature["Name"] == "缺陷长" or feature["Name"] == "缺陷宽(深)":
                    val = value[feature["Name"]] / NUM_TRANS_PARAM

                else:
                    val = value[feature["Name"]]

                if val < feature["Range"][0] or val > feature["Range"][1]:
                    return [True, classifier_name]

        return [False, classifier_name]


def single_aoi_test(value, rule):
    for classifier_name in rule.keys():
        channelkey = classifier_name[:4]
        if channelkey != value["channelkey"]:
            continue
        factor = rule[classifier_name][-1]
        if single_clf(value, classifier_name, rule, factor)[0]:
            continue
        else:
            return ["NG", classifier_name]
    return ["OK", "noclass"]
