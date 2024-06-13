from sklearn.metrics import confusion_matrix


def get_confusion_matrix(data):
    # 这里的OK 是改判的ok 和 预测的ok
    # 计算混淆矩阵
    cm = confusion_matrix(data["label"], data["new_label"], labels=["OK", "NG"])
    # 计算原改判率
    orign_change = (cm[0][0] + cm[0][1]) / (cm[0][0] + cm[0][1] + cm[1][0] + cm[1][1])
    # 计算新方案的改判率
    new_change = (cm[0][1]) / (cm[0][0] + cm[0][1] + cm[1][0] + cm[1][1])
    # 统计可能新增的漏检数量
    new_forget_num = cm[1][0]

    return cm, orign_change, new_change, new_forget_num
