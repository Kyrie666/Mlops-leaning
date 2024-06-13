import json

dic_feature_name_en_cn = {
    "dist2baseContourDeviation": "缺陷到基准轮廓的距离方差",
    "innerWidth": "内接矩形宽度",
    "meanGray": "均值",
    "varianceGray": "方差",
    "numLowGrayGrad": "低灰度差像素数",
    "numHighGrayGrad": "高灰度差像素数",
    "highGrayDiffArea5": "高梯度差像素个数5",
    "highgraydiffarea2defectareaRatio2": "高梯度差面积2占缺陷比例",
    "tworegionsmeangrayratio": "两连通域平均灰度比值",
    "lowGrayDiffArea2": "低梯度差像素个数2",
    "highgraydiffarea2defectareaRatio5": "高梯度差面积5占缺陷比例",
    "grayNeiRegionRing2Mean": "缺陷邻域之外的邻域背景平均灰度",
    "numHoles": "孔洞个数",
    "singleMaxregionArea": "最大缺陷面积",
    "rect2Len2": "最小外接矩形短边长度",
    "regionbackgroundMeangraydiff": "缺陷与背景平均灰度差值绝对值",
    "grayNeiRegionMin": "包含邻域的灰度最小值",
    "rect2Len1": "最小外接矩形长边长度",
    "grayRange": "灰度范围",
    "grayRb": "灰度椭圆短轴长度",
    "grayNeiRegionRingDeviation": "缺陷邻域灰度标准差",
    "tworegionsmindistance": "两连通域最小距离",
    "grayNeiRegionhighGrayDiffArea3": "包含邻域高梯度差像素个数3",
    "lowgraydiffarea2defectareaRatio4": "低梯度差面积4占缺陷比例",
    "structFactor": "结构因子(anisometry*bulkiness-1)",
    "lightdarkrowdist": "极亮点极暗点行坐标差",
    "highGrayDiffArea1": "高梯度差像素个数1",
    "maxDiameter": "最大直径",
    "innerHeight": "内接矩形高度",
    "innerRadius": "内接圆半径",
    "dist2baseContourRange": "缺陷到基准轮廓的距离范围",
    "dist2baseContourMean": "缺陷到基准轮廓的平均距离",
    "highGrayDiffArea3": "高梯度差像素个数3",
    "singleMaxregionRect2Len2": "最大缺陷宽度",
    "phi": "等价椭圆角度",
    "maxregionroundness": "最大连通域圆度",
    "flawLength": "缺陷长",
    "area": "缺陷的像素个数",
    "grayNeiRegionRange": "包含邻域的灰度范围",
    "lowGrayDiffArea3": "低梯度差像素个数3",
    "area2smallestRectRatio": "缺陷与最小外接矩形面积比例",
    "grayNeiRegionDeviation": "包含邻域的灰度方差",
    "grayNeiRegionMax": "包含邻域的灰度最大值",
    "grayNeiRegionlowGrayDiffArea3": "包含邻域低梯度差像素个数3",
    "roundness": "圆度1",
    "grayNeiRegionMean": "包含邻域的灰度平均值",
    "grayNeiRegionlowGrayDiffArea2": "包含邻域低梯度差像素个数2",
    "orientation": "区域方向",
    "edgeDensity": "边缘密度",
    "highgraydiffarea2defectareaRatio3": "高梯度差面积3占缺陷比例",
    "regionlinearity": "缺陷区域线性度",
    "rectangularity": "矩形度",
    "tworegionsarearatio": "两连通域面积比值",
    "areaHoles": "孔洞面积",
    "lowGrayDiffArea4": "低梯度差像素个数4",
    "grayNeiRegionlowGrayDiffArea1": "包含邻域低梯度差像素个数1",
    "highGrayDiffArea4": "高梯度差像素个数4",
    "regionmeanwidth": "缺陷平均宽度",
    "hight": "外接矩形高度",
    "grayNeiRegionRingMean": "缺陷邻域背景平均灰度",
    "grayPhi": "灰度椭圆角度",
    "flawWidth": "缺陷宽(深)",
    "porosity": "孔面积占比",
    "lowgraydiffarea2defectareaRatio2": "低梯度差面积2占缺陷比例",
    "anisometry": "异向性",
    "ra": "等价椭圆长轴长度",
    "grayMean": "灰度平均值",
    "grayNeiRegionhighGrayDiffArea2": "包含邻域高梯度差像素个数2",
    "compactness": "紧凑度",
    "lowGrayDiffArea5": "低梯度差像素个数5",
    "contlength": "轮廓长度",
    "rect2Phi": "最小外接矩形角度",
    "grayAnisotropy": "灰度异向性",
    "grayMin": "灰度最小值",
    "numSides": "近似多边形边个数",
    "numConnected": "独立区域个数",
    "grayDeviation": "灰度方差",
    "graymax": "灰度最大值",
    "distMean": "边缘到中心平均距离",
    "grayNeiRegionhighGrayDiffArea1": "包含邻域高梯度差像素个数1",
    "grayNeiRegionRingRange": "缺陷邻域灰度范围",
    "flawArea": "缺陷面积",
    "lowgraydiffarea2defectareaRatio3": "低梯度差面积3占缺陷比例",
    "highgraydiffarea2defectareaRatio4": "高梯度差面积4占缺陷比例",
    "lowgraydiffarea2defectareaRatio5": "低梯度差面积5占缺陷比例",
    "lowGrayDiffArea1": "低梯度差像素个数1",
    "bulkiness": "彭松度",
    "flawCustomType": "子类型",
    "grayArea": "灰度面积",
    "outerRadius": "外接圆半径",
    "lowgraydiffarea2defectareaRatio1": "低梯度差面积1占缺陷比例 ",
    "rb": "等价椭圆短轴长度",
    "distDeviation": "边缘中心距离方差",
    "convexity": "凸包性",
    "highgraydiffarea2defectareaRatio1": "高梯度差面积1占缺陷比例",
    "singleMaxregionRect2Len1": "最大缺陷长度",
    "grayRa": "灰度椭圆长轴长度",
    "grayEntrophy": "灰度熵",
    "circularity": "圆度2",
    "width": "外接矩形宽度",
    "highGrayDiffArea2": "高梯度差像素个数2",
    "eulerNumber": "欧拉个数",
}


def get_rule_json(json_file):
    """
    Reads a JSON file and returns the parsed data.

    Args:
        json_file (str): The path to the JSON file.

    Returns:
        dict: The parsed JSON data.
    """
    with open(json_file, "r", encoding="utf-8") as file:
        json_data = json.load(file)
    return json_data


def parse_csv(df):

    res = df.to_json(orient="records")
    parsed = json.loads(res)
    return parsed
