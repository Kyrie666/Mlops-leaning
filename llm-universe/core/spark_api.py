from config.setting import spark_api_secret, spark_api_key, spark_app_id
from sparkai.llm.llm import ChatSparkLLM, ChunkPrintHandler
from sparkai.core.messages import ChatMessage


def gen_spark_params(model) -> dict[str, str]:
    """
    构造星火模型请求参数
    """

    spark_url_tpl = "wss://spark-api.xf-yun.com/{}/chat"
    model_params_dict: dict[str, dict[str, str]] = {
        # v1.5 版本
        "v1.5": {
            "domain": "general",  # 用于配置大模型版本
            "spark_url": spark_url_tpl.format("v1.1"),  # 云端环境的服务地址
        },
        # v2.0 版本
        "v2.0": {
            "domain": "generalv2",  # 用于配置大模型版本
            "spark_url": spark_url_tpl.format("v2.1"),  # 云端环境的服务地址
        },
        # v3.0 版本
        "v3.0": {
            "domain": "generalv3",  # 用于配置大模型版本
            "spark_url": spark_url_tpl.format("v3.1"),  # 云端环境的服务地址
        },
        # v3.5 版本
        "v3.5": {
            "domain": "generalv3.5",  # 用于配置大模型版本
            "spark_url": spark_url_tpl.format("v3.5"),  # 云端环境的服务地址
        },
    }
    return model_params_dict[model]


def gen_spark_messages(prompt) -> list[ChatMessage]:
    """
    构造星火模型请求参数 messages

    请求参数：
        prompt: 对应的用户提示词
    """

    messages: list[ChatMessage] = [ChatMessage(role="user", content=prompt)]
    return messages


def get_completion(prompt, model="v3.5", temperature=0.1):
    """
    获取星火模型调用结果

    请求参数：
        prompt: 对应的提示词
        model: 调用的模型，默认为 v3.5，也可以按需选择 v3.0 等其他模型
        temperature: 模型输出的温度系数，控制输出的随机程度，取值范围是 0~1.0，且不能设置为 0。温度系数越低，输出内容越一致。
    """

    spark_llm = ChatSparkLLM(
        spark_api_url=gen_spark_params(model)["spark_url"],  # 云端环境的服务地址
        spark_app_id=spark_app_id,
        spark_api_key=spark_api_key,
        spark_api_secret=spark_api_secret,  # api 配置
        spark_llm_domain=gen_spark_params(model)["domain"],  # 大模型版本
        temperature=temperature,
        streaming=False,
    )

    # 设计prompt 这部分根据具体的业务场景来设计
    #     prompt = f"""
    # 请生成包括书名、作者和类别的三本虚构的、非真实存在的中文书籍清单，\
    # 并以 JSON 格式提供，其中包含以下键:book_id、title、author、genre。
    # """

    messages = gen_spark_messages(prompt)
    handler = ChunkPrintHandler()
    # 当 streaming设置为 False的时候, callbacks 并不起作用
    resp = spark_llm.generate([messages], callbacks=[handler])
    return resp
