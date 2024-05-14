import os
from dotenv import load_dotenv, find_dotenv

# 读取本地/项目的环境变量。

# find_dotenv()寻找并定位.env文件的路径
# load_dotenv()读取该.env文件，并将其中的环境变量加载到当前的运行环境中
# 如果你设置的是全局的环境变量，这行代码则没有任何作用。
_ = load_dotenv(find_dotenv())

# 获取环境变量
spark_app_id = os.environ["SPARK_APPID"]
spark_api_key = os.environ["SPARK_API_KEY"]
spark_api_secret = os.environ["SPARK_API_SECRET"]
zhipuai_api_key = os.environ["ZHIPUAI_API_KEY"]
