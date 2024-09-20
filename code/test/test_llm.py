from openai import AzureOpenAI
import os
from dotenv import load_dotenv

load_dotenv()


def get_response():

    client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version="2024-02-01",
    )

    response = client.chat.completions.create(
        model="vst_gpt4o",  # model = "deployment_name".
        messages=[
            {
                "role": "system",
                "content": "你是一个专注于软件开发的编程助手。你可以帮助解决编程问题。",
            },
            {
                "role": "user",
                "content": "我有一个关于 Python 编程的问题。我在尝试使用列表理解来创建一个包含 1 到 10 的所有偶数的列表。我试过 `[x for x in range(1, 11) if x % 2 == 0]`，但是结果不符合预期。我应该如何修改我的代码？",
            },
            {
                "role": "assistant",
                "content": "你的代码已经很接近了，但是你需要将 `range(1, 11)` 改为 `range(1, 11)`，这样才能包括 10。下面是修改后的代码：```python even_numbers = [x for x in range(1, 11) if x % 2 == 0] print(even_numbers)```",
            },
        ],
    )

    print(response.choices[0].message.content)


if __name__ == "__main__":
    get_response()
