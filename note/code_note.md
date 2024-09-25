# note for coding
1. AzureOpenAI 的调用方式
``` python
  client = AzureOpenAI(
        azure_endpoint=os.getenv("AZURE_OPENAI_ENDPOINT"),
        api_key=os.getenv("AZURE_OPENAI_API_KEY"),
        api_version="2024-02-01",
    )
```
2. AzureOpenAI tool 的调用方式
  - First API call: Ask the model to use the function
  - Handle function calls
  - Second API call: Get the final response from the model


3. 学习langchain框架的使用
  - LANGCHAIN_API_KEY 注册，真麻烦，每个平台注册一个；
  - from langchain_chroma import Chroma 需要sqlite3 直接安装版本不够，需要通过源码编译；
    - 居然还遇到了兼容性问题
    ``` python
    __import__('pysqlite3')
    import sys
    sys.modules['sqlite3'] = sys.modules.pop('pysqlite3')
    ```
  - vectorstore 这里，由于我没有部署embedding的模型，就暂时略过了；
  - add tools for model:这里需要将工具函数注释写的比较完整，效果才会好；也可以使用pydantic 来定义函数，这样就可以自动生成函数的schema；
    ```python
      # The function name, type hints, and docstring are all part of the tool
      # schema that's passed to the model. Defining good, descriptive schemas
      # is an extension of prompt engineering and is an important part of
      # getting models to perform well.
      @tool
      def add(a: int, b: int) -> int:
          """Add two integers.

          Args:
              a: First integer
              b: Second integer
          """
          return a + b


      from langchain_core.pydantic_v1 import BaseModel, Field
      class add(BaseModel):
          """Add two integers."""

          a: int = Field(..., description="First integer")
          b: int = Field(..., description="Second integer")


      class multiply(BaseModel):
          """Multiply two integers."""

          a: int = Field(..., description="First integer")
          b: int = Field(..., description="Second integer")
          ```
  - Message list 流转方式:
    - [HumanMessage]  # 输入
    - [HumanMessage-AImessage] # invoke
    - [HumanMessage-AImessage-ToolMessage] # tool 调用
    - AIMessage # 最后的回答



## llm compiler框架：
1. 没有添加agent的选项吗？？？——看下源代码
2. 找到了plan不成功的原因：没有为工具添加合适的description——找下合适的写法；