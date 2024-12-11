from langchain_openai import AzureChatOpenAI
from langchain_openai import ChatOpenAI
import os


def get_model(type: str = "openai"):
    if type == "openai":
        llm = ChatOpenAI(
            model=os.environ["MODEL_NAME"],
            temperature=0.0,
            openai_api_key=os.environ["OPENAI_API_KEY"],
            openai_api_base=os.environ["OPENAI_API_BASE"],
        )
        return llm
    else:
        raise NotImplementedError


model = get_model()
