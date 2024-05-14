# 要实现自定义 Embeddings，需要定义一个自定义类继承自 LangChain 的 Embeddings 基类，
# 然后定义两个函数：① embed_query 方法，用于对单个字符串（query）进行 embedding；
# ②embed_documents 方法，用于对字符串列表（documents）进行 embedding。

from __future__ import annotations
import logging
from typing import Dict, List, Any
from langchain.embeddings.base import Embeddings
from langchain.pydantic_v1 import BaseModel, root_validator

logger = logging.getLogger(__name__)


class ZhipuAIEmbeddings(BaseModel, Embeddings):
    """`Zhipuai Embeddings` embedding models."""

    client: Any
    """`zhipuai.ZhipuAI"""

    @root_validator()
    def validate_environment(cls, values: Dict) -> Dict:
        """
        实例化ZhipuAI为values["client"]

        Args:

            values (Dict): 包含配置信息的字典，必须包含 client 的字段.
        Returns:

            values (Dict): 包含配置信息的字典。如果环境中有zhipuai库，则将返回实例化的ZhipuAI类；否则将报错 'ModuleNotFoundError: No module named 'zhipuai''.
        """
        from zhipuai import ZhipuAI

        values["client"] = ZhipuAI()
        return values

    def embed_query(self, query: str) -> List[float]:
        embeddings = self.client.embeddings.create(model="embedding-2", input=query)
        return embeddings.data[0].embedding

    def embed_documents(self, documents: List[str]) -> List[List[float]]:
        return [self.embed_query(text) for text in documents]

    async def aembed_documents(self, texts: List[str]) -> List[List[float]]:
        """Asynchronous Embed search docs."""
        raise NotImplementedError(
            "Please use `embed_documents`. Official does not support asynchronous requests"
        )

    async def aembed_query(self, text: str) -> List[float]:
        """Asynchronous Embed query text."""
        raise NotImplementedError(
            "Please use `aembed_query`. Official does not support asynchronous requests"
        )
