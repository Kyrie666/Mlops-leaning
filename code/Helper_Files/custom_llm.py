from typing import Any, Dict, Iterator, List, Mapping, Optional
from langchain_core.callbacks.manager import CallbackManagerForLLMRun
from langchain_core.language_models.llms import LLM
from langchain_core.outputs import GenerationChunk


class Qwen(LLM):
    """一个自定义的大模型。

    当为LangChain贡献实现时，仔细记录包括初始化参数在内的模型，
    包括如何初始化模型的示例以及包括任何相关的
    链接到基础模型文档或API。
    """

    api_base: str = None
    api_key: str = None
    max_token: int = 128 * 1000
    temperature: float = 0.01
    top_p: float = 0.9
    history_len: int = 3

    def __init__(self, **kwargs):
        super().__init__()
        self.api_base = kwargs.get("api_base")
        self.api_key = kwargs.get("api_key")

    def _call(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> str:
        """在给定输入上运行LLM。
        重写此方法以实现LLM逻辑。

        参数:
            prompt: 用于生成的提示。
            stop: 生成时使用的停用词。 模型输出在任何停止子串的第一次出现时被截断。
                如果不支持停用词，请考虑引发NotImplementedError。
            run_manager: 运行的回调管理器。
            **kwargs: 任意额外的关键字参数。 通常传递给模型提供者API调用。

        返回:
            模型输出作为字符串。 实际完成不应包括提示。
        """
        if stop is not None:
            raise ValueError("不允许使用停用词参数。")

        return prompt[: self.n]

    def _stream(
        self,
        prompt: str,
        stop: Optional[List[str]] = None,
        run_manager: Optional[CallbackManagerForLLMRun] = None,
        **kwargs: Any,
    ) -> Iterator[GenerationChunk]:
        """在给定提示上流式传输LLM。

        应该由支持流传输的子类重写此方法。

        如果没有实现，对流进行调用的默认行为将是退回到模型的非流版本并返回
        作为单个块的输出。

        参数:
            prompt: 生成的提示。
            stop: 生成时使用的停用词。 模型输出在任何这些子字符串的第一次出现时被截断。
            run_manager: 运行的回调管理器。
            **kwargs: 任意额外的关键字参数。 通常传递给模型提供者API调用。

        返回:
            一个GenerationChunks的迭代器。
        """
        for char in prompt[: self.n]:
            chunk = GenerationChunk(text=char)
            if run_manager:
                run_manager.on_llm_new_token(chunk.text, chunk=chunk)

            yield chunk

    @property
    def _llm_type(self) -> str:
        return "Qwen"

    @property
    def _history_len(self) -> int:
        return self.history_len

    def set_history_len(self, history_len: int = 10) -> None:
        self.history_len = history_len

    @property
    def _identifying_params(self) -> Mapping[str, Any]:
        """Get the identifying parameters."""
        return {
            "max_token": self.max_token,
            "temperature": self.temperature,
            "top_p": self.top_p,
            "history_len": self.history_len,
        }
