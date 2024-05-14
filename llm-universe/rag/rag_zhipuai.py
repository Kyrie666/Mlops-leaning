from config.zhipuai_llm import ZhipuAILLM
from config.setting import zhipuai_api_key
from config.zhipu_embedding import ZhipuAIEmbeddings
from langchain.vectorstores.chroma import Chroma

api_key = zhipuai_api_key
zhipuai_model = ZhipuAILLM(model="chatglm_std", temperature=0, api_key=api_key)
# 调用模型
# zhipuai_model("你好，请你自我介绍一下！")

# 定义 Embeddings
embedding = ZhipuAIEmbeddings()

# 向量数据库持久化路径
persist_directory = "../data_base/vector_db/chroma"

# 加载数据库
vectordb = Chroma(
    persist_directory=persist_directory,  # 允许我们将persist_directory目录保存到磁盘上
    embedding_function=embedding,
)


from langchain.prompts import PromptTemplate

template = """使用以下上下文来回答最后的问题。如果你不知道答案，就说你不知道，不要试图编造答
案。最多使用三句话。尽量使答案简明扼要。总是在回答的最后说“谢谢你的提问！”。
{context}
问题: {question}
"""

QA_CHAIN_PROMPT = PromptTemplate(
    input_variables=["context", "question"], template=template
)


from langchain.chains import RetrievalQA

qa_chain = RetrievalQA.from_chain_type(
    zhipuai_model,
    retriever=vectordb.as_retriever(),
    return_source_documents=True,
    chain_type_kwargs={"prompt": QA_CHAIN_PROMPT},
)

# 添加历史对话的记忆功能
from langchain.memory import ConversationBufferMemory

# 参考 langchain 的 Memory 部分的相关文档。
memory = ConversationBufferMemory(
    memory_key="chat_history",  # 与 prompt 的输入变量保持一致。
    return_messages=True,  # 将以消息列表的形式返回聊天记录，而不是单个字符串
)


from langchain.chains import ConversationalRetrievalChain

retriever = vectordb.as_retriever()

qa = ConversationalRetrievalChain.from_llm(
    zhipuai_model, retriever=retriever, memory=memory
)
question = "我可以学习到关于提示工程的知识吗？"
result = qa({"question": question})
print(result["answer"])
