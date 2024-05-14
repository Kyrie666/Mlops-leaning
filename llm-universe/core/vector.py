from zhipu_embedding import ZhipuAIEmbeddings
from operate_data import split_docs

embedding = ZhipuAIEmbeddings()
# 定义持久化路径

persist_directory = "../data_base/vector_db/chroma"
from langchain.vectorstores.chroma import Chroma

vectordb = Chroma.from_documents(
    documents=split_docs[
        :20
    ],  # 为了速度，只选择前 20 个切分的 doc 进行生成；使用千帆时因QPS限制，建议选择前 5 个doc
    embedding=embedding,
    persist_directory=persist_directory,  # 允许我们将persist_directory目录保存到磁盘上
)
# 持久化向量数据库
vectordb.persist()


# 向量检索
# 余弦相似度
# 举例：
question = "什么是大语言模型"

sim_docs = vectordb.similarity_search(question, k=3)
print(f"检索到的内容数：{len(sim_docs)}")

# 最大边际相关性 (MMR, Maximum marginal relevance)
# 核心思想是在已经选择了一个相关性高的文档之后，再选择一个与已选文档相关性较低但是信息丰富的文档。这样可以在保持相关性的同时，增加内容的多样性，避免过于单一的结果。
mmr_docs = vectordb.max_marginal_relevance_search(question, k=3)
