from langchain.document_loaders.pdf import PyMuPDFLoader
import re

# PyMuPDFLoader 是 PDF 解析器中速度最快的一种，结果会包含 PDF 及其页面的详细元数据，并且每页返回一个文档。

loader = PyMuPDFLoader("../data_base/pumpkin_book.pdf")
pdf_pages = loader.load()
pdf_page = pdf_pages[1]
print(
    f"每一个元素的类型：{type(pdf_page)}.",
    f"该文档的描述性数据：{pdf_page.metadata}",
    f"查看该文档的内容:\n{pdf_page.page_content}",
    sep="\n------\n",
)
# 读取 Markdown 文件
# from langchain.document_loaders.markdown import UnstructuredMarkdownLoader

# loader = UnstructuredMarkdownLoader("../../data_base/knowledge_db/prompt_engineering/1. 简介 Introduction.md")
# md_pages = loader.load()


# 上文中读取的pdf文件不仅将一句话按照原文的分行添加了换行符\n，
# 也在原本两个符号中间插入了\n，我们可以使用正则表达式匹配并删除掉\n。

pattern = re.compile(r"[^\u4e00-\u9fff](\n)[^\u4e00-\u9fff]", re.DOTALL)
pdf_page.page_content = re.sub(
    pattern, lambda match: match.group(0).replace("\n", ""), pdf_page.page_content
)
print(pdf_page.page_content)
# 去除不需要的符号
pdf_page.page_content = pdf_page.page_content.replace("•", "")
pdf_page.page_content = pdf_page.page_content.replace(" ", "")
print(pdf_page.page_content)


# 由于单个文档的长度往往会超过模型支持的上下文，需要对文档进行分割
# Langchain 中文本分割器都根据 chunk_size (块大小)和 chunk_overlap (块与块之间的重叠大小)进行分割。
""" 
* RecursiveCharacterTextSplitter 递归字符文本分割
RecursiveCharacterTextSplitter 将按不同的字符递归地分割(按照这个优先级["\n\n", "\n", " ", ""])，
    这样就能尽量把所有和语义相关的内容尽可能长时间地保留在同一位置
RecursiveCharacterTextSplitter需要关注的是4个参数：

* separators - 分隔符字符串数组
* chunk_size - 每个文档的字符数量限制
* chunk_overlap - 两份文档重叠区域的长度
* length_function - 长度计算函数
"""
# 导入文本分割器
from langchain.text_splitter import RecursiveCharacterTextSplitter

# 知识库中单段文本长度
CHUNK_SIZE = 500

# 知识库中相邻文本重合长度
OVERLAP_SIZE = 50
# 使用递归字符文本分割器
text_splitter = RecursiveCharacterTextSplitter(
    chunk_size=CHUNK_SIZE, chunk_overlap=OVERLAP_SIZE
)
text_splitter.split_text(pdf_page.page_content[0:1000])
split_docs = text_splitter.split_documents(pdf_pages)
print(f"切分后的文件数量：{len(split_docs)}")
print(
    f"切分后的字符数（可以用来大致评估 token 数）：{sum([len(doc.page_content) for doc in split_docs])}"
)
