# langgraph学习
## 核心流程
1. 状态保存方法
2. 定义模型
3. 创建图,添加节点
4. 设置图的Start和End
5. 编译图

## Enhancing the Chatbot with Tools
1. langchain 工具链:https://python.langchain.com/v0.2/docs/integrations/tools/
2. define tool:工具可直接使用,这应该是由于这是正在维护的tool
3. 将工具添加到图中
4. 设置tool 的condition edge
5. 设置其他节点,并编译;

## Adding Memory to the Chatbot
1. 通过配置checkpoint,可以保存状态;

## Human-in-the-loop
1. 通过add interruption 可以添加人工干预;

## Manually Updating the State

