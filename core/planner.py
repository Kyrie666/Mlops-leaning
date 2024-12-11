from langchain_core.prompts import ChatPromptTemplate


def create_planner(prompt, model):
    prompt_template = ChatPromptTemplate.from_messages([{"user": prompt}])
    planner = prompt_template | model
    return planner
