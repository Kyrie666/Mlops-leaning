from langgraph.graph import END, StateGraph, START
from langgraph.graph.message import add_messages
from typing import TypedDict, Annotated

from langchain_core.messages import AIMessage
from task_fetching import plan_and_schedule
from joiner import joiner


class State(TypedDict):
    messages: Annotated[list, add_messages]


def construct_graph(plan_and_schedule, joiner):
    graph_builder = StateGraph(State)
    # 1.  Define vertices
    # We defined plan_and_schedule above already
    # Assign each node to a state variable to update
    graph_builder.add_node("plan_and_schedule", plan_and_schedule)
    graph_builder.add_node("join", joiner)
    ## Define edges
    graph_builder.add_edge("plan_and_schedule", "join")

    ### This condition determines looping logic
    def should_continue(state):
        messages = state["messages"]
        if isinstance(messages[-1], AIMessage):
            return END
        return "plan_and_schedule"

    graph_builder.add_conditional_edges(
        "join",
        # Next, we pass in the function that will determine which node is called next.
        should_continue,
    )

    graph_builder.add_edge(START, "plan_and_schedule")
    chain = graph_builder.compile()
    return chain


if __name__ == "__main__":
    chain = construct_graph(plan_and_schedule, joiner)
