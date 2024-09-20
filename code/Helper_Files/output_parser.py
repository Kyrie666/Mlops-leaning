
# Import necessary modules and types
import ast
import re
from typing import (
    Any,
    Dict,
    Iterator,
    List,
    Optional,
    Sequence,
    Tuple,
    Union,
)

from langchain_core.exceptions import OutputParserException
from langchain_core.messages import BaseMessage
from langchain_core.output_parsers.transform import BaseTransformOutputParser
from langchain_core.runnables import RunnableConfig
from langchain_core.tools import BaseTool
from typing_extensions import TypedDict

# Define regular expression patterns for thought, action, and placeholder
THOUGHT_PATTERN = r"Thought: ([^\n]*)"
ACTION_PATTERN = r"\n*(\d+)\. (\w+)\((.*)\)(\s*#\w+\n)?"
# $1 or ${1} -> 1
ID_PATTERN = r"\$\{?(\d+)\}?"
# Define the end-of-plan marker
END_OF_PLAN = "<END_OF_PLAN>"


### Helper functions

# Parse a string to Python object using ast.literal_eval, fallback to the original string
def _ast_parse(arg: str) -> Any:
    try:
        return ast.literal_eval(arg)
    except:  # noqa
        return arg


# Parse arguments from a string for a specific tool
def _parse_llm_compiler_action_args(args: str, tool: Union[str, BaseTool]) -> list[Any]:
    """Parse arguments from a string."""
    if args == "":
        return ()
    # If the tool is a string, return empty list, as it indicates tool without arguments
    if isinstance(tool, str):
        return ()
    # Initialize a dictionary to hold extracted arguments
    extracted_args = {}
    tool_key = None
    prev_idx = None
    # Iterate through the keys of the tool's argument dictionary
    for key in tool.args.keys():
        # Split if present
        if f"{key}=" in args:
            idx = args.index(f"{key}=")
            # If prev_idx is not none, parse previous arguments
            if prev_idx is not None:
                extracted_args[tool_key] = _ast_parse(
                    args[prev_idx:idx].strip().rstrip(",")
                )
            # Update arguments, tool key, and prev_idx
            args = args.split(f"{key}=", 1)[1]
            tool_key = key
            prev_idx = 0
    # Parse final arguments if prev_idx is not none
    if prev_idx is not None:
        extracted_args[tool_key] = _ast_parse(
            args[prev_idx:].strip().rstrip(",").rstrip(")")
        )
    # Return list of extracted arguments
    return extracted_args


# Define default dependency rule
def default_dependency_rule(idx, args: str):
    matches = re.findall(ID_PATTERN, args)
    numbers = [int(match) for match in matches]
    return idx in numbers


# Get dependencies from a graph
def _get_dependencies_from_graph(
    idx: int, tool_name: str, args: Dict[str, Any]
) -> dict[str, list[str]]:
    """Get dependencies from a graph."""
    if tool_name == "join":
        return list(range(1, idx))
    return [i for i in range(1, idx) if default_dependency_rule(i, str(args))]


# Define a typed dictionary for Task
class Task(TypedDict):
    idx: int
    tool: BaseTool
    args: list
    dependencies: Dict[str, list]
    thought: Optional[str]


# Instantiate a task from given parameters
def instantiate_task(
    tools: Sequence[BaseTool],
    idx: int,
    tool_name: str,
    args: Union[str, Any],
    thought: Optional[str] = None,
) -> Task:
    if tool_name == "join":
        tool = "join"
    else:
        try:
            # Find the tool by its name in the list of tools
            tool = tools[[tool.name for tool in tools].index(tool_name)]
        except ValueError as e:
            # If the tool is not found, raise an exception
            raise OutputParserException(f"Tool {tool_name} not found.") from e
    # Parse the arguments for the tool
    tool_args = _parse_llm_compiler_action_args(args, tool)
    # Get the dependencies for the task
    dependencies = _get_dependencies_from_graph(idx, tool_name, tool_args)

    return Task(
        idx=idx,
        tool=tool,
        args=tool_args,
        dependencies=dependencies,
        thought=thought,
    )


class LLMCompilerPlanParser(BaseTransformOutputParser[dict], extra="allow"):
    """Planning output parser."""

    tools: List[BaseTool]

    def _transform(self, input: Iterator[Union[str, BaseMessage]]) -> Iterator[Task]:
        # Initialize an empty list to hold the text chunks
        texts = []
        # Initialize the thought to None
        thought = None
        # Iterate over each chunk in the input
        for chunk in input:
            # Assume input is str. TODO: support vision/other formats
            text = chunk if isinstance(chunk, str) else str(chunk.content)
            # Ingest the token and get the task and thought
            for task, thought in self.ingest_token(text, texts, thought):
                yield task
        # Final possible task
        if texts:
            # Parse the task from the concatenated texts and the thought
            task, _ = self._parse_task("".join(texts), thought)
            if task:
                yield task

    def parse(self, text: str) -> List[Task]:
        return list(self._transform([text]))

    def stream(
        self,
        input: str | BaseMessage,
        config: RunnableConfig | None = None,
        **kwargs: Any | None,
    ) -> Iterator[Task]:
        yield from self.transform([input], config, **kwargs)

    def ingest_token(
        self, token: str, buffer: List[str], thought: Optional[str]
    ) -> Iterator[Tuple[Optional[Task], str]]:
        # Append the token to the buffer
        buffer.append(token)
        # If the token contains a newline character
        if "\n" in token:
            # Split the buffer into lines
            buffer_ = "".join(buffer).split("\n")
            # Get the last line
            suffix = buffer_[-1]
            # Iterate over each line in the buffer
            for line in buffer_[:-1]:
                # Parse the task from the line and the thought
                task, thought = self._parse_task(line, thought)
                if task:
                    # Yield the task and the thought
                    yield task, thought
            buffer.clear()
            buffer.append(suffix)

    def _parse_task(self, line: str, thought: Optional[str] = None):
        # Initialize the task to None
        task = None
        # If the line matches the thought pattern
        if match := re.match(THOUGHT_PATTERN, line):
            # Optionally, action can be preceded by a thought
            thought = match.group(1)
        # If the line matches the action pattern
        elif match := re.match(ACTION_PATTERN, line):
            # if action is parsed, return the task, and clear the buffer
            idx, tool_name, args, _ = match.groups()
            idx = int(idx)
            # Instantiate the task from the index, tool name, arguments, and thought
            task = instantiate_task(
                tools=self.tools,
                idx=idx,
                tool_name=tool_name,
                args=args,
                thought=thought,
            )
            thought = None
        # Else it is just dropped
        # Return the task and the thought
        return task, thought
