"""
Agent workflow and state definitions.
"""
from typing import TypedDict
import pandas as pd
from utilits.model_loader import ModelLoader
from langgraph.graph import StateGraph, MessagesState, END, START


# The `AgentState` class is a TypedDict containing file paths, raw data, cleaned data, exceptions, and
# agent outcomes.
class AgentState(TypedDict):
    file_path: str
    raw_data: pd.DataFrame
    cleaned_data: pd.DataFrame
    exceptions: pd.DataFrame
    agent_outcome: str


# Place AgentState and workflow setup here.
class GraphBuilder:
    def __init__(self,model_provider:str='openai'):
        self.model_loader=ModelLoader(model_provider=model_provider)
        self.llm=self.model_loader.load_llm()

        self.tools=[]




    def agent_function(self,state: MessagesState):
        """Main agent function"""
        user_question = state["messages"]
        input_question = [self.system_prompt] + user_question
        response = self.llm_with_tools.invoke(input_question)
        return {"messages": [response]}
    



    def build_graph(self):
        workflow = StateGraph(AgentState)
        workflow.add_node("ingest_data", self.ingest_data)
        workflow.add_node("clean_and_validate_data", self.clean_and_validate_data)
        workflow.add_node("validate_data", self.validate_data)
        workflow.add_node("save_results", self.save_results)
        workflow.set_entry_point("ingest_data")
        workflow.add_edge("ingest_data", "clean_and_validate_data")
        workflow.add_edge("clean_and_validate_data", "validate_data")
        # Conditional edge based on validation result
        workflow.add_conditional_edges(
            "validate_data",
            lambda state: "is_valid" if state.get("is_valid") else "needs_reprocessing",
            {
                "is_valid": "save_results",
                "needs_reprocessing": "clean_and_validate_data"
            }
        )
        workflow.add_edge("save_results", END)
        # Compile the graph into a runnable app
        self.app = workflow.compile()
        return self.app
    



    def __call__(self):

        return self.build_graph()