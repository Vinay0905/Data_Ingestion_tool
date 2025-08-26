"""
Agent workflow and state definitions.
"""
from typing import TypedDict
import pandas as pd

from utilits.model_loader import ModelLoader
from langgraph.graph import StateGraph, MessagesState, END, START
from tools.Typedict_state import AgentState
from tools.Ingest_clean_data import DataProcessingTools
from tools.Validation_cleaning_data import clean_and_validate_data, validate_data
from tools.First_ingest_clean import Ingest_clean_data
from utilits.save_document import ExcelSaver

# The `AgentState` class is a TypedDict containing file paths, raw data, cleaned data, exceptions, and
# agent outcomes.
# class AgentState(TypedDict):
#     file_path: str
#     raw_data: pd.DataFrame
#     cleaned_data: pd.DataFrame
#     exceptions: pd.DataFrame
#     agent_outcome: str


# Place AgentState and workflow setup here.
class GraphBuilder:

    def __init__(self, model_provider: str = 'openai'):
        self.model_loader = ModelLoader(model_provider=model_provider)
        self.llm = self.model_loader.load_llm()
        self.data_tools = DataProcessingTools()
        self.saver = ExcelSaver()
        self.system_prompt = "You are a helpful data agent."





    def agent_function(self, state: dict):
        """Main agent function to run the workflow graph."""
        # state should be a dict with at least 'file_path'
        workflow = self.build_graph()
        result = workflow.invoke(state)
        return result
    




    def ingest_data(self, state: AgentState):
       
        return Ingest_clean_data(state)

    def clean_and_validate_data(self, state: AgentState):
        return clean_and_validate_data(state)

    def validate_data(self, state: AgentState):
        return validate_data(state)

    def save_results(self, state: AgentState):
        return self.saver.save_results(state)

    def build_graph(self):
        workflow = StateGraph(AgentState)
        workflow.add_node("ingest_data", self.ingest_data)
        workflow.add_node("clean_and_validate_data", self.clean_and_validate_data)
        workflow.add_node("validate_data", self.validate_data)
        workflow.add_node("save_results", self.save_results)
        workflow.set_entry_point("ingest_data")
        workflow.add_edge("ingest_data", "clean_and_validate_data")
        workflow.add_edge("clean_and_validate_data", "validate_data")
        workflow.add_conditional_edges(
            "validate_data",
            lambda state: "is_valid" if state.get("is_valid") else "needs_reprocessing",
            {
                "is_valid": "save_results",
                "needs_reprocessing": "clean_and_validate_data"
            }
        )
        workflow.add_edge("save_results", END)
        self.app = workflow.compile()
        return self.app
    



    def __call__(self):

        return self.build_graph()