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
    def escalate_or_request_input(self, state: AgentState):
        """Escalate or request user input after repeated validation failures."""
        print("Validation failed multiple times. Escalating or requesting user input.")
        # You can add logic here to notify a user, log, or request new cleaning instructions
        return {"agent_outcome": "Validation failed multiple times. Please review the data or provide new cleaning instructions."}

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
        workflow.add_node("escalate_or_request_input", self.escalate_or_request_input)
        workflow.set_entry_point("ingest_data")
        workflow.add_edge("ingest_data", "clean_and_validate_data")
        workflow.add_edge("clean_and_validate_data", "validate_data")

        # Add a counter to state to track validation attempts
        def validation_decision(state):
            if state.get("is_valid"):
                return "is_valid"
            # Track number of validation attempts
            attempts = state.get("validation_attempts", 0) + 1
            state["validation_attempts"] = attempts
            if attempts >= 2:
                return "escalate"
            return "needs_reprocessing"

        workflow.add_conditional_edges(
            "validate_data",
            validation_decision,
            {
                "is_valid": "save_results",
                "needs_reprocessing": "clean_and_validate_data",
                "escalate": "escalate_or_request_input"
            }
        )
        workflow.add_edge("save_results", END)
        workflow.add_edge("escalate_or_request_input", END)
        self.app = workflow.compile()
        return self.app
    





    def __call__(self):

        return self.build_graph()