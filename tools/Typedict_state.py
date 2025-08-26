from typing import TypedDict
import pandas as pd




class AgentState(TypedDict):
    file_path: str
    raw_data: pd.DataFrame
    cleaned_data: pd.DataFrame
    exceptions: pd.DataFrame
    agent_outcome: str