"""
Data ingestion and cleaning functions.
"""
import pandas as pd
from tools.Typedict_state import AgentState



# Place ingest_and_clean_data and ingest_data functions here.

import os

class Ingest_clean_data:
    @staticmethod
    def ingest_data(state: AgentState):
        """
        Node to ingest data from a local file path.
        In a real-world scenario, this could be extended to fetch from FTP or SharePoint.
        """
        print("---INGESTING DATA---")
        file_path = state.get('file_path')
        # Always use INPUT_FILES directory
        if not file_path.startswith("INPUT_FILES"):  # avoid double prefix
            file_path = os.path.join("INPUT_FILES", file_path)
        try:
            # Handle different file types
            file_ext = file_path.split('.')[-1].lower()
            if file_ext == 'csv':
                raw_df = pd.read_csv(file_path)
            elif file_ext in ['xlsx', 'xls']:
                raw_df = pd.read_excel(file_path)
            elif file_ext == 'json':
                raw_df = pd.read_json(file_path)
            elif file_ext == 'parquet':
                raw_df = pd.read_parquet(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")
            return {"raw_data": raw_df}
        except Exception as e:
            print(f"Error ingesting data: {e}")
            return {"agent_outcome": f"Failed to ingest data: {e}"}

