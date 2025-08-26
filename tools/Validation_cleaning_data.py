
from tools.Typedict_state import AgentState
import pandas as pd
from tools.Ingest_clean_data import DataProcessingTools
from utilits.model_loader import ModelLoader

# Initialize LLM model loader
llm = ModelLoader(model_provider="openai").load_llm()

def clean_and_validate_data(state: AgentState):
    """
    Node to clean and validate data using DataProcessingTools
    """
    print("---CLEANING DATA---")
    processing_tools = DataProcessingTools()
    cleaned_df = processing_tools.ingest_and_clean_data(
        state["file_path"], 
        state.get("cleaning_instructions")
    )
    return {"cleaned_data": cleaned_df}

def llm_validate_data(cleaned_df: pd.DataFrame) -> dict:
    """
    Uses the LLM to validate the cleaned DataFrame and generate feedback.
    """
    try:
        # Take a sample of the cleaned data for prompt brevity
        sample_json = cleaned_df.head(10).to_json()
        prompt = f"""
You are a data quality expert. Given the following data sample (in JSON), identify any data quality issues (missing columns, missing values, duplicates, etc.) and suggest cleaning steps:
{sample_json}
"""
        llm_feedback = llm.invoke(prompt)
        print("---LLM VALIDATION FEEDBACK---")
        print(llm_feedback.content)
        # Optionally, you can parse the LLM response for structured feedback
        return {
            "is_valid": "no issues" in llm_feedback.content.lower(),
            "llm_feedback": llm_feedback.content
        }
    except Exception as e:
        print(f"LLM validation error: {e}")
        return {
            "is_valid": False,
            "llm_feedback": f"LLM validation failed: {e}"
        }

def validate_data(state: AgentState):
    """
    Node to validate data and determine next steps using LLM-based validation.
    """
    print("---VALIDATING DATA WITH LLM---")
    cleaned_df = state["cleaned_data"]
    llm_result = llm_validate_data(cleaned_df)
    if llm_result["is_valid"]:
        return {"agent_outcome": "Validation successful (LLM)", **llm_result}
    return {"agent_outcome": "Validation failed (LLM)", **llm_result}