# main.ipynb

# 1. Install necessary libraries
# You can run this cell once to install everything.
# !pip install langchain langgraph langchain-community langchain-openai python-dotenv pandas Office365-REST-Python-Client

# 2. Import Libraries
import os
from dotenv import load_dotenv
import pandas as pd
from typing import TypedDict, Annotated, Sequence
import operator
from langchain_core.messages import BaseMessage, HumanMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END

# 3. Load Environment Variables
# Create a .env file in your project directory and add your OpenRouter API key:
# OPENROUTER_API_KEY="your_key_here"
load_dotenv()

# It's a good practice to check if the key is loaded
if "OPENROUTER_API_KEY" not in os.environ:
    print("OpenRouter API key not found. Please add it to your .env file.")

# 4. Set up the Language Model using OpenRouter
# We will use the ChatOpenAI class, but point it to the OpenRouter API.
# You can choose a free model from the OpenRouter documentation.
llm = ChatOpenAI(
    base_url="https://openrouter.ai/api/v1",
    api_key=os.environ["OPENROUTER_API_KEY"],
    model="mistralai/mistral-7b-instruct:free", # Example of a free model
    temperature=0,
)

class DataProcessingTools:
    def ingest_and_clean_data(self, file_path: str, cleaning_instructions: dict = None) -> pd.DataFrame:
        """
        Ingests data from a file and performs cleaning operations.
        Handles format detection and basic cleaning automatically.
        Applies cleaning_instructions when provided from validation feedback.
        """
        try:
            file_ext = file_path.split('.')[-1].lower()
            if file_ext == 'csv':
                df = pd.read_csv(file_path)
            elif file_ext in ['xlsx', 'xls']:
                df = pd.read_excel(file_path)
            else:
                raise ValueError(f"Unsupported file format: {file_ext}")

            # Basic cleaning
            df = df.drop_duplicates().rename(columns=str.lower).dropna(how='all')
            
            # Apply validation feedback instructions if provided
            if cleaning_instructions:
                if 'rename_columns' in cleaning_instructions:
                    df = df.rename(columns=cleaning_instructions['rename_columns'])
                if 'drop_columns' in cleaning_instructions:
                    df = df.drop(columns=cleaning_instructions['drop_columns'], errors='ignore')
                if 'fill_na' in cleaning_instructions:
                    df = df.fillna(cleaning_instructions['fill_na'])
            
            return df
        except Exception as e:
            print(f"Data processing error: {e}")
            raise

    def validate_data(self, dataframe: pd.DataFrame) -> dict:
        """
        Validates DataFrame against quality rules and returns feedback.
        """
        feedback = {}
        issues = []
        
        # Check for missing critical columns
        required_columns = ['sale_id', 'sale_amount', 'customer_name']
        missing = [col for col in required_columns if col not in dataframe.columns]
        if missing:
            issues.append(f"Missing critical columns: {', '.join(missing)}")
            feedback['rename_columns'] = {
                old: new for old, new in zip(dataframe.columns, required_columns)
            }

        # Check for missing values in critical fields
        if 'sale_amount' in dataframe.columns:
            null_sales = dataframe['sale_amount'].isnull().sum()
            if null_sales > 0:
                issues.append(f"{null_sales} records missing sale amounts")
                feedback['fill_na'] = {'sale_amount': 0}

        # Check for duplicate sale IDs
        if 'sale_id' in dataframe.columns:
            duplicates = dataframe.duplicated('sale_id').sum()
            if duplicates > 0:
                issues.append(f"{duplicates} duplicate sale IDs found")
                feedback['drop_columns'] = ['duplicate_flag']

        return {
            "is_valid": len(issues) == 0,
            "feedback": feedback if issues else {},
            "issues": issues
        }

# 5. Define the State for our Graph
# The state will hold the data as it passes through the graph.
class AgentState(TypedDict):
    file_path: str
    raw_data: pd.DataFrame
    cleaned_data: pd.DataFrame
    exceptions: pd.DataFrame
    agent_outcome: str

# 6. Define the Nodes of our Graph (The Agent's Tools)

def ingest_data(state: AgentState):
    """
    Node to ingest data from a local file path.
    In a real-world scenario, this could be extended to fetch from FTP or SharePoint.
    """
    print("---INGESTING DATA---")
    file_path = state.get('file_path')
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

def save_results(state: AgentState):
    """
    Node to save the final, processed data.
    """
    print("---SAVING RESULTS---")
    cleaned_df = state.get('cleaned_data')
    exceptions_df = state.get('exceptions')

    if cleaned_df is not None:
        cleaned_df.to_csv("data/output/cleaned_data.csv", index=False)
        print("Cleaned data saved.")

    if exceptions_df is not None:
        exceptions_df.to_csv("data/exceptions/exceptions.csv", index=False)
        print("Exceptions saved.")

    return {"agent_outcome": "Processing complete."}

# 7. Define the Graph
workflow = StateGraph(AgentState)

# Add the nodes
workflow.add_node("ingest_data", ingest_data)
workflow.add_node("clean_and_validate_data", clean_and_validate_data)
workflow.add_node("validate_data", validate_data)
workflow.add_node("save_results", save_results)

# Set the entry and end points
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
app = workflow.compile()


# 8. Run the Agent
# To run this, you'll first need to create a sample CSV file.
# For example, create a file named 'sample_data.csv' in the 'INPUT_FILES/' directory.
#
# sample_data.csv:
# Sale ID,Customer Name,Sale Amount
# 1,John Doe,100
# 2,Jane Smith,
# 3,John Doe,100
# 4,Peter Jones,250

# The input to the app is a dictionary with the initial state.
inputs = {"file_path": "INPUT_FILES/sample_data.csv"}
result = app.invoke(inputs)

print("\n---FINAL OUTCOME---")
print(result.get('agent_outcome'))
