
from tools.Typedict_state import AgentState



class ExcelSaver:
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