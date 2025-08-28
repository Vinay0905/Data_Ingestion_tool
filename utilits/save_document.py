
from tools.Typedict_state import AgentState
import os
from utilits.Googledrive_api import upload_file_to_drive

class ExcelSaver:
    def save_results(state: AgentState, drive_folder_id=None):
        """
        Node to save the final, processed data and upload to Google Drive if folder_id is provided.
        """
        print("---SAVING RESULTS---")
        cleaned_df = state.get('cleaned_data')
        exceptions_df = state.get('exceptions')

        output_dir = "OUTPUT_FILES"
        os.makedirs(output_dir, exist_ok=True)
        cleaned_path = os.path.join(output_dir, "cleaned_data.csv")

        if cleaned_df is not None:
            cleaned_df.to_csv(cleaned_path, index=False)
            print(f"Cleaned data saved to {cleaned_path}.")
            # Upload to Google Drive if folder_id is provided
            if drive_folder_id:
                upload_file_to_drive(cleaned_path, drive_folder_id, new_name="cleaned_data_uploaded.csv")

        if exceptions_df is not None:
            exceptions_path = os.path.join(output_dir, "exceptions.csv")
            exceptions_df.to_csv(exceptions_path, index=False)
            print(f"Exceptions saved to {exceptions_path}.")

        return {"agent_outcome": "Processing complete."}