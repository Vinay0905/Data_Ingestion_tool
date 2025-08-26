import pandas as pd








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