import pandas as pd





class Validate_data:
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