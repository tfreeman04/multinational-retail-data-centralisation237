import pandas as pd
class DataCleaning:

    def __init__(self):
        pass

    def clean_user_data(self,df):
        df.remove_empty_columns()
        df.handle_null_values()

        return df
    
    def clean_card_data(self,df):
        df.remove_empty_columns()
        df.handle_null_values()

        return df
    


    def remove_empty_columns(self, df):
        ''' 
        HANDLE NULL values in the Dataframe Columns
        - this will drop a whole column if it only has NULL values
        param : df
        return : the dataframe without any empty columns
        '''
        df = df_drop = df.dropna(axis=1, how='all')
        
        return df
    
    def calculate_empty_values(self, df):
        '''
        This method will calculate how many columns have completely NULL values
        param: df
        return df containing percentages of columns that include all NULL values. 
        
        '''
        # find out how many columns have null values 
        percentage_of_empty_values = self.df.isna().mean() * 100
        return percentage_of_empty_values

    def handle_null_values(self, df):
        """
        Handle NULL values in the DataFrame.
        - Fill or drop NULL values based on specific column logic.

        :param df: The DataFrame to handle NULL values for.
        :return: The DataFrame with handled NULL values.
        """
        # Example: Fill missing values in 'name' column with 'Unknown'
        df['first_name'].fillna('Unknown', inplace=True)

        # Example: Drop rows where 'email' is NULL
        df.dropna(subset=['email_address'], inplace=True)

        # Implement other specific NULL handling logic here...

        return df