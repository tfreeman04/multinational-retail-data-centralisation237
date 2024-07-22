import pandas as pd
import re 
import psycopg2
class DataCleaning:

    def __init__(self):
        pass

    def clean_user_data(self, df):
        """
        Clean the user data by removing empty columns and handling NULL values.
        
        :param df: The DataFrame containing user data.
        :return: A cleaned DataFrame.
        """
        df = self.remove_empty_columns(df)
        df = self.handle_null_values(df)
        df = self.correct_date_errors(df)
        df = self.correctly_type_values(df)
        df = self.remove_incorrect_rows(df)
        cleaned_df = df.dropna(subset=['user_uuid'])

    # Ensure there are no duplicates in the user_uuid column
        cleaned_df = cleaned_df.drop_duplicates(subset=['user_uuid'])

    # Fill any remaining NULL values in other columns with 'Unknown'
        cleaned_df = cleaned_df.fillna('Unknown')

        return cleaned_df
        
    
    def clean_card_data(self, df):
        """
        Clean the card data by removing empty columns and handling NULL values.

        :param df: The DataFrame containing card data.
        :return: A cleaned DataFrame.
        """
        df = self.remove_empty_columns(df)
        df = self.handle_null_values(df)
        df = self.correct_date_errors(df)
        df = self.correctly_type_values(df)
        df = self.remove_incorrect_rows(df)
        return df
    
    def clean_store_data(self,df):

        df = self.remove_empty_columns(df)
        df = self.handle_null_values(df)
        df = self.correct_date_errors(df)
        df = self.correctly_type_values(df)
        df = self.remove_incorrect_rows(df)
        return df
    
    def clean_date_time_data(self,df):
        df = self.remove_empty_columns(df)
        df = self.handle_null_values(df)
        df = self.correct_date_errors(df)
        df = self.correctly_type_values(df)
        df = self.remove_incorrect_rows(df)
        return df

    
    def remove_empty_columns(self, df):
        ''' 
        Handle NULL values in the DataFrame columns by dropping columns with all NULL values.
        
        :param df: The DataFrame to clean.
        :return: The DataFrame without any empty columns.
        '''
        df = df.dropna(axis=1, how='all')
        return df
    
    def calculate_empty_values(self, df):
        '''
        Calculate the percentage of columns that contain all NULL values.
        
        :param df: The DataFrame to analyze.
        :return: A Series containing the percentages of NULL values per column.
        '''
        percentage_of_empty_values = df.isna().mean() * 100
        return percentage_of_empty_values

    def handle_null_values(self, df):
        """
        Handle NULL values in the DataFrame by filling or dropping them based on specific column logic.

        :param df: The DataFrame to handle NULL values for.
        :return: The DataFrame with handled NULL values.
        """
        # Example: Fill missing values in 'first_name' column with 'Unknown'
        if 'first_name' in df.columns:
            cleaned_df =df['first_name'].fillna('Unknown', inplace=True)

        # Example: Drop rows where 'email_address' is NULL
        if 'email_address' in df.columns:
            cleaned_df=df.dropna(subset=['email_address'], inplace=True)

        if 'card_details' in df.columns:
            cleaned_df=df.dropna(subset=['card_details'], inplace=True)

        # Implement other specific NULL handling logic here as needed...
        cleaned_df=df.dropna(how = "any")

        cleaned_df = df.dropna(how='any')  # Drop all rows with any NULL values
        cleaned_df.reset_index(drop=True, inplace=True) 
        
        return cleaned_df
    
    def correct_date_errors(self, df):
        """
        Correct date errors in the DataFrame.

        :param df: The DataFrame containing date columns.
        :return: The DataFrame with corrected dates.
        """
        # Example: Ensure 'registration_date' is in the correct format
        if 'registration_date' in df.columns:
            df['registration_date'] = pd.to_datetime(df['registration_date'], errors='coerce')
        return df

    def correctly_type_values(self, df):
        """
        Ensure values in the DataFrame are correctly typed.

        :param df: The DataFrame containing various columns.
        :return: The DataFrame with correctly typed values.
        """
        # Example: Ensure 'age' is of type int
        if 'age' in df.columns:
            df['age'] = pd.to_numeric(df['age'], errors='coerce').fillna(0).astype(int)
        return df

    def remove_incorrect_rows(self, df):
        """
        Remove rows with incorrect information from the DataFrame.

        :param df: The DataFrame to clean.
        :return: The DataFrame with incorrect rows removed.
        """
        # Example: Remove rows where 'first_name' is 'Unknown'
        if 'first_name' in df.columns:
            df = df[df['first_name'] != 'Unknown']
        return df
    
    def convert_product_weights(self, products_df):
        def convert_weight(weight):
            weight = str(weight).lower().strip()
            
            try:
                # Handle 'kg'
                if 'kg' in weight:
                    return float(weight.replace('kg', '').strip())
                # Handle 'g'
                elif 'g' in weight:
                    return float(weight.replace('g', '').strip()) / 1000
                # Handle 'ml'
                elif 'ml' in weight:
                    return float(weight.replace('ml', '').strip()) / 1000
                # Handle 'l'
                elif 'l' in weight:
                    return float(weight.replace('l', '').strip())
                # Handle patterns like '12 x 100g'
                elif 'x' in weight:
                    parts = weight.split('x')
                    if len(parts) == 2:
                        num_items = float(parts[0].strip())
                        weight_per_item = float(parts[1].strip().replace('g', '').replace('kg', '').replace('ml', '').replace('l', '').strip())
                        total_weight = num_items * weight_per_item
                        # Assuming weight per item is in grams if no unit is given
                        if 'kg' in weight or (total_weight >= 1000 and 'g' not in weight and 'ml' not in weight):
                            return total_weight
                        return total_weight / 1000
                # Default case for unexpected formats
                else:
                    return None
            except ValueError:
                return None

        products_df['weight'] = products_df['weight'].apply(convert_weight)
        return products_df
    
    def clean_products_data(self, products_df):
        # Convert product weights to a uniform format (kg)
        products_df = self.convert_product_weights(products_df)

        # Handle missing values
        products_df.dropna(inplace=True)  # Drop rows with any missing values

        # Remove duplicates
        products_df.drop_duplicates(inplace=True)

        # Fix data types
        products_df['weight'] = products_df['weight'].astype(float)  # Ensure weight is a float

        # Standardize text data (example: product names to lower case)
        if 'product' in products_df.columns:
            products_df['product'] = products_df['product'].str.lower().str.strip()

        # Handle outliers (example: remove rows with weights that are unrealistically high or low)
        products_df = products_df[(products_df['weight'] > 0) & (products_df['weight'] < 100)]

        return products_df
    
    def clean_orders_data(self, orders_df):
        # Remove specified columns from the orders DataFrame
        columns_to_remove = ['first_name', 'last_name', '1']
        orders_df_cleaned = orders_df.drop(columns=columns_to_remove, errors='ignore')
        
        # Optionally, you can also remove any rows with NaN values
        orders_df_cleaned = orders_df_cleaned.dropna()

        return orders_df_cleaned
    

    def clean_and_add_primary_key(self,db_config, table_name, column_name):
        try:
            # Connect to the PostgreSQL database
            conn = psycopg2.connect(
                dbname=db_config['DATABASE'],
                user=db_config['USER'],
                password=db_config['PASSWORD'],
                host=db_config['HOST'],
                port=db_config['PORT']
            )
            cur = conn.cursor()

            # Remove rows with NULL values in the primary key column
            delete_nulls_query = f"""
            DELETE FROM {table_name}
            WHERE {column_name} IS NULL;
            """
            cur.execute(delete_nulls_query)
            conn.commit()

            # Ensure uniqueness of the primary key column
            check_duplicates_query = f"""
            SELECT {column_name}, COUNT(*)
            FROM {table_name}
            GROUP BY {column_name}
            HAVING COUNT(*) > 1;
            """
            cur.execute(check_duplicates_query)
            duplicates = cur.fetchall()

            if duplicates:
                print(f"Duplicates found in {column_name} column: {duplicates}")
                return

            # Add primary key constraint
            add_pk_query = f"""
            ALTER TABLE {table_name}
            ADD CONSTRAINT pk_{column_name} PRIMARY KEY ({column_name});
            """
            cur.execute(add_pk_query)
            conn.commit()

            print(f"Primary key added to {column_name} in {table_name} successfully.")

            # Close the cursor and connection
            cur.close()
            conn.close()

        except Exception as e:
            print(f"Error adding primary key: {e}")
            if conn is not None:
                conn.rollback()
            if cur is not None:
                cur.close()
            if conn is not None:
                conn.close()
