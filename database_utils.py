import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
from data_extraction import DataExtractor
from data_cleaning import DataCleaning

class DatabaseConnector:
    def __init__(self, db_creds=None, db_config=None):
        """
        Initialize DatabaseConnector with either db_creds YAML file or db_config dictionary.
        :param db_creds: Path to the YAML file containing database credentials.
        :param db_config: Dictionary containing database credentials.
        """
        self.db_creds = db_creds
        self.db_config = db_config
        if db_creds:
            self.data_loaded = self.read_db_creds(db_creds)
        elif db_config:
            self.data_loaded = db_config
        else:
            raise ValueError("Either db_creds or db_config must be provided")
        
        self.data_loaded = self.standardize_keys(self.data_loaded)
        self.engine = self.init_db_engine()

    def read_db_creds(self, db_creds):
        """
        Reads the database credentials from a YAML file and returns them as a dictionary.
        :return: A dictionary with the database credentials.
        """
        with open(db_creds, 'r') as file:
            data_loaded = yaml.safe_load(file)
        print("Loaded Data from YAML:", data_loaded)  # Debugging line
        return data_loaded

    def standardize_keys(self, data):
        """
        Standardizes the keys in the credentials dictionary to match the required keys.
        :param data: The dictionary with original keys.
        :return: A dictionary with standardized keys.
        """
        key_mapping = {
            'RDS_HOST': 'HOST',
            'RDS_PASSWORD': 'PASSWORD',
            'RDS_USER': 'USER',
            'RDS_DATABASE': 'DATABASE',
            'RDS_PORT': 'PORT'
        }
        standardized_data = {key_mapping.get(k, k): v for k, v in data.items()}
        print("Standardized Data:", standardized_data)  # Debugging line
        return standardized_data

    def init_db_engine(self):
        """
        Initializes and returns an SQLAlchemy database engine using the provided credentials.
        :return: An SQLAlchemy database engine.
        """
        required_keys = ['HOST', 'PASSWORD', 'USER', 'DATABASE', 'PORT']
        for key in required_keys:
            if key not in self.data_loaded:
                raise KeyError(f"Missing required key in credentials: {key}")

        db_url = (f"postgresql+psycopg2://{self.data_loaded['USER']}:{self.data_loaded['PASSWORD']}@"
                  f"{self.data_loaded['HOST']}:{self.data_loaded['PORT']}/"
                  f"{self.data_loaded['DATABASE']}")
        engine = create_engine(db_url)
        return engine
    
    def list_db_tables(self):
        """
        Lists all the tables in the connected database.
        :return: A list of table names.
        """
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()
        return tables
    
    def upload_to_db(self, df, table_name, if_exists='fail'):
        """
        Uploads a Pandas DataFrame to a specified table in the current database.
        :param df: The DataFrame to upload.
        :param table_name: The name of the table to upload the data to.
        :param if_exists: What to do if the table already exists. Options are 'fail', 'replace', or 'append'.
                          Default is 'fail'.
        """
        df.to_sql(table_name, self.engine, if_exists=if_exists, index=False)

    def list_field_names(self, table_name):
        """
        Lists all field names (columns) in the specified table.
        :param table_name: The name of the table to list field names for.
        :return: A list of field names (column names).
        """
        inspector = inspect(self.engine)
        columns = inspector.get_columns(table_name)
        field_names = [column['name'] for column in columns]
        return field_names

# Example usage
if __name__ == "__main__":
    # Initialize the DatabaseConnector for the source database using a YAML file
    source_db_connector = DatabaseConnector(db_creds='db_creds.yaml')
    
    # Initialize the DatabaseConnector for the target database using a dictionary
    target_db_config = {
        'HOST': 'localhost',
        'PASSWORD': 'Password',  # Replace with your actual password
        'USER': 'postgres',
        'DATABASE': 'sales_data',
        'PORT': 5432
    }
    target_db_connector = DatabaseConnector(db_config=target_db_config)
    
    # List all tables in the source database
    source_tables = source_db_connector.list_db_tables()
    print("Tables in the source database:", source_tables)

    # Identify the user table in the source database
    user_table = None
    for table in source_tables:
        if 'users' in table.lower():
            user_table = table
            break
    
    if not user_table:
        raise ValueError("No table containing user data found in the source database.")

    print(f"User data table identified: {user_table}")

    # List field names for the user data table
    field_names = source_db_connector.list_field_names(user_table)
    print(f"Field names in the table {user_table}:", field_names)

    # Initialize the DataExtractor
    data_extractor = DataExtractor(db_connector=source_db_connector)
    
    # Read data from the user data table in the source database
    df = data_extractor.read_rds_table(user_table)
    print(f"Data from table {user_table} in the source database:\n", df.head())

    # Initialize the DataCleaning
    data_cleaning = DataCleaning()

    # Clean the user data
    cleaned_df = data_cleaning.clean_user_data(df)
    print(f"Cleaned data from table {user_table}:\n", cleaned_df.head())

    # Upload cleaned data to a new table in the target database
    new_table_name = 'dim_users'
    target_db_connector.upload_to_db(cleaned_df, new_table_name, if_exists='replace')
    print(f"Cleaned data uploaded to table {new_table_name} in the target database.")

    pdf_link = "https://data-handling-public.s3.eu-west-1.amazonaws.com/card_details.pdf"
    pdf_data_df = data_extractor.retrieve_pdf_data(pdf_link)
    print("Extracted data from PDF:\n", pdf_data_df.head())
    
    cleaned_df = data_cleaning.clean_card_data(pdf_data_df)
    print("cleaned data from table dim card details",cleaned_df.head())
    
    new_table_name_card = 'dim_card_details'
    #upload cleaned card data to table in the target database
    target_db_connector.upload_to_db(cleaned_df,new_table_name_card,if_exists ='replace')
    print(f"Cleaned data uploaded to table {new_table_name_card} in the target database.")

