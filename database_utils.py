import yaml
from sqlalchemy import create_engine, inspect
from data_extraction import DataExtractor

class DatabaseConnector:
    def __init__(self, db_creds):
        self.db_creds = db_creds
        self.data_loaded = self.read_db_creds()
        self.engine = self.init_db_engine()
        # self.data_extractor = DataExtractor(self) can refactor this here ? 

    def read_db_creds(self):
        """
        Reads the database credentials from a YAML file and returns them as a dictionary.

        :return: A dictionary with the database credentials.
        """
        with open(self.db_creds, 'r') as file:
            data_loaded = yaml.safe_load(file)
        print("Loaded Data:", data_loaded)  # Debugging line
        return data_loaded

    def init_db_engine(self):
        """
        Initializes and returns an SQLAlchemy database engine using the provided credentials.

        :return: An SQLAlchemy database engine.
        """
        required_keys = ['RDS_HOST', 'RDS_PASSWORD', 'RDS_USER', 'RDS_DATABASE', 'RDS_PORT']
        for key in required_keys:
            if key not in self.data_loaded:
                raise KeyError(f"Missing required key in credentials: {key}")

        db_url = (f"postgresql+psycopg2://{self.data_loaded['RDS_USER']}:{self.data_loaded['RDS_PASSWORD']}@"
                  f"{self.data_loaded['RDS_HOST']}:{self.data_loaded['RDS_PORT']}/"
                  f"{self.data_loaded['RDS_DATABASE']}")
        engine = create_engine(db_url)
        return engine
    
    def list_db_tables(self):
        self.engine.execution_options(isolation_level='AUTOCOMMIT').connect()
        inspector = inspect(self.engine)
        tables = inspector.get_table_names()

        return tables
    
    def upload_to_db(self, df, table_name):
        ''' change below
        from sqlalchemy import create_engine
        engine = create_engine('postgresql://username:password@localhost:5432/mydatabase')
        df.to_sql('table_name', engine) '''
        
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
    # Initialize the DatabaseConnector with the path to the credentials file
    db_connector = DatabaseConnector('db_creds.yaml')
    
    # Access the database credentials
    print(db_connector.data_loaded)
    
    # Access the database engine
    print(db_connector.engine)
    db_connector.list_db_tables()
    tables = db_connector.list_db_tables()
    
    print("Tables in the database:", tables)

    data_extractor = DataExtractor(db_connector)
    
    # Read data from a specific table
    if tables:
        table_name = tables[2]  # Just an example, you can choose any table from the list
        df = data_extractor.read_rds_table(table_name)
        print(f"Data from table {table_name}:\n", df.head())
    field_names = db_connector.list_field_names("legacy_users")
    print(field_names)