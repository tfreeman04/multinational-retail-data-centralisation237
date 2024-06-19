import pandas as pd
class DataExtractor:
    
    def __init__(self,db_connector):
        self.db_connector = db_connector

    def read_rds_table(self,table_name):
        """
        Reads data from a specific table in the database.

        :param table_name: The name of the table to read data from.
        :return: A DataFrame containing the data from the table.
        """
        query = f"SELECT * FROM {table_name}"
        with self.db_connector.engine.connect() as connection:
            df = pd.read_sql(query, connection)
        return df

