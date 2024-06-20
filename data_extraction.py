import pandas as pd
import tabula as tb 
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
    
    def retrieve_pdf_data(self,pdf_file_link):
        ''' 
        Access pdf file from a link and returns a datframe
        
        :param: pdf_file_link provides a link to a pdf file
        :return: A dataframe of the extracted data '''
        pdf_file_link = pdf_file_link

        df = tb.read_pdf(pdf_file_link,pages = "all",stream = True)

        return df


    

