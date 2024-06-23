import pandas as pd
import pdfplumber
import requests
from sqlalchemy import create_engine

class DataExtractor:
    def __init__(self, db_connector):
        self.db_connector = db_connector

    def read_rds_table(self, table_name):
        """
        Read data from an RDS table and return it as a DataFrame.
        
        :param table_name: The name of the table to read data from.
        :return: A DataFrame containing the table data.
        """
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql(query, self.db_connector.engine)
        return df
    
    def retrieve_pdf_data(self, pdf_link):
        """
        Retrieve data from a PDF document and return it as a DataFrame.
        
        :param pdf_link: The link to the PDF document.
        :return: A DataFrame containing the extracted data from the PDF.
        """
        try:
            # Download the PDF file to a temporary location
            temp_pdf = "temp.pdf"
            response = requests.get(pdf_link)
            with open(temp_pdf, 'wb') as f:
                f.write(response.content)

            # Extract tables using pdfplumber
            dfs = []
            with pdfplumber.open(temp_pdf) as pdf:
                for page in pdf.pages:
                    table = page.extract_table()
                    if table:
                        dfs.append(pd.DataFrame(table[1:], columns=table[0]))

            if dfs:
                combined_df = pd.concat(dfs, ignore_index=True)
                return combined_df
            else:
                return pd.DataFrame()
        except Exception as e:
            print(f"Error occurred while extracting data from PDF: {e}")
            return pd.DataFrame()



    

