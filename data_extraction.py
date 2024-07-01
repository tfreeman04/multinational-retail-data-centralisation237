import pandas as pd
import pdfplumber
import requests
from sqlalchemy import create_engine
import time
from requests.exceptions import RequestException
import boto3
from io import StringIO

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
        
    def list_number_of_stores(self, endpoint, headers):
        """
        Retrieve the number of stores from the API.
        
        :param endpoint: The endpoint URL to get the number of stores.
        :param headers: The headers to include in the API request.
        :return: The number of stores as an integer.
        """
        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()  # Raise an error for bad status codes
            response_json = response.json()
            print("API response JSON:", response_json)  # Debugging line

            # Use the correct key from the API response
            num_stores = response_json.get('number_stores')
            
            if num_stores is None:
                print("Expected key 'number_stores' not found in the response. Response keys:", response_json.keys())
                raise ValueError("The API response did not contain the number of stores.")
            
            return int(num_stores)
        
        except (requests.RequestException, ValueError) as e:
            print(f"Error occurred while retrieving the number of stores: {e}")
            return None


    def retrieve_stores_data(self, store_endpoint, headers, num_stores):
        stores_data = []
        for store_number in range(1, num_stores + 1):
            success = False
            retries = 3
            while not success and retries > 0:
                try:
                    # Set a timeout to prevent hanging indefinitely
                    response = requests.get(store_endpoint.format(store_number=store_number), headers=headers, timeout=10)
                    response.raise_for_status()
                    store_data = response.json()
                    stores_data.append(store_data)
                    success = True
                except (RequestException, ValueError) as e:
                    print(f"Error occurred while retrieving data for store number {store_number}: {e}")
                    retries -= 1
                    if retries > 0:
                        print(f"Retrying... ({3 - retries} retries left)")
                        time.sleep(1)
                    else:
                        print(f"Failed to retrieve data for store number {store_number} after multiple attempts.")
                        
                except KeyboardInterrupt:
                    print("Process interrupted by user. Exiting.")
                    return pd.DataFrame(stores_data)
        return pd.DataFrame(stores_data)
    
    def extract_from_s3(self, s3_address):
        try:
            # Parse the S3 address
            if not s3_address.startswith("s3://"):
                raise ValueError("Invalid S3 address. It should start with 's3://'.")
            
            s3_parts = s3_address.replace("s3://", "").split("/", 1)
            
            if len(s3_parts) != 2:
                raise ValueError("Invalid S3 address. It should contain both bucket name and key.")
            
            bucket_name, key = s3_parts

            if not bucket_name or not key:
                raise ValueError("Invalid S3 address. Bucket name or key is missing.")

            # Initialize a session using Amazon S3
            s3 = boto3.client('s3')

            # Get the object from the S3 bucket
            obj = s3.get_object(Bucket=bucket_name, Key=key)

            # Read the data into a pandas DataFrame
            data = obj['Body'].read().decode('utf-8')
            df = pd.read_csv(StringIO(data))

            return df
        except Exception as e:
            print(f"Error occurred while extracting data from S3: {e}")
            return pd.DataFrame()


    

