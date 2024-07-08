import yaml
from sqlalchemy import create_engine, inspect
import pandas as pd
from data_extraction import DataExtractor
from data_cleaning import DataCleaning
import psycopg2

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
    '''
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
    print(f"Cleaned data uploaded to table {new_table_name_card} in the target database.") '''
     # Initialize the DataExtractor
    data_extractor = DataExtractor(db_connector=source_db_connector)
    # Initialize the DataCleaning
    data_cleaning = DataCleaning()
    '''
    # Define the API details
    api_key = 'yFBQbwXe9J3sd6zWVAMrK6lcxxr0q1lr2PT6DDMX'
    headers = {'x-api-key': api_key}
    number_of_stores_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/number_stores'
    store_details_endpoint = 'https://aqj7u5id95.execute-api.eu-west-1.amazonaws.com/prod/store_details/{store_number}'

    target_db_config = {
            'RDS_HOST': 'localhost',
            'RDS_PASSWORD': 'Password',
            'RDS_USER': 'postgres',
            'RDS_DATABASE': 'sales_data',
            'RDS_PORT': 5432
        }
    target_db_connector = DatabaseConnector(db_config=target_db_config)
    # Get the number of stores
    number_of_stores = data_extractor.list_number_of_stores(number_of_stores_endpoint, headers)
    print(f"Number of stores: {number_of_stores}")

     # Retrieve and print data for all stores
    stores_df = data_extractor.retrieve_stores_data(store_details_endpoint, headers, number_of_stores)
    print(f"Data for all stores:\n{stores_df}")

     # Clean the store data
    cleaned_stores_df = data_cleaning.clean_store_data(stores_df)
    print(f"Cleaned data for all stores:\n{cleaned_stores_df}")

    new_table_name = 'dim_store_details'
    target_db_connector.upload_to_db(cleaned_stores_df, new_table_name, if_exists='replace')
    print(f"Cleaned data uploaded to table {new_table_name} in the target database.") 

    #bucket details 
    s3_address = "s3://data-handling-public/products.csv"
    products_df = data_extractor.extract_from_s3(s3_address)
    #test data pulled from buscet correctly 
    print(products_df.head())

    #cleaned_product_df = data_cleaning.convert_product_weights(products_df)
    #test data is cean
    #added method to clean all of the data and included the conversion inside this method. 
    cleaned_product_df = data_cleaning.clean_products_data(products_df) 
    print(cleaned_product_df.head)
    #working perfectly
    new_table_name = "dim_products"
    #uploaded the cleaded data to the database
    target_db_connector.upload_to_db(cleaned_product_df,new_table_name,if_exists='replace')
    print(f"Cleaned data uploaded to table {new_table_name} in the target database.")
    # list the tables to find the orders table 
    source_tables = source_db_connector.list_db_tables()
    print("Tables in the database:")
    #for table in source_tables:
    #    print(table)
    #df = data_extractor.read_rds_table(orders_table)

    # Identify the user table in the source database
    orders_table = None
    for table in source_tables:
        if 'order' in table.lower():
            orders_table = table
            break
    
    if not orders_table:
        raise ValueError("No table containing user data found in the source database.")

    print(f"Orders_data table identified: {orders_table}")

    # List field names for the user data table
    field_names = source_db_connector.list_field_names(orders_table)
    print(f"Field names in the table {orders_table}:", field_names)'''

    # Initialize the DataExtractor
    #data_extractor = DataExtractor(db_connector=source_db_connector)
    
    # Read data from the user data table in the source database
    '''orders_data_df = data_extractor.read_rds_table(orders_table)
    print(f"Data from table {orders_table} in the source database:\n", orders_data_df.head())

    # Clean orders data
    cleaned_orders_df = data_cleaning.clean_orders_data(orders_data_df)
    new_table_name = "orders_table"
    #uploaded the cleaded data to the database
    target_db_connector.upload_to_db(cleaned_orders_df,new_table_name,if_exists='replace')
    print(f"Cleaned data uploaded to table {new_table_name} in the target database.")


    s3_address = "https://data-handling-public.s3.eu-west-1.amazonaws.com/date_details.json"
    dim_date_times_df = data_extractor.extract_from_s3_link(s3_address)
    #test data pulled from buscet correctly 
    
    #print(dim_date_times_df.head())

    #clean the datat from date_times table 

    cleaned_dim_date_times_df = data_cleaning.clean_date_time_data(dim_date_times_df)
    
    new_table_name = "dim_date_times"
    #uploaded the cleaded data to the database
    target_db_connector.upload_to_db(cleaned_dim_date_times_df,new_table_name,if_exists='replace')
    print(f"Cleaned data uploaded to table {new_table_name} in the target database.")'''

    #update the data types in the orders table using the targetDatabase created earlier. 

    # Establish a connection to the database
    try:
        conn = psycopg2.connect(
            host=target_db_config['HOST'],
            user=target_db_config['USER'],
            password=target_db_config['PASSWORD'],
            database=target_db_config['DATABASE'],
            port=target_db_config['PORT']
        )
        cursor = conn.cursor()
        print("Connected to the database!")
    except psycopg2.Error as e:
        print(f"Error connecting to the database: {e}")
        exit(1)

        #once the connection is established change the datatypes in the orders table 
    '''
    try:
        # Alter date_uuid column to UUID type
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;")
        conn.commit()

        # Alter user_uuid column to UUID type
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN user_uuid TYPE UUID USING user_uuid::UUID;")
        conn.commit()

        # Alter card_number column to VARCHAR with appropriate length
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN card_number TYPE VARCHAR(20);")  
        conn.commit()

        # Alter store_code column to VARCHAR with appropriate length
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN store_code TYPE VARCHAR(20);")  
        conn.commit()

        # Alter product_code column to VARCHAR with appropriate length
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN product_code TYPE VARCHAR(20);")  
        conn.commit()

        # Alter product_quantity column to SMALLINT
        cursor.execute("ALTER TABLE orders_table ALTER COLUMN product_quantity TYPE SMALLINT;")
        conn.commit()

        print("Successfully updated data types in the orders table.")
    
    except psycopg2.Error as e:

        print(f"Error updating data types: {e}")
    finally:
        cursor.close()
        conn.close()

        #code ran successfully '''
    '''
    # SQL command to remove invalid longitude entries
    clean_longitude_query = """
        DELETE FROM dim_store_details WHERE longitude !~ '^-?\\d+(\\.\\d+)?$';
        """

    # SQL command to remove invalid latitude entries
    clean_latitude_query = """
        DELETE FROM dim_store_details WHERE latitude !~ '^-?\\d+(\\.\\d+)?$';
        """

    # Execute the queries
    cursor.execute(clean_longitude_query)
    cursor.execute(clean_latitude_query)

    # Commit the changes
    conn.commit()

    

    print("Invalid entries removed successfully!")    
    '''
    
    '''clean_invalid_staff_numbers_query = """
            DELETE FROM dim_store_details WHERE staff_numbers !~ '^\d+$';
            """
    cursor.execute(clean_invalid_staff_numbers_query)'''

    ''' 
    #looking at dim_store_details_table again to check date column for errors 
    #show dim_store_details query
    show_store_details = """
            SELECT * FROM dim_store_details;
            """
    cursor.execute(show_store_details)
    rows = cursor.fetchall()
    colnames = [desc[0]for desc in cursor.description]
    print(f"Column names: {colnames}")
    for row in rows:
            print(row)
    ''' 
    #purged the opening_date column
    #clean_opening_date_query = """
    #UPDATE dim_store_details SET opening_date = NULL WHERE opening_date !~ '^\\d{4}-\\d{2}-\\d{2}$';
    #"""
    #cursor.execute(clean_opening_date_query)

    
    '''try:
        # Alter longitude column to float type
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN longitude TYPE FLOAT USING longitude::double precision ;")
        conn.commit()

        # Alter locality column to VARCHAR type
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN locality TYPE VARCHAR(255);")
        conn.commit()

        # Alter store_code column to VARCHAR with appropriate length
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN store_code TYPE VARCHAR(20);")  
        conn.commit()

        # Alter staff_numbers column to SMALLINT 
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN staff_numbers TYPE SMALLINT USING staff_numbers::smallint;")  
        conn.commit()

        # Alter opening_date column to DATE
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN opening_date TYPE DATE USING opening_date::date;")  
        conn.commit()

        # Alter store_type column to VARCHAR NULLABLE
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN store_type TYPE VARCHAR(255);")
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN store_type DROP NOT NULL;")
        
        conn.commit()

        #Alter latitude column to float type
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN latitude TYPE FLOAT USING latitude::double precision;")
        conn.commit()

        # Alter country_code to VARCHAR with appropriate length 
        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN country_code TYPE VARCHAR(255);")

        #Alter continent column to VARCHAR with apprpriate length

        cursor.execute("ALTER TABLE dim_store_details ALTER COLUMN continent TYPE VARCHAR(255);")


        print("Successfully updated data types in the dim_store_details.")

        # Remove '£' character from product_price column
        remove_pound_sign_query = """
        UPDATE dim_products
        SET product_price = REPLACE(product_price, '£', '');
        """
        cursor.execute(remove_pound_sign_query)


        conn.commit()
        # add new column and details for column based on weight
        add_weight_class_query = """
        ALTER TABLE dim_products
        ADD COLUMN weight_class VARCHAR(20);

        UPDATE dim_products
        SET weight_class = CASE
            WHEN weight < 2 THEN 'Light'
            WHEN weight >= 2 AND weight < 40 THEN 'Mid_Sized'
            WHEN weight >= 40 AND weight < 140 THEN 'Heavy'
            ELSE 'Truck_Required'
        END;
        """
        cursor.execute(add_weight_class_query)
        conn.commit()
        print("product details updated successfully")
    
    except psycopg2.Error as e:

        print(f"Error updating data types: {e}")
    finally:
        cursor.close()
        conn.close()
        
    '''
    
    #cleaning the date_added column 

    #clean_date_added_query = """
       # UPDATE dim_products SET date_added = NULL WHERE date_added !~ '^\\d{4}-\\d{2}-\\d{2}$';
       # """
    #cursor.execute(clean_date_added_query)
    
    # changing the datatypes for the dim_products table 
    '''
    try:
        # ALTER product_price column from TEXT to FLOAT 
        cursor.execute("ALTER TABLE dim_products ALTER COLUMN product_price TYPE FLOAT USING product_price::double precision ;")
        conn.commit()
        
        # ALTER weight column from TEXT to FLOAT
        cursor.execute("ALTER TABLE dim_products ALTER COLUMN weight TYPE FLOAT USING weight::double precision ;")
        conn.commit()

        #ALTER EAN column from TEXT to VARCHAR 
        #cursor.execute("ALTER TABLE dim_products ALTER COLUMN EAN TYPE VARCHAR(255);")
        #conn.commit()

        #ALTER product_code column from TEXT to VARCHAR
        cursor.execute("ALTER TABLE dim_products ALTER COLUMN product_code TYPE VARCHAR(255);")
        conn.commit()

        #ALTER date_added column to DATE
        cursor.execute("ALTER TABLE dim_products ALTER COLUMN date_added TYPE DATE USING date_added::date;")  
        conn.commit()

        #ALTER uuid column from TEXT to UUID
        cursor.execute("ALTER TABLE dim_products ALTER COLUMN uuid TYPE UUID USING uuid::UUID;")
        conn.commit()

        #ALTER still_available column from TEXT to BOOL
        #cursor.execute("ALTER TABLE dim_products ALTER COLUMN still_available TYPE BOOL;")
        #conn.commit()

        #ALTER weight_class column from TEXT to VARCHAR

        cursor.execute("ALTER TABLE dim_products ALTER COLUMN weight_class TYPE VARCHAR(100);")
        conn.commit()

        print("Successfully updated data types in the dim_products.")

    except psycopg2.Error as e:

        print(f"Error updating data types: {e}")
    finally:
        cursor.close()
        conn.close()
        '''
    # updated the product_table but realised I needed to rename the removed column to still_available.
    '''
    try:
    
        #cursor.execute("ALTER TABLE dim_products  RENAME COLUMN removed TO still_available;")
        #conn.commit()
        #ALTER still_available column from TEXT to BOOL
        cursor.execute("ALTER TABLE dim_products ALTER COLUMN still_available TYPE BOOL USING case still_available when still_available then true else false end;")
        conn.commit()



    except psycopg2.Error as e:

        print(f"Error updating data types: {e}")
    finally:
        cursor.close()
    '''
        
        
    # cursor.execute("SELECT * FROM dim_products;")
    # rows = cursor.fetchall()
    # colnames = [desc[0]for desc in cursor.description]
    # print(f"Column names: {colnames}")
    # for row in rows:
    #         print(row)
    # conn.close()
    # checked the columns updated correctly 

    #Updating the dim_date_times table 
    '''
    try:
        # ALTER column month from TEXT to VARCHAR (10)

        cursor.execute("ALTER TABLE dim_date_times ALTER COLUMN month TYPE VARCHAR(10);")
        conn.commit()

        #ALTER column year from TEXT to VARCHAR (10)
        cursor.execute("ALTER TABLE dim_date_times ALTER COLUMN year TYPE VARCHAR(10);")
        conn.commit()

        #ALTER column day from TEXT to VARCHAR (10)
        cursor.execute("ALTER TABLE dim_date_times ALTER COLUMN day TYPE VARCHAR(10);")
        conn.commit()

        #ALTER column time_period from TEXT to VARCHAR (10)
        cursor.execute("ALTER TABLE dim_date_times ALTER COLUMN time_period TYPE VARCHAR(50);")
        conn.commit()

        #ALTER column date_uuid from TEXT to UUID (10)
        cursor.execute("ALTER TABLE dim_date_times ALTER COLUMN date_uuid TYPE UUID USING date_uuid::UUID;")
        conn.commit()

        
    except psycopg2.Error as e:

        print(f"Error updating data types: {e}")
    finally:
        cursor.close()
        conn.close()
    '''

        # updating the dim_card_details table changing data types

    try:

        #ALTER column card_number column TYPE from TEXT to VARCHAR(17)
        cursor.execute("ALTER TABLE dim_card_details ALTER COLUMN card_number TYPE VARCHAR(25);")
        conn.commit()

        #ALTER column expiry_date column TYPE from TEXT to VARCHAR(10)
        cursor.execute("ALTER TABLE dim_card_details ALTER COLUMN expiry_date TYPE VARCHAR(10);")
        conn.commit()

        # clean the date_payment_confirmed column 

        clean_date_payment_confirmed_query = """
        UPDATE dim_card_details SET date_payment_confirmed = NULL WHERE date_payment_confirmed !~ '^\\d{4}-\\d{2}-\\d{2}$';
        """
        cursor.execute(clean_date_payment_confirmed_query)
        #ALTER column date_payment TYPE from TEXT to DATE 
        cursor.execute("ALTER TABLE dim_card_details ALTER COLUMN date_payment_confirmed TYPE DATE USING date_payment_confirmed::date;")  
        conn.commit()


        print("dim_card_details updated successfully")
        
    except psycopg2.Error as e:

        print(f"Error updating data types: {e}")
    finally:
        cursor.close()
        conn.close()


