# multinational-retail-data-centralisation

## Table of Contents
- Description
- Installation Instructions
- Usage Instructions
- File Structure
- License Information

## Description
This project demonstrates how to extract data from AWS S3, clean it, and upload it to a PostgreSQL database using Python.

The aim of the project is to showcase integration of AWS S3 and PostgreSQL with data cleaning techniques using pandas and database interaction using psycopg2.

## Installation Instructions
- Clone the repository: `git clone https://github.com/tfreeman04/multinational-retail-data-centralisation237.git"
- Install dependencies: `pip install -r requirements.txt`

## Usage Instructions
- Ensure AWS credentials are set up properly.
- Modify `db_credentials` dictionary in the script with your PostgreSQL credentials.
- Run the script to extract, clean, and upload data: `python main_script.py`

## File Structure

├── main_script.py
├── data_extraction.py
├── data_cleaning.py
├── README.md
└── requirements.txt


## License Information
This project is licensed under the MIT License. See LICENSE.txt for details.




