# ETL Project for Largest Banks Data
## Overview
This project extracts data on the largest banks by market capitalization from a Wikipedia page, transforms it using exchange rate data, and then loads the data into both a CSV file and an SQLite database. The process involves three main steps: Extraction, Transformation, and Loading (ETL). Additionally, SQL queries are executed on the database to verify and analyze the data.

## Prerequisites
Before running the ETL process, ensure that you have the following:

Python installed
Required Python libraries (pandas, numpy, beautifulsoup4, requests, sqlite3)
Access to the following files and paths:
exchange_rate.csv (contains exchange rates for currency conversion)
largest_banks.csv (destination for the transformed data in CSV format)
etl_project_log.txt (log file to track the ETL process)
SQLite database file: Banks.db
Code Explanation
## 1. Importing Required Libraries
python
Copy code
import os
import sqlite3
from bs4 import BeautifulSoup as soup
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
from io import StringIO
## 2. File and URL Paths
Specify paths for the URL to scrape, CSV files, log file, and SQLite database.

### (python) Copy code
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = r"C:\Users\Abdo\Desktop\Projects\ETL_Preoject_Bank\largest_banks.csv"
log_path = r"./etl_project_log.txt"
exchange_rate_path = r"C:\Users\Abdo\Desktop\Projects\ETL_Preoject_Bank\exchange_rate.csv"
db_name = 'Banks.db'
table_name = 'Largest_banks'
## 3. Logging Function
Logs the progress of the ETL process.

### (python) Copy code
def log_progress(message, log_path):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%b-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a") as f:
        f.write(timestamp + ' : ' + message + '\n')
## 4. Extraction Function
Extracts data from the specified URL and processes it into a DataFrame.

### (python) Copy code
def extract(url):
    page = requests.get(url).text
    data = soup(page, 'html.parser')
    heading = data.find('span', {'id': 'By_market_capitalization'})
    table = heading.find_next('table', {'class': 'wikitable'})
    df = pd.read_html(StringIO(str(table)))[0]
    df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].apply(lambda x: float(str(x).strip()[:-1]))
    return df
## 5. Transformation Function
Transforms the DataFrame by converting market capitalization to other currencies using exchange rates.

### (python) Copy code
def transform(df, exchange_rate):
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['Market cap (US$ billion)']]
    return df
## 6. Load to CSV Function
Saves the transformed DataFrame to a CSV file.

### (python) Copy code
def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")
## 7. Load to Database Function
Loads the DataFrame into an SQLite database.

### (python) Copy code
def load_to_db(conn, table_name, df):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data loaded into table {table_name} in database.")
## 8. Run SQL Queries Function
Executes SQL queries on the SQLite database and logs the output.

### (python) Copy code
def run_queries(query, conn):
    """
    Executes the given SQL query and prints the results.
    
    Parameters:
    query (str): The SQL query to execute.
    conn (sqlite3.Connection): The SQLite3 database connection object.
    """
    print(f"Executing Query: {query}")
    try:
        df = pd.read_sql_query(query, conn)
        print("Query Output:")
        print(df)
    except Exception as e:
        print(f"Error: {e}")
    
    log_progress(f"Executed Query: {query}\nOutput: {df.to_string(index=False)}", log_path)
## 9. Main Execution
Extract data from the URL
Transform the data using exchange rates
Save the data to CSV
Load the data into the SQLite database
Execute and log SQL queries
### (python) Copy code
log_progress('Preliminaries complete. Initiating ETL process', log_path)

df = extract(url)
log_progress('Data extraction complete.', log_path)

## Read exchange rate CSV and convert to dictionary
exchange_rate_df = pd.read_csv(exchange_rate_path)
exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

log_progress('Exchange rate data loaded.', log_path)

df = transform(df, exchange_rate)
log_progress('Data transformation complete.', log_path)

## Print the specific required value
print(df['MC_EUR_Billion'][4])
log_progress('Specific value printed successfully.', log_path)

## Save to CSV
load_to_csv(df, csv_path)
log_progress('Transformed data saved to CSV file.', log_path)

## Load to SQLite database
conn = sqlite3.connect(db_name)
load_to_db(conn, table_name, df)
log_progress(f'Data loaded into table {table_name} in database.', log_path)
conn.close()

## Connect to the SQLite database
conn = sqlite3.connect(db_name)

## Queries
queries = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT `Bank name` FROM Largest_banks LIMIT 5"  # Adjusted column name
]

## Execute and log each query
for query in queries:
    run_queries(query, conn)

## Close the database connection
conn.close()
Notes
Ensure that the file paths and URLs are correctly set to avoid errors.
Check the log file (etl_project_log.txt) for detailed progress and any issues encountered during the ETL process.
Adjust the column names and SQL queries as needed based on the actual data structure.
