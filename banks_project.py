# Importing the required libraries
import os
import sqlite3
from bs4 import BeautifulSoup as soup
import requests
import pandas as pd
import numpy as np
from datetime import datetime 
from io import StringIO

# File and URL paths
url = 'https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks'
csv_path = r"C:\Users\Abdo\Desktop\Projects\ETL_Preoject_Bank\largest_banks.csv"
log_path = r"./etl_project_log.txt"
exchange_rate_path = r"C:\Users\Abdo\Desktop\Projects\ETL_Preoject_Bank\exchange_rate.csv"
db_name = 'Banks.db'
table_name = 'Largest_banks'

# Logging function
def log_progress(message, log_path):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    timestamp_format = '%Y-%b-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    with open(log_path, "a") as f: 
        f.write(timestamp + ' : ' + message + '\n')

# Extraction function
def extract(url):
    page = requests.get(url).text
    data = soup(page, 'html.parser')
    
    # Locate the table under the heading "By market capitalization"
    heading = data.find('span', {'id': 'By_market_capitalization'})
    table = heading.find_next('table', {'class': 'wikitable'})
    
    # Parse the table into a DataFrame
    df = pd.read_html(StringIO(str(table)))[0]
    
    # Process the 'Market cap (US$ billion)' column to remove the last character and convert to float
    df['Market cap (US$ billion)'] = df['Market cap (US$ billion)'].apply(lambda x: float(str(x).strip()[:-1]))
    
    return df

# Transformation function
def transform(df, exchange_rate):
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['Market cap (US$ billion)']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['Market cap (US$ billion)']]
    return df

# Load to CSV function
def load_to_csv(df, csv_path):
    df.to_csv(csv_path, index=False)
    print(f"Data saved to {csv_path}")

# Load to database function
def load_to_db(conn, table_name, df):
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    print(f"Data loaded into table {table_name} in database.")

# Run SQL queries function
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
    
    # Log the query and output
    log_progress(f"Executed Query: {query}\nOutput: {df.to_string(index=False)}", log_path)

# Main execution
log_progress('Preliminaries complete. Initiating ETL process', log_path)

df = extract(url)
log_progress('Data extraction complete.', log_path)

# Read exchange rate CSV and convert to dictionary
exchange_rate_df = pd.read_csv(exchange_rate_path)
exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']

log_progress('Exchange rate data loaded.', log_path)

df = transform(df, exchange_rate)
log_progress('Data transformation complete.', log_path)

# Print the specific required value
print(df['MC_EUR_Billion'][4])
log_progress('Specific value printed successfully.', log_path)

# Save to CSV
load_to_csv(df, csv_path)
log_progress('Transformed data saved to CSV file.', log_path)

# Load to SQLite database
conn = sqlite3.connect(db_name)
load_to_db(conn, table_name, df)
log_progress(f'Data loaded into table {table_name} in database.', log_path)
conn.close()

# Connect to the SQLite database
conn = sqlite3.connect(db_name)

# Queries
queries = [
    "SELECT * FROM Largest_banks",
    "SELECT AVG(MC_GBP_Billion) FROM Largest_banks",
    "SELECT `Bank name` FROM Largest_banks LIMIT 5"  # Adjusted column name
]

# Execute and log each query
for query in queries:
    run_queries(query, conn)

# Close the database connection
conn.close()
