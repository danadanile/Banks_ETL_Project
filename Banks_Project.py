# Code for ETL operations on Banks data

import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
import numpy as np
from datetime import datetime
from io import StringIO

def log_progress(message):
    ''' This function logs the given message of a particular stage 
    of the code execution to a log file. Function returns nothing. '''

    log_file = "code_log.txt"
    timestamp_format = '%Y-%m-%d-%H:%M:%S'
    now = datetime.now()
    timestamp = now.strftime(timestamp_format)
    with open(log_file, "a") as f:
        f.write(timestamp + ': ' + message + '\n')

def extract(url, table_attribs):
    '''
    The purpose of this function is to extract the required
    information from the website and save it to a dataframe.
    The function returns the dataframe for further processing.
    '''

    log_progress("Starting data extraction.")
    
    # 1) Get the page content
    page = requests.get(url).text


    # 2) Parse the HTML with BeautifulSoup
    soup = BeautifulSoup(page, 'html.parser')

    # 3) Create an empty DataFrame, with columns as per 'table_attribs'
    df = pd.DataFrame(columns=table_attribs)

    # 4) Find all <tbody> tags
    tables = soup.find_all('tbody')
    if not tables:
        raise Exception("No <tbody> found in the page")

    # 5) Get all <tr> rows from the first <tbody>
    rows = tables[0].find_all('tr')
    if not rows:
        raise Exception("No <tr> rows found in the first <tbody>")

    # 6) Iterate over each <tr> to extract bank name & market cap
    for row in rows:
        if row.find('td') is not None:
            col = row.find_all('td')
            # Make sure we have enough columns in this row
            if len(col) < 3:
                continue

            # Extract Bank name
            try:
                bank_name = col[1].find_all('a')[1]['title']
            except (IndexError, KeyError, TypeError):
                # Fallback: if not found, use text from col[1]
                bank_name = col[1].get_text(strip=True)

            # Extract Market Cap, trimming the last character
            # (e.g., if it's a newline or trailing symbol)
            try:
                raw_market_cap = col[2].contents[0]
                market_cap_str = raw_market_cap[:-1]  # remove last char
                market_cap_float = float(market_cap_str)
            except (IndexError, ValueError):
                # If something is wrong, skip or set a default
                market_cap_float = 0.0

            # Create a row to append
            data_dict = {
                "Name": bank_name,
                "MC_USD_Billion": market_cap_float
            }

            # 7) Convert that row to a small DataFrame & append
            df_temp = pd.DataFrame([data_dict])
            df = pd.concat([df, df_temp], ignore_index=True)

    log_progress("Data extraction complete. Initiating Transformation process.")
    return df

def transform(df, csv_path):
    ''' 
    This function accesses the CSV file for exchange rate information,
    and adds three columns to the dataframe, each containing the 
    transformed version of MC_USD_Billion to respective currencies
    '''
    log_progress("Starting data transformation.")

    # 1) Read the exchange rate CSV
    exchange_rate_df = pd.read_csv(csv_path)
    
    # 2) Convert the CSV DataFrame to a dictionary, as in the sample code
    exchange_rate = exchange_rate_df.set_index('Currency').to_dict()['Rate']
    
    # 3) Create new columns for each currency
    df['MC_GBP_Billion'] = [np.round(x * exchange_rate['GBP'], 2) for x in df['MC_USD_Billion']]
    df['MC_EUR_Billion'] = [np.round(x * exchange_rate['EUR'], 2) for x in df['MC_USD_Billion']]
    df['MC_INR_Billion'] = [np.round(x * exchange_rate['INR'], 2) for x in df['MC_USD_Billion']]

    log_progress("Data transformation complete. Initiating Loading process.")
    return df

def load_to_csv(df, output_path):
    ''' 
    This function saves the final DataFrame as a 'CSV' file 
    in the provided path. Function returns nothing.
    (Matches the sample, with log statements kept.)
    '''
    log_progress("Saving data to CSV.")
    df.to_csv(output_path, index=False)
    log_progress("Data saved to CSV file.")

def load_to_db(df, sql_connection, table_name):
    '''
    This function saves the final data frame to a database
    table with the provided name. Function returns nothing.
    Matches the sample code snippet + logging.
    '''
    log_progress("Saving data to Database.")
    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)
    log_progress("Data loaded to Database as a table, Executing queries.")

def run_query(query_statement, sql_connection):
    ''' 
    This function runs the query on the database table and prints
    the output on the terminal. Function returns nothing.
    We keep the log if needed. 
    '''
    log_progress(f"Executing Query: {query_statement}")
    
    cursor = sql_connection.cursor()
    cursor.execute(query_statement)
    results = cursor.fetchall()

    print("\nQuery Statement:", query_statement)
    for row in results:
        print(row)

    log_progress("Query execution completed")

# --------------------------------------------------
# Orchestrate the ETL below
# --------------------------------------------------
if __name__ == "__main__":
    # 1) Log the start
    log_progress("Preliminaries complete. Initiating ETL process.")

    # 2) EXTRACT
    url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
    table_attributes = ["Name", "MC_USD_Billion"] 
    df_extracted = extract(url, table_attributes)

    # 3) TRANSFORM
    csv_path = "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBMSkillsNetwork-PY0221EN-Coursera/labs/v2/exchange_rate.csv"
    df_transformed = transform(df_extracted, csv_path)

    # 4) LOAD to CSV
    output_csv_path = "./Largest_banks_data.csv"
    load_to_csv(df_transformed, output_csv_path)

    # 5) LOAD to DB
    connection = sqlite3.connect('Banks.db')
    load_to_db(df_transformed, connection, "Largest_banks")

    # 6) QUERIES
    query_1 = "SELECT * FROM Largest_banks"
    query_2 = "SELECT AVG(MC_GBP_Billion) FROM Largest_banks"
    query_3 = "SELECT Name FROM Largest_banks LIMIT 5"

    run_query(query_1, connection)
    run_query(query_2, connection)
    run_query(query_3, connection)

    # 7) Close connection and log
    connection.close()
    log_progress("Server Connection closed. Process Complete.")
