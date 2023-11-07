# 2023-11-07, KN, Code for ETL operations on Country-GDP data

# Importing the required libraries
import requests
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3 
import numpy as np 
from datetime import datetime

# Initialize all known entities
url = 'https://web.archive.org/web/20230902185326/https://en.wikipedia.org/wiki/List_of_countries_by_GDP_%28nominal%29'
table_attribs = ['Country', 'GDP_USD_millions']
db_name = 'World_Economies.db'
table_name = 'Countries_by_GDP'
csv_path = './Countries_by_GDP.csv'


def extract(url, table_attribs):
    ''' This function extracts the required
    information from the website and saves it to a dataframe. The
    function returns the dataframe for further processing. '''

    html_page = requests.get(url).text  # Extract the webpage as text
    data = BeautifulSoup(html_page, 'html.parser') # Parse the text into an HTML object
    df = pd.DataFrame(columns=table_attribs) # Create a pandas dataframe with table attributes as columns
    tables = data.find_all('tbody') # Extract all 'tbody' attributes of the HTML object 
    rows = tables[2].find_all('tr') # Extract all the rows of the index 2 table using the 'tr' attribute.

    #check the contents of each row with attribute td, to make sure 1. the row is not empty, 2. the first column contains hyper link, 3. the third column doesn't contain '-'
    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0], 
                             "GDP_USD_millions": col[2].contents[0]} #store all entries matching the conditions to a dictionary with keys same as entries in table_attrib 
                df1 = pd.DateFrame(data_dict, index =[0]) #create a new dataframe with the data in the dictionary
                df = pd.concat([df,df1], ignore_index=True) #append all these dictionaries to the original dataframe

    return df


def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''
    
    GDP_list = df['GDP_USD_millions'].tolist()  #convert the contents of a column into a list
    GDP_list = [float("".join(x.split(',')))for x in GDP_list] #change all entries in the list into floats
    GDP_list = [np.round(x/1000,2)for x in GDP_list] #divide by 1000 and round to 2 decimal places
    df["GDP_USD_millions"] = GDP_list  #assign the modified list back to the dataframe column
    df=df.rename(columns = {"GDP_USD_millions":"GDP_USD_billions"}) #rename the column 
    
    return df


def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. '''
    
    df.to_csv(csv_path)


def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. '''

    df.to_sql(table_name, sql_connection, if_exists='replace', index=False)


def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal.'''

    print(query_statement)
    query_output = pd.read_sql(query_statement, sql_connection)
    print(query_output)

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    with open("./etl_project_log.txt","a") as f: 
        f.write(timestamp + ',' + message + '\n') 

