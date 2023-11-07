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

    for row in rows:
        col = row.find_all('td')
        if len(col)!= 0:
            if col[0].find('a') is not None and '—' not in col[2]:
                data_dict = {"Country": col[0].a.contents[0], 
                             "GDP_USD_millions": col[2].contents[0]}
                df1 = pd.DateFrame(data_dict, index =[0])
                df = pd.concat([df,df1], ignore_index=True)



    return df

def transform(df):
    ''' This function converts the GDP information from Currency
    format to float value, transforms the information of GDP from
    USD (Millions) to USD (Billions) rounding to 2 decimal places.
    The function returns the transformed dataframe.'''

    return df

def load_to_csv(df, csv_path):
    ''' This function saves the final dataframe as a `CSV` file 
    in the provided path. Function returns nothing.'''

def load_to_db(df, sql_connection, table_name):
    ''' This function saves the final dataframe as a database table
    with the provided name. Function returns nothing.'''

def run_query(query_statement, sql_connection):
    ''' This function runs the stated query on the database table and
    prints the output on the terminal. Function returns nothing. '''

def log_progress(message):
    ''' This function logs the mentioned message at a given stage of the code execution to a log file. Function returns nothing'''

''' Here, you define the required entities and call the relevant 
functions in the correct order to complete the project. Note that this
portion is not inside any function.'''