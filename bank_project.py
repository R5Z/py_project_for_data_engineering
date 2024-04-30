import requests
import sqlite3
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime

url = 'https://web.archive.org/web/20230908091635 /https://en.wikipedia.org/wiki/List_of_largest_banks'
db_name = 'Banks.db'
csv_name = 'Largest_banks_data.csv'
table_name = 'Largest_Banks'
attribute_list_extract = ['Country', 'MC_USD_Billion']
attribute_list_final = ['Country','MC_USD_Billion','MC_GBP_Billion','MC_EUR_Billion','MC_INR_Billion']

csv_path = f'./{csv_name}'
db_path = f'./{db_name}'

log_file = "code_log.txt"
log_path = f'./{log_file}'

def log_progress(message): 
    log_file_path = log_path
    
    timestamp_format = '%Y-%h-%d-%H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second 
    now = datetime.now() # get current timestamp 
    timestamp = now.strftime(timestamp_format) 
    
    with open(log_file_path,"a") as f: 
        f.write(timestamp + ',' + message + '\n')

def extract(url):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page, 'html.parser')

    tables = data.find_all('tbody')
    rows = tables[0].find_all('tr')

    df_list = []

    count = 0

    for row in rows:
        if count < 10:
            col = row.find_all('td')
            if len(col) != 0:
                data_dict = {"Country": col[1].find_all('a')[1].get('title'),
                             "MC_USD_Billion": float(col[2].contents[0])}
                df_list.append(pd.DataFrame(data_dict, index=[0]))
                count += 1
        else:
            break

    df = pd.concat(df_list, ignore_index=True)
    return df


def transform(df, new_columns):
    columns_to_add = [col for col in new_columns if col not in attribute_list_extract]
    
    for column in columns_to_add:
        df[column] = None
    
    # Read exchange rates
    exchange_rates = pd.read_csv('./exchange_rate.csv')
    exchange_rates.set_index('Currency', inplace=True)

    # Convert USD to other currencies
    df['MC_GBP_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['GBP', 'Rate'],2)
    df['MC_EUR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['EUR', 'Rate'],2)
    df['MC_INR_Billion'] = round(df['MC_USD_Billion'] * exchange_rates.loc['INR', 'Rate'],2)

    return df

def load_to_csv(csv_path, df):
    df.to_csv(csv_path, index=False)

def load_to_db(db_path, df, table_name):
    conn = sqlite3.connect(db_path)
    df.to_sql(table_name, conn, if_exists='replace', index=False)
    conn.close()

def execute_query(db_path, query):
    conn = sqlite3.connect(db_path)
    df = pd.read_sql_query(query, conn)
    conn.close()
    print(df.to_string(index=False))
    print("\n")
    return df

log_progress('Preliminaries complete. Initiating ETL process')
df_extracted = extract(url)
print(df_extracted)

log_progress('Data extraction complete. Initiating Transformation process')
df_transformed = transform(df_extracted, attribute_list_final)
print(df_transformed)

log_progress('Data transformation complete. Initiating loading process')
load_to_csv(csv_path, df_transformed)
load_to_db(db_path, df_transformed, table_name)

log_progress('ETL Finished')


log_progress('Query for the entire table')
query = "SELECT * FROM Largest_Banks"
execute_query(db_path, query)

log_progress('Query for the average MC_GBP_Billion from all the banks')
query = "SELECT AVG(MC_GBP_Billion) FROM Largest_Banks"
execute_query(db_path, query)

log_progress('Query for the top 5 banks')
query = "SELECT * FROM Largest_Banks ORDER BY MC_USD_Billion DESC LIMIT 5"
execute_query(db_path, query)
