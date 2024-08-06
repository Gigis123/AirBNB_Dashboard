'''
=================================================
Milestone 3

Nama  : Achmad Abdillah Ghifari
Batch : FTDS-006-BSD

This program is made to create a dag in order to do a series of task which include fetching raw data from postgress, cleaning the raw data and turn it into a cleaned data, and to turn the data from sql to no sql using elastic search to analyze it using kibana. we will do this process using apache airflow and setting a schedule to repeat this process every 6.30 utc   

=================================================
'''

# importing library of datetime for scheduling
import datetime as dt
from datetime import timedelta

# importing library of airflow for creating the apache airflow   
from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator

# importing library of pandas for reading the sql and csv, psycopg2 for connecting sql to python, and elastic search for changing the data to nosql and so we can connect it to kibana
import pandas as pd
import psycopg2 as db
from elasticsearch import Elasticsearch

def queryPostgresql():
    '''
    This function is used to fetch the raw data from postgressql in order to be cleaned in the next process.

    Parameters:
    url: string - postgres
    database: string - airflow
    table: string - table_m3

    Return
    data: list of str - P2M3_A.A.Ghifari_data_raw.csv
     
    Contoh penggunaan:
    data = get_data_from_postgresql('postgres', 'airflow', 'table_m3')
    '''
    conn_string="dbname='airflow' host='postgres' user='airflow' password='airflow'"
    conn=db.connect(conn_string)
    df=pd.read_sql("select * from table_m3",conn)
    df.to_csv('/opt/airflow/dags/P2M3_A.A.Ghifari_data_raw.csv',index=False)
    print("-------Data Saved------")

def cleanAirbnb():
    '''
    This function is used to do data cleaning for the data fetched from postgres. this data will then be connected to nosql in the form of elastic search in the next process.

    Processes:
    reading the csv
    dropping unnecessary column
    dropping the duplicate column
    deleting the symbol for price and service fee the data
    filling missing value
    changing the data into the correct datatype
    modifying the column name
    saving the cleaned column

    Return
    data: list of str - P2M3_A.A.Ghifari_data_cleaned.csv
    '''
    # reading the csv saved previously
    data=pd.read_csv('/opt/airflow/dags/P2M3_A.A.Ghifari_data_raw.csv')
    # dropping the column license due to multiple missing values and redundant column (with no meaning and unimportant for the analysis)
    data = data.drop(['license'], axis=1)
    # dropping the row with name brookln (due to mistyping)
    data = data.drop(data[data['neighbourhood group'] == 'brookln'].index)
    # dropping the duplicate row in the dataset
    data = data.drop_duplicates()
    # deleting the $ and (,) from price and service fee to turn them into integer
    data['price'] = data['price'].str.replace('$', '').str.replace(',', '')
    data['service fee'] = data['service fee'].str.replace('$', '').str.replace(',', '')
    # filling the missing value by using median for numerical, mean for categorical, no data for data that cant use median or mean, or dropping the data entirely for both price and service fee
    num_cols = ['Construction year', 'minimum nights', 'number of reviews', 'reviews per month', 'review rate number', 'calculated host listings count', 'availability 365']
    cat_cols = ['host_identity_verified', 'neighbourhood group', 'neighbourhood', 'country', 'country code', 'instant_bookable', 'cancellation_policy', 'room type', 'last review']
    null_cols = ['NAME', 'host name', 'house_rules']
    for i in num_cols:
        data[i].fillna(data[i].median(), inplace=True)
    for i in cat_cols:
        data[i].fillna(data[i].mode()[0], inplace=True)
    for i in null_cols:
        data[i] = data[i].fillna('No Data')
    data.dropna(axis = 0, inplace = True)   
    # changing the datatype of instant bookable to boolean
    data.instant_bookable  = data.instant_bookable.astype(bool) 
    # changing the datatype of these columns to integer
    change_type = ['price', 'service fee', 'minimum nights', 'number of reviews', 'Construction year', 'review rate number', 'calculated host listings count', 'availability 365']
    for i in change_type:
        data[i] = data[i].astype(int) 
    # changing the last review column into datetime
    data['last review'] = pd.to_datetime(data['last review'])
    # changing the column name to all be lowercase
    data.columns = [x.lower() for x in data.columns]
    # changing the space in column name into _
    data.columns = data.columns.str.replace(' ', '_')
    # saving the csv of the cleaned data
    data.to_csv('/opt/airflow/dags/P2M3_A.A.Ghifari_data_cleaned.csv')

def insertElasticsearch():
    '''
    This function is used to connect the data to nosql using elastic search. this is used to index the data from the cleaned csv from the previous step.

    Parameters:
    index = frompostgresql
    doc_type = doc
    body = .json
    table = P2M3_A.A.Ghifari_data_cleaned.csv

    Return
    data: frompostgresql.json
    '''
    es = Elasticsearch('http://elasticsearch:9200') 
    df=pd.read_csv('/opt/airflow/dags/P2M3_A.A.Ghifari_data_cleaned.csv')
    for i,r in df.iterrows():
        doc=r.to_json()
        res=es.index(index="frompostgresql",doc_type="doc",body=doc)
        
        print(res)	

# defining the dag for the airflow
default_args = {
    'owner': 'gigis', # specifying the owner
    'start_date': dt.datetime(2024, 7, 19), # specify the start time to 19 july 2024
    'retries': 1, # retry the dag 1 in case of failure
    'retry_delay': dt.timedelta(minutes=15), # delay the retry time to 15 minute after the original
}

# creating a new dag called H8cleandata
with DAG('H8CleanData',
         default_args=default_args, # setting default args to the previously defined args
         schedule_interval= '30 6 * * *', # schedule the dag to run everyday every 6.30 utc
         ) as dag:

# define the task to fetch the raw data from postgresql
    Fetch_from_Postgresql = PythonOperator(task_id='queryPostgresql',
                                 python_callable=queryPostgresql)

# define the task to clean the data fetched previously
    Data_Cleaning = PythonOperator(task_id='cleanAirbnb',
                                 python_callable=cleanAirbnb)

# define the task to connect the clean data to elastic search for kibana
    Post_to_Elasticsearch = PythonOperator(task_id='insertElasticsearch',
                                 python_callable=insertElasticsearch)

# define the order of the task
Fetch_from_Postgresql >> Data_Cleaning >> Post_to_Elasticsearch
