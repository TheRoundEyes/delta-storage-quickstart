import mysql.connector as connection
import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

df_names = ['orderDetails','customers','orders','payments','products']

def get_data_from_sql():
    db_connection = connection.connect(host='127.0.0.1', database='classicmodels', user='root', password='Freddie0211!')

    df_names = ['orderDetails','customers','orders','payments','products']
    for i,j in enumerate(df_names,0):
        for i,name in enumerate(df_names,0):
            j = pd.read_sql('SELECT * FROM ' + str(name), con=db_connection)
            j.to_csv(str(name)+'.csv')  
    # print(str(df_names) + " Fetched successfully")

dag = DAG('Delta_Storage_Pipeline',
        description='Delta Storage Pipeline ( Landing )',
        schedule_interval='@daily',
        start_date=datetime.utcnow())



get_data_tasks = PythonOperator(
    task_id = 'get_retails_raw_data',
    python_callable=get_data_from_sql,
    dag=dag
)


get_data_tasks