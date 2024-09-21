from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from stock_pipeline import fetch_stock_data, store_data_to_db

default_args = {
    'owner': 'MKS',
    'start_date': datetime(2024, 7, 20),
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

with DAG('stock_data_pipeline',
         default_args=default_args,
         schedule='@daily', 
         catchup=False) as dag:

    def stream_data():
        symbol = 'AAPL'  
        api_key = 'O588RI6I361SDH0V' 

        db_url = 'sqlite:///stock_data.db'

        df = fetch_stock_data(symbol, api_key)

        store_data_to_db(df, db_url)

    streaming_task = PythonOperator(
        task_id='fetch_and_store_stock_data',
        python_callable=stream_data
    )

    streaming_task 
