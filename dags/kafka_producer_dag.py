from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import csv
import os
import pandas as pd
import json
from airflow.providers.apache.kafka.operators.produce import ProduceToTopicOperator

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023,8,11),
    'retries':1,
    'retry_delay':timedelta(minutes=30)
}

def start():
    print('Hello World')

# def read_csv():
#     AIRFLOW_HOME = os.getenv('AIRFLOW_HOME');
#     path = AIRFLOW_HOME + '/dags/airtravel.csv'
#     my_list = []
#     with open(r''+path,'r') as file:
#         csvReader = csv.reader(file)
#         for line in csvReader:
#             my_list.append(line)
#             print(line)
#     return my_list

def read_csv_transform_to_json():
    AIRFLOW_HOME = os.getenv('AIRFLOW_HOME')
    path = AIRFLOW_HOME + '/dags/airtravel.csv'
    df = pd.read_csv(r''+path)

    json_data = df.to_json(orient='records',lines=False)
    return json_data
    
def push_data_to_kafka(datas):
    if not datas:
        raise 'No such data found in the XComs'
    print('The data is : ')
    json_array = json.loads(datas)
    print(json_array)
    for rec in json_array:
        key = rec['Month']
        value = rec
        print('key: ' + str(key) + ' value: ' + str(value))
        yield(
            json.dumps(key),
            json.dumps(value)
        )

with DAG(
    dag_id='producer_dag',
    default_args=default_args,
    # schedule_interval=timedelta(minutes=30)
) as dag:
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start
    )

    read_csv = PythonOperator(
        task_id='read_csv_file',
        python_callable=read_csv_transform_to_json
    )

    push_data_to_kafka = ProduceToTopicOperator(
        task_id='push_data_to_kafka',
        kafka_config_id='kafka_cloud_config',
        topic='first_topic',
        producer_function=push_data_to_kafka,
        producer_function_args=["{{ ti.xcom_pull(task_ids='read_csv_file')}}"],
        poll_timeout=2
    )

    start_task >> read_csv >> push_data_to_kafka


