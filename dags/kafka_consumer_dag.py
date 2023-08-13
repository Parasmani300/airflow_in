from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime,timedelta
import csv
import os
import pandas as pd
import json
from airflow.providers.apache.kafka.operators.consume import ConsumeFromTopicOperator

default_args = {
    'owner':'airflow',
    'depends_on_past':False,
    'start_date':datetime(2023,8,11),
    'retries':1,
    'retry_delay':timedelta(minutes=30)
}

def start():
    print('Hello World')

def consume_records(message):
    # for message in messages:
    key = json.loads(message.key())
    value = json.loads(message.value())
    print(f"{message.topic()} @ {message.offset()}; {key} {value}")


with DAG(
    dag_id='consumer_dag',
    default_args=default_args,
    schedule_interval=timedelta(minutes=2)
) as dag:
    start_task = PythonOperator(
        task_id='start_task',
        python_callable=start
    )

    consumer_task = ConsumeFromTopicOperator(
        task_id='consume_reocrds',
        kafka_config_id='kafka_consumer_config',
        topics=['first_topic'],
        apply_function=consume_records,
        commit_cadence="end_of_batch",
        max_messages=20,
        max_batch_size=5,
    )

    start_task >> consumer_task