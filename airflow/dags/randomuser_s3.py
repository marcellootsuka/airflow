from airflow import DAG
from datetime import datetime
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

import json
import pandas as pd
from io import StringIO, BytesIO

compression = Variable.get("compression_randomuser")
bucket_name = Variable.get("bucket_name_randomuser")

default_args = {
    'owner' : 'DaniloSousa',
    'depends_on_past' : False,
    'retries' : 1
}

def processing_user(ti,datasource,ds, compression,arq_type):
    s3 = S3Hook()
    users = ti.xcom_pull(task_ids=['get_users'])
    pd.set_option('display.max_columns', None)
    if not len(users) or 'results' not in users[0]:
        raise ValueError('User is empty')
    df = pd.json_normalize(users[0]['results'])
    df.info()
    #schema = build_table_schema(df)
    df2 = pd.DataFrame(df,dtype=str)
    df2.info()
    print("df2.head: ) ",df2.head())

    if arq_type == 'csv':
        out_buffer = StringIO()
        s3key = datasource + '/' + ds + '/' + datasource + '/' + datasource + '.csv'
        df2.to_csv(out_buffer, index=False,compression=compression)
        s3.load_string(bytes_data=out_buffer.getvalue(), bucket_name=bucket_name,
                      replace=True, key=s3key)
    if arq_type == 'parquet':
        out_buffer = BytesIO()
        s3key = datasource + '/' + ds + '/' + datasource + '/' + datasource + '.parquet'
        df2.to_parquet(out_buffer, index=False,compression=compression)
        s3.load_bytes(bytes_data=out_buffer.getvalue(), bucket_name=bucket_name,
                      replace=True, key=s3key)

with DAG('randomuser_s3', schedule_interval='@daily', default_args=default_args, catchup=False, start_date=datetime.now()) as dag:
    api_available = HttpSensor(
        task_id = 'api_availble',
        http_conn_id = 'randomuser_api',
        endpoint = '/api'
    )
    get_users = SimpleHttpOperator(
        task_id = 'get_users',
        http_conn_id = 'randomuser_api',
        endpoint= '/api/?results=10',
        method = 'GET',
        response_filter = lambda response: json.loads(response.text),
        log_response=True
    )
    processing_user = PythonOperator(
        task_id = 'processing_user',
        python_callable=processing_user,
        op_kwargs={'datasource': 'randomuser', 'compression': compression, 'arq_type' : 'parquet'}
    )
api_available >> get_users >> processing_user
