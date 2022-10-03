import json

from airflow import DAG
from datetime import datetime

from airflow.operators.python import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.providers.http.sensors.http import HttpSensor
from airflow.providers.postgres.operators.postgres import PostgresOperator

from utils import _store_airplane, _process_airplane, _extract_airplanes, _get_from_db, \
    _grouping_airplanes, _save_grouped

with DAG('final_test', start_date=datetime(2022, 9, 1),
         schedule_interval='@daily', catchup=True) as dag:
    create_table = PostgresOperator(
        postgres_conn_id='test_db',
        task_id='create_table',
        sql='''
            CREATE TABLE IF NOT EXISTS airplanes(
            departure TEXT NOT NULL, 
            call_sign TEXT NOT NULL,
            arrival TEXT NOT NULL,
            date date NOT NULL);''')

    extract_airplanes = PythonOperator(
        task_id='extract_airplane',
        python_callable=_extract_airplanes
    )

    process_airplanes = PythonOperator(
        task_id='process_airplane',
        python_callable=_process_airplane
    )

    store_airplanes = PythonOperator(
        task_id='store_airplane',
        python_callable=_store_airplane,
    )

    airplanes_from_db = PythonOperator(
        task_id='airplane_from_db',
        python_callable=_get_from_db
    )

    grouping_planes = PythonOperator(
        task_id='air_group',
        python_callable=_grouping_airplanes
    )

    create_table_group = PostgresOperator(
        postgres_conn_id='test_db',
        task_id='create_table_grouped',
        sql='''
                CREATE TABLE IF NOT EXISTS airplanes_grouped(
                date date NOT NULL, 
                amount int);''')

    save_result= PythonOperator(
        task_id='res_saving',
        python_callable=_save_grouped
    )

    create_table >> extract_airplanes >> process_airplanes >> store_airplanes >> create_table_group
    create_table_group >> airplanes_from_db >> grouping_planes >> save_result
