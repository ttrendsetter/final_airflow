import json
import datetime
from dateutil.parser import parse
from airflow.providers.postgres.hooks.postgres import PostgresHook
from pandas import json_normalize
import requests
from time import sleep
from pandas import DataFrame

def _extract_airplanes(ti, **kwargs):
    result = {}
    execution_date = parse(str(kwargs["execution_date"]))
    result['execution_date'] = str(execution_date.date())
    data = []
    with requests.Session() as session:
        for hours in range(0, 22, 2):
            start = int((execution_date + datetime.timedelta(hours=hours)).timestamp())
            end = int((execution_date + datetime.timedelta(hours=hours + 2)).timestamp())
            response = session.get(f'https://opensky-network.org/api/flights/all?begin={start}&end={end}')
            response = response.text
            if response:
                data.extend(json.loads(response))
            sleep(0.3)
    result['data'] = data
    ti.xcom_push(key='parse_res', value=result)


def _process_airplane(ti, **kwargs):
    airplanes = ti.xcom_pull(key='parse_res')

    processed_airplanes = json_normalize([{
        'departure': plane['estDepartureAirport'],
        'call_sign': plane['callsign'],
        'arrival': plane['estArrivalAirport'],
        'date': airplanes['execution_date']
    } for plane in airplanes['data']])

    processed_airplanes.to_csv(f'/tmp/processed_planes{kwargs["run_id"]}.csv', index=None, header=False, )


def _store_airplane(**kwargs):
    hook = PostgresHook(postgres_conn_id='test_db')
    hook.copy_expert(sql="COPY airplanes FROM stdin WITH DELIMITER ','",
                     filename=f'/tmp/processed_planes{kwargs["run_id"]}.csv')


def _get_from_db(ti, **kwargs):
    hook = PostgresHook(postgres_conn_id='test_db')
    request = f"SELECT * FROM airplanes where date = '{str(kwargs['execution_date'].date())}'"
    connection = hook.get_conn()
    cursor = connection.cursor()
    cursor.execute(request)
    res = [(i[0], i[1], i[2], str(i[3])) for i in cursor.fetchall()]
    ti.xcom_push(key='res', value=res)


def _grouping_airplanes(ti, **kwargs):
    dep_data = ti.xcom_pull(key='res')
    df = DataFrame(dep_data, columns=['departure', 'call', 'arrival', 'date'])
    df = df.groupby(['date']).count()
    res = {'date': str(kwargs['execution_date'].date())}
    try:
        res['amount'] = df['departure'][0]
    except IndexError:
        res['amount'] = 0
    print(res)
    res = json_normalize(res)
    res.to_csv(f'/tmp/grouped_planes{kwargs["run_id"]}.csv', index=None, header=False, )


def _save_grouped(ti, **kwargs):
    hook = PostgresHook(postgres_conn_id='test_db')
    hook.copy_expert(sql="COPY airplanes_grouped FROM stdin WITH DELIMITER ','",
                     filename=f'/tmp/grouped_planes{kwargs["run_id"]}.csv')
