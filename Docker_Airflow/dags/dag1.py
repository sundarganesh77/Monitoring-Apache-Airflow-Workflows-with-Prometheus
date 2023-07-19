from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import statsd
try:
    from finance_calendars import finance_calendars as fc
except:
    pass
from subprocess import run

def download_nasdaq_dividends():
    start_date = datetime.now()
    end_date = start_date + timedelta(days=30)
    dividends_list = []
    for day in range((end_date - start_date).days + 1):
        current_date = start_date + timedelta(days=day)
        dividends = fc.get_dividends_by_date(current_date)
        dividends_list.append(dividends)
    
    df = pd.concat(dividends_list)
    df.to_csv('New Nasdaq Dividends data.csv', index=False)
    send_metrics(True)

def get_dividends_by_date(date):
    url = 'https://api.nasdaq.com/api/calendar/dividends'
    params = {'date': date.strftime('%Y-%m-%d')}
    response = requests.get(url, params=params)
    try:
        data = response.json()['data']
    except json.decoder.JSONDecodeError:
        # Log the error or handle it gracefully
        print(f"Failed to parse JSON response for date {date}.")
        data = []
    dividends = pd.DataFrame(data)
    return dividends

def send_metrics(success):
    client = statsd.StatsClient('localhost', 9125, prefix='airflow.nasdaq_dividends.')
    # Send the metrics using the client's `timing` method
    client.timing('test_api_call.orders-api.timer./v1/orders', 84)
    client.timing('test_api_call.orders-api.timer./v1/orders', 95)
    if success:
        client.incr('download_success')
    else:
        client.incr('download_failure')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 3, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'get_nasdaq_dividends',
    default_args=default_args,
    description='Download Nasdaq Dividends data for the next 30 days',
    schedule_interval=timedelta(minutes=10),
)

def install_finance_calendars():
    run(['pip', 'install', 'finance-calendars'])

t0 = PythonOperator(
    task_id='install_finance_calendars',
    python_callable=install_finance_calendars,
    dag=dag
)
t1 = PythonOperator(
    task_id='download_nasdaq_dividends',
    python_callable=download_nasdaq_dividends,
    dag=dag
)
t0 >> t1