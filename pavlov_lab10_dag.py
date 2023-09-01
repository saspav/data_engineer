import requests
from airflow import DAG
from airflow import configuration
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

MSG = 'Павлов А.В.: Всем удачи! 🙂'
DAG_NAME = 'pavlov_dag'
GP_CONN_ID = 'pavlov_con'
SQL_INS = f"insert into lab_10_pavlov(message) values('{MSG}');"

args = {'owner': 'pavlov',
        'start_date': datetime(2023, 9, 1),
        'retries': 3,
        'retry_delay': timedelta(seconds=60)}


def start_task(**kwargs):
    print('Start')


def finish_task(**kwargs):
    send_text = f'https://api.telegram.org/bot968097013:AAGfYL_p6CJmfcZctBN81MwEsmgZ4zeENX0/sendMessage?chat_id=-1001915901409&parse_mode=Markdown&text={MSG}'
    requests.get(send_text)
    print('Finish')


with DAG(DAG_NAME, description="Pavlov_DAG",
         schedule_interval='* * * * *',
         catchup=False,
         max_active_runs=1,
         default_args=args,
         params={'labels': {'env': 'prod', 'priority': 'high'}}) as dag:
    start_operator = PythonOperator(task_id='startSP',
                                    python_callable=start_task,
                                    provide_context=True)

    finish_operator = PythonOperator(task_id='finishSP',
                                     python_callable=finish_task,
                                     provide_context=True)

    sql_ins = PostgresOperator(task_id='GP_SP',
                               sql=SQL_INS,
                               postgres_conn_id=GP_CONN_ID,
                               autocommit=True)

    start_operator >> sql_ins >> finish_operator
