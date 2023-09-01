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
SQL_INS = f'insert into lab_10_pavlov(message) values({MSG});'

args = {'owner': 'pavlov',
        'start_date': datetime(2023, 9, 1),
        'retries': 3,
        'retry_delay': timedelta(seconds=600)}


def check_and_create_table():
    # Подключение к БД PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id=GP_CONN_ID)
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Проверка наличия таблицы
    check_table_query = """
        SELECT EXISTS (SELECT FROM information_schema.tables WHERE table_name = 'lab_10_pavlov')
    """
    cursor.execute(check_table_query)
    table_exists = cursor.fetchone()[0]

    # Если таблица не существует, создаем ее
    if not table_exists:
        create_table_query = "CREATE TABLE lab_10_pavlov (message text)"
        cursor.execute(create_table_query)
        conn.commit()

    # Закрытие соединения
    cursor.close()
    conn.close()


def start_task(**kwargs):
    print('Start')


def finish_task(**kwargs):
    send_text = f'https://api.telegram.org/bot968097013:AAGfYL_p6CJmfcZctBN81MwEsmgZ4zeENX0/sendMessage?chat_id=-1001915901409&parse_mode=Markdown&text={MSG}'
    requests.get(send_text)
    print('Finish')


with DAG(DAG_NAME, description="Pavlov's DAG",
         schedule_interval='* * * * *',
         catchup=False,
         max_active_runs=1,
         default_args=args,
         params={'labels': {'env': 'prod', 'priority': 'high'}}) as dag:
    start_operator = PythonOperator(task_id='startSP',
                                    python_callable=start_task,
                                    provide_context=True)

    check_create_table_operator = PythonOperator(task_id='check_create_table',
                                                 python_callable=check_and_create_table,
                                                 provide_context=True
                                                 )

    finish_operator = PythonOperator(task_id='finishSP',
                                     python_callable=finish_task,
                                     provide_context=True)

    sql_ins = PostgresOperator(task_id='GP_SP',
                               sql=SQL_INS,
                               postgres_conn_id=GP_CONN_ID,
                               autocommit=True)

    start_operator >> check_create_table_operator >> sql_ins >> finish_operator
