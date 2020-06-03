import os

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

from baskerville.db.database_maintenance import maintain_db

now = datetime.utcnow().replace(hour=23, minute=00, second=00, microsecond=00)
email = os.environ.get('AIRFLOW_EMAIL')
# Monday is 0, Sunday is 6, run every sunday
start_date = (now + timedelta(days=6 - now.weekday())).replace(
    hour=23, minute=00, second=00, microsecond=00
)
schedule_interval = timedelta(days=7)

# start_date = (datetime.utcnow() - timedelta(minutes=1))
# schedule_interval = timedelta(minutes=1)

print(start_date)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date.strftime("%Y-%m-%d %H:%M:%S %z"),
    'email': [email],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

database_maintenance_dag = DAG(
    'database_maintenance',
    default_args=default_args,
    schedule_interval=schedule_interval,
    catchup=True
)

maintain_db_python_op = PythonOperator(
    task_id='database_maintenance',
    python_callable=maintain_db,
    dag=database_maintenance_dag
)
