# Copyright (c) 2020, eQualit.ie inc.
# All rights reserved.
#
# This source code is licensed under the BSD-style license found in the
# LICENSE file in the root directory of this source tree.


import os

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta

start_date = (datetime.utcnow() - timedelta(minutes=1)
              ).strftime("%Y-%m-%d %H:%M:%S %z")
email = os.environ.get('AIRFLOW_EMAIL')
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': start_date,
    'email': [email],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

periodic_gc_dag = DAG(
    'periodic_gc',
    default_args=default_args,
    schedule_interval=timedelta(minutes=10, seconds=30)
)

templated_command = """
pgrep -f 'org.apache.spark.executor.CoarseGrainedExecutorBackend'| while read -r line ; do
  echo "Processing $line"
  jcmd $line GC.run;
done
"""
periodic_gc = BashOperator(
    task_id='perform_gc',
    bash_command='date',
    dag=periodic_gc_dag
)
