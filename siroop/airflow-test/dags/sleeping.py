from __future__ import print_function
import os
from builtins import range
from airflow.operators import PythonOperator
from airflow.models import DAG
from datetime import datetime, timedelta

import time
from pprint import pprint
import logging
import random

seven_days_ago = datetime(2017,3,8,16,14,0)

args = {
    'owner': 'airflow',
    'start_date': seven_days_ago,
}

dag = DAG(
    dag_id='siroop_showcase_dag_simple', default_args=args,
    schedule_interval="0 0 * * * ")


def run_successful_task(**context):
    '''This is a function that will run within the DAG execution'''
    time.sleep(10)
    logging.info("I am inside the plain python operation")
    logging.info("My context is {}".format(context))
    logging.info("Pushing a val thru xcom")
    context['ti'].xcom_push(key='my_val', value=42)



def maybe_raise_exception(**context):
    time.sleep(1)
    val = random.randint(1,2)
    if val==1:
        raise Exception("aaa")

def run_final_task(**context):
    '''This is a function that will run within the DAG execution'''
    time.sleep(1)
    context['ti'].xcom_push(key='my_val', value=42)
    logging.info("I am teh final task")
    val = context['ti'].xcom_pull(key="my_val", task_ids="task_that_suceeds")
    logging.info("Value read from previous task via xcom: {}".format(val))
    


succesful_task = PythonOperator(
    task_id='task_that_succeeds',
    provide_context=True,
    python_callable=run_successful_task,
    params={"foo":"bar"},
    dag=dag)


flaky_task_that_works_some_of_the_time = PythonOperator(
    task_id='flaky_task_that_works_some_of_the_time',
    provide_context=True,
    retry_delay=timedelta(seconds=5),
    python_callable=maybe_raise_exception,
    #on_failure_callback=send_email,
    retries=1,
    params={"foo":"bar"},
    dag=dag)

final_task = PythonOperator(
    task_id='final_task',
    provide_context=True,
    retry_delay=timedelta(seconds=5),
    python_callable=run_final_task,
    retries=3,
    trigger_rule="all_done",
    dag=dag)

final_task.set_upstream(flaky_task_that_works_some_of_the_time)
final_task.set_upstream(succesful_task)
