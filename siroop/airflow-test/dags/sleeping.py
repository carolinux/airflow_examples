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


def custom_print(**context):
    '''This is a function that will run within the DAG execution'''
    time.sleep(1)
    logging.info("I am inside the plain python operation")
    logging.info("My context is {}".format(context))
    logging.info("Env var value available before starting up sched/server is {}".format(os.environ["AIRFLOW_HOME"]))
    #logging.info("Env var value available after starting up sched/server is {}".format(os.environ["FOO"]))

def raise_exception(**context):
    time.sleep(10)
    raise Exception("aaa")


def maybe_raise_exception(**context):
    time.sleep(10)
    val = random.randint(1,2)
    if val==1:
        raise Exception("aaa")

def run_final_task(**context):
    '''This is a function that will run within the DAG execution'''
    time.sleep(1)
    logging.info("I am teh final task")
    


succesful_task = PythonOperator(
    task_id='task_that_succeeds',
    provide_context=True,
    python_callable=custom_print,
    params={"foo":"bar"},
    dag=dag)

#fail_miserably = PythonOperator(
#    task_id='fail',
#    provide_context=True,
#    retry_delay=timedelta(seconds=20),
#    python_callable=raise_exception,
#    on_failure_callback=send_email,
#    retries=3,
#    params={"foo":"bar"},
#    dag=dag)

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
