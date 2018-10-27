from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta


'''
DAG used to run spark jobs on a daily basis
'''

# DAG settings
default_args = {
    'owner': 'rohith28',
    'depends_on_past': False,
    'start_date': datetime(2018, 10, 19),
    'email': ['rohithuppala28@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'schedule_interval': '@weekly',
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
  dag_id='movie_insight_dag', 
  description='DAG to run Movie Insights',
  default_args=default_args)

# Task 1 : MovieParser.py
task1 = BashOperator(
    task_id='finding',
    bash_command='cd /home/ubuntu/MovieInsights/src/; ./MovieParser.sh',
    dag=dag)

# second task: JsonParser.py
task2 = BashOperator(
    task_id='counting',
    bash_command='cd /home/ubuntu/MovieInsights/src/; ./JsonParser.sh',
    dag=dag)

# task2 executes only when task1 finishes
task2.set_upstream(task1)
# task2 >> task1
