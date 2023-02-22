# import the libraries

from datetime import timedelta
# The DAG object; we'll need this to instantiate a DAG
from airflow import DAG
# Operators; we need this to write tasks!
from airflow.operators.bash_operator import BashOperator
# This makes scheduling easy
from airflow.utils.dates import days_ago

#defining DAG arguments

# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Ram Param',
    'start_date': days_ago(0),
    'email': ['ramesh@somemail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(seconds=2),
}

# defining the DAG

# define the DAG
dag = DAG(
    'ETL_Server_Access_Log_Processing',
    default_args=default_args,
    description='ETL pipeline text change',
    schedule_interval=timedelta(seconds=1),
)

# define the tasks

# define the first task         download task

download = BashOperator(
    task_id='download',
    bash_command='wget "https://cf-courses-data.s3.us.cloud-object-storage.appdomain.cloud/IBM-DB0250EN-SkillsNetwork/labs/Apache%20Airflow/Build%20a%20DAG%20using%20Airflow/web-server-access-log.txt"',
    dag=dag,
)

# define the second task        extract task

extract = BashOperator(
    task_id='extract',
    bash_command='cut -d "#" -f 1,4 ./web-server-access-log.txt > /home/project/airflow/dags/extracted-data_etl.txt',
    dag=dag,
)

# define the third task         transform task
transform = BashOperator(
    task_id='transform',
    bash_command='tr [:lower:] [:upper:] < /home/project/airflow/dags/extracted-data_etl.txt > /home/project/airflow/dags/transformed-data_etl.csv',
    dag=dag,
)

# define the fourth task        load task
load = BashOperator(
    task_id='load',
    bash_command='zip log.zip transformed-data_etl.csv',
    dag=dag,
)


# task pipeline

download >> extract >> transform >> load