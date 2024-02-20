from datetime import timedelta 
from airflow import DAG 
from airflow.operators.bash_operator import BashOperator 
from airflow.utils.dates import days_ago 

default_args = {
    'owner': "VictorPractice",
    'start_date': days_ago(0),
    'email': ['tandoannrc@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    dag_id = 'ETL_toll_data',
    schedule_interval = timedelta(days=1),
    default_args=default_args,
    description = 'Apache Airflow Final Assignment',
)

unzip_data = BashOperator(
    task_id= 'unzip_data',
    bash_command= 'tar zxvf tolldata.tgz',
    dag=dag,
)

extract_data_from_csv = BashOperator(
    task_id = 'extract_data_from_csv',
    bash_command = 'cut -d"," -f1-4 vehicle-data.csv > csv_data.csv',
    dag=dag,
)
# tr -d "\r" is used to delete "\r" characters
extract_data_from_tsv = BashOperator(
    task_id = 'extract_data_from_tsv',
    bash_command = 'cut -f5-7 tollplaza-data.tsv | tr -d "\r" | tr "[:blank:]" "," > tsv_data.csv',
    dag=dag,
)

extract_data_from_fixed_width = BashOperator(
    task_id = 'extract_data_from_fixed_width',
    bash_command = 'cat payment-data.txt | tr -s "[:space:]" | cut -d" " -f11-12 | tr " " "," > fixed_width_data.csv',
    dag=dag,
)

consolidate_data = BashOperator(
    task_id = 'consolidate_data',
    bash_command = 'paste -d"," csv_data.csv tsv_data.csv fixed_width_data.csv > extracted_data.csv',
    dag=dag,
)

transform_data = BashOperator(
    task_id = 'transform_data',
    bash_command = 'awk "BEGIN{FS=","; OFS=","} {$4=toupper($4)} {print}" extracted_data.csv >transformed_data.csv',
    dag=dag,
)

unzip_data >> extract_data_from_csv >> extract_data_from_tsv >> extract_data_from_fixed_width >> consolidate_data >> transform_data