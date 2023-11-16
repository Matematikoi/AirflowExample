from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import os
import tarfile

def scan_for_log_func():
    file_path = "/home/jclozdib/the_logs/log.txt"
    if os.path.exists(file_path):
    	return True
    else:
    	return False
		
def extract_data_func():
    file_path = "/home/jclozdib/the_logs/log.txt"
    file_extract = "/home/jclozdib/the_logs/extracted_data.txt"
    with open(file_path, 'r') as source_file:
    	lines = source_file.readlines()
    ips = [line.split()[0] for line in lines]
    
    with open(file_extract, 'a') as destination_file:
    	for value in ips:
    	     destination_file.write(value + '\n')
    	     
def transform_data_func():
    file_extract = "/home/jclozdib/the_logs/extracted_data.txt"
    file_transform = "/home/jclozdib/the_logs/transformed_data.txt"
    with open(file_extract, 'r') as source_file:
    	lines = source_file.readlines()
    ips = [line.split()[0] for line in lines]
    
    with open(file_transform, 'a') as destination_file:
    	counter = 1
    	for value in ips:
    	     if value != "198.46.149.143":
    	     	destination_file.write(f'{counter}. ' + value + '\n')
    	     	counter = counter + 1
    	     	
def load_data_func():
    file_transform = "/home/jclozdib/the_logs/transformed_data.txt"
    tar_file = "/home/jclozdib/the_logs/weblog.tar"
    with tarfile.open(tar_file, 'w') as tar:
    	tar.add(file_transform, arcname=os.path.basename(file_transform))

with DAG('process_web_log', start_date=datetime(2023, 1, 1),
         description='Workflow to transform a web server log file', tags=['info-h420'],
         schedule='@daily', catchup=False):

    scan_for_log = PythonOperator(task_id='scan_for_log', python_callable=scan_for_log_func)
    extract_data = PythonOperator(task_id='extract_data', python_callable=extract_data_func)
    transform_data = PythonOperator(task_id='transform_data', python_callable=transform_data_func)
    load_data = PythonOperator(task_id='load_data', python_callable=load_data_func)
    
    scan_for_log >> extract_data >> transform_data >> load_data
    