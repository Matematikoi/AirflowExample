from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
import filenames as io
import os
import tarfile
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from smtplib import SMTP


def scan_for_log_func():
    file_path = io.get_absolute_path(io.Filename.initial_log)
    if os.path.exists(file_path):
        return True
    else:
        return False


def extract_data_func():
    file_path = io.get_absolute_path(io.Filename.initial_log)
    file_extract = io.get_absolute_path(io.Filename.extracted_data)
    with open(file_path, 'r') as source_file:
        lines = source_file.readlines()
    ips = [line.split()[0] for line in lines]

    with open(file_extract, 'a') as destination_file:
        for value in ips:
            destination_file.write(value + '\n')


def transform_data_func():
    file_extract = io.get_absolute_path(io.Filename.extracted_data)
    file_transform = io.get_absolute_path(io.Filename.transformed_data)
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
    file_transform = io.get_absolute_path(io.Filename.transformed_data)
    tar_file = io.get_absolute_path(io.Filename.weblog)
    with tarfile.open(tar_file, 'w') as tar:
        tar.add(file_transform, arcname=os.path.basename(file_transform))


def send_message_func():
    # Email configuration
    sender_email = "testerlozano@gmail.com"
    receiver_email = "jose.lozano.dibildox@ulb.be"
    password = "zsjs fwxs dqdw szqe"  # App password

    # Message configuration (change content if you think another version is
    # more appropiate)
    subject = "Workload success"
    body = "The workflow has been executed"

    # Set up the MIMEText and MIMEMultipart
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = receiver_email
    message["Subject"] = subject

    # Attach the body of the email
    message.attach(MIMEText(body, "plain"))

    with SMTP("smtp.gmail.com", 587) as server:
        server.starttls()  # Start TLS for security
        server.login(sender_email, password)  # Log in to the email account
        server.sendmail(
            sender_email,
            receiver_email,
            message.as_string())  # Send the email
        server.quit()  # Quit the SMTP server


with DAG('process_web_log', start_date=datetime(2023, 1, 1),
         description='Workflow to transform a web server log file', tags=['info-h420'],
         schedule='@daily', catchup=False):

    scan_for_log = PythonOperator(
        task_id='scan_for_log',
        python_callable=scan_for_log_func)
    extract_data = PythonOperator(
        task_id='extract_data',
        python_callable=extract_data_func)
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data_func)
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_data_func)
    send_message = PythonOperator(
        task_id='send_message',
        python_callable=send_message_func)

    scan_for_log >> extract_data >> transform_data >> load_data >> send_message
