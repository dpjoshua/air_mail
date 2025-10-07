import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.exceptions import AirflowException
from airflow.utils.email import send_email
from datetime import datetime, timedelta
import subprocess
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule  # Import TriggerRule

# Default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the external script to run
def run_external_script(**kwargs):
    """
    Goal: Runs an external Python script and captures the output.
    
    param data: Keyword arguments passed from the DAG context.
    return: None. Pushes the status of the script (either 'success' or 'failed') to XCom.
    
    This function runs an external Python script using subprocess and captures the output.
    If the script runs successfully, the status 'success' is pushed to XCom.
    If the script fails, the status 'failed' is pushed to XCom, and an exception is raised
    to mark the task as failed.
    """
    script_path = "/opt/airflow/dags/workflow_one.py"
    try:
        result = subprocess.run(['python', script_path], check=True, capture_output=True, text=True)
        print(result.stdout)
        # Push 'success' status to XCom if the script runs successfully
        kwargs['ti'].xcom_push(key='script_status', value='success')
    except subprocess.CalledProcessError as e:
        print(f"Error occurred: {e.stderr}")
        # Push 'failed' status to XCom if an error occurs
        kwargs['ti'].xcom_push(key='script_status', value='failed')
        raise AirflowException(f"Error running the external script: {e.stderr}")  # Raising error to mark task as failed

# DAG definition
with DAG(
        'my_python_operator_dag',
        default_args=default_args,
        schedule_interval=None,
        start_date=datetime(2025, 1, 1),
        catchup=False,
) as dag:
    
    # PythonOperator to run the script
    run_python_script = PythonOperator(
        task_id='run_python_script',
        python_callable=run_external_script,
        provide_context=True  # Enable context to access XCom
    )

    # Email notification based on the status of run_python_script
    def send_status_email(**kwargs):
        """
        Goal: Sends an email based on the execution status of the `run_python_script` task.
        
        param data: Keyword arguments passed from the DAG context.
        return: None
        
        This function checks the status of the `run_python_script` task from XCom
        and sends an email indicating whether the task was successful or failed.
        """
        # Pull the status of the previous task from XCom
        task_status = kwargs['ti'].xcom_pull(task_ids='run_python_script', key='script_status')
        
        # Email subject and content
        subject = f"Task Execution Status: {kwargs['dag'].dag_id}"
        if task_status == 'success':
            html_content = f"""<h3>Task Status</h3>
                               <p>Task <b>run_python_script</b> is <b>successful</b>.</p>"""
        else:
            html_content = f"""<h3>Task Status</h3>
                               <p>Task <b>run_python_script</b> has <b>failed</b>.</p>"""

        # Send an email
        send_email(
            to="pauljoshua.devarapalli@gmail.com",  #Email
            subject=subject,
            html_content=html_content
        )

    # PythonOperator to send an email after the script runs
    email_notification = PythonOperator(
        task_id='email_notification',
        python_callable=send_status_email,
        provide_context=True,  # Required to pass context variables like task instance
        trigger_rule='all_done'  # Set the trigger rule to all_done to ensure it runs regardless of the previous task's outcome
    )

# Define task dependencies
run_python_script >> email_notification
