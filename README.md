Air Mail

Air Mail is an Apache Airflow-based automation pipeline that executes a Python data processing script and automatically sends an email notification based on the success or failure of the run.

This project demonstrates how to use Airflow’s PythonOperator and EmailOperator to:

Run external Python scripts via Airflow.

Manage retries and failure handling.

Send automated status emails.

🧩 Project Overview

The Air Mail DAG (my_python_operator_dag) performs the following sequence:

Runs an external script (workflow_one.py) that:

Reads and processes city data.

Fetches weather information from an API.

Merges and stores the processed data into a SQLite database.

Sends an email notification indicating whether the script succeeded or failed.

This setup makes it easy to automate any external Python workflow while maintaining Airflow’s retry, logging, and alerting capabilities.

⚙️ Project Structure
air_mail/
│
├── dags/
│   ├── data_pipeline_dag.py        # Main Airflow DAG definition
│   ├── workflow_one.py             # External Python script (data processing logic)
│   └── cities.csv                  # Input CSV with city data
│
├── db/
│   └── merged_data.db              # Output SQLite database
│
├── docker-compose.yaml             # Docker setup for Airflow
├── requirements.txt                # Python dependencies
└── README.md                       # Project documentation

🚀 Getting Started
1. Clone the repository
git clone https://github.com/dpjoshua/air_mail.git
cd air_mail

2. Set up Airflow using Docker Compose

Make sure you have Docker and Docker Compose installed, then run:

docker-compose up -d


This starts the Airflow webserver, scheduler, and worker containers.

Once running, access the Airflow UI at:

🔗 http://localhost:8080

3. Configure environment variables

In your docker-compose.yaml, add:

environment:
  - EMAIL=your_email@example.com
  - AIRFLOW__SMTP__SMTP_HOST=smtp.gmail.com
  - AIRFLOW__SMTP__SMTP_PORT=587
  - AIRFLOW__SMTP__SMTP_STARTTLS=True
  - AIRFLOW__SMTP__SMTP_USER=your_email@example.com
  - AIRFLOW__SMTP__SMTP_PASSWORD=your_app_password
  - AIRFLOW__SMTP__SMTP_MAIL_FROM=your_email@example.com


⚠️ For Gmail users: use an App Password, not your regular account password.

Restart the containers after adding these values:

docker-compose down
docker-compose up -d

4. Trigger the DAG

In the Airflow UI:

Enable the DAG my_python_operator_dag.

Click “Trigger DAG” to start it manually.

You’ll see logs under each task:

run_python_script

email_notification

📧 Email Notifications

The DAG automatically sends an email with the result of the script:

✅ Success: “Task run_python_script is successful.”

❌ Failure: “Task run_python_script has failed.”

The email subject line will look like:

Task Execution Status: my_python_operator_dag

🧠 Key Components
Component	Description
PythonOperator	Executes the external script (workflow_one.py)
EmailOperator / send_email	Sends success/failure notifications
XCom	Shares data between tasks (success/failure flag)
AirflowException	Used for robust error handling
🧰 Tech Stack

Apache Airflow — Workflow orchestration

Python 3.7+

SQLite — Lightweight local database

Docker & Docker Compose

OpenWeatherMap API — Example weather data source

SMTP (Email) — For task notifications

📦 Dependencies

Example requirements.txt:

apache-airflow==2.8.0
pandas
sqlalchemy
requests

🧪 Troubleshooting
Issue	Cause	Fix
TypeError: expected str, bytes or os.PathLike	Missing script path	Update the DAG to use full path
TypeError: Received 'NoneType'	EMAIL not set	Set EMAIL env var
SMTP Authentication Error	Gmail security	Use App Password + STARTTLS
No logs / blank UI	Containers not healthy	Run docker-compose ps and restart Airflow
🏁 Next Steps

Add more scripts and automate multiple workflows.

Integrate Slack or Teams notifications.

Add sensors for data availability.

Use TaskGroup for modular workflow design.

Store credentials securely using Airflow Connections.

👨‍💻 Author

Paul Joshua
📧 pauljoshua.devarapalli@gmail.com

🔗 

🪪 License

This project is licensed under the MIT License — feel free to use, modify, and distribute.