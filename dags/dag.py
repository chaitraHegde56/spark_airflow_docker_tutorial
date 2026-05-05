from airflow import DAG
# from airflow.utils.dates import days_ago
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.task_group import TaskGroup
from airflow.utils.email import send_email
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=30),
    # "email_on_failure": False,
    # "email": ["chaihegde66@gmail.com"],
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    dag_id="revenue_aggregation_dag",
    default_args=default_args,
    description="7-day rolling revenue aggregation",
    schedule="0 2 * * *",
    start_date=datetime(2024, 6, 1),
    catchup=False,
    max_active_runs=1,  # prevents overlap
    tags=["revenue", "spark", "production"],
    sla_miss_callback=None,
) as dag:

    start = EmptyOperator(task_id="start")

    # 2️⃣ Spark Job Submission
    spark_job = SparkSubmitOperator(
        task_id="run_revenue_aggregation",
        application="/opt/airflow/spark_jobs/job.py",
        name="revenue_aggregation",
        conn_id="spark_default",
        # master="spark://spark-master:7077",
        deploy_mode="client",
        packages="org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
        verbose=True,
        
    )

    end = EmptyOperator(task_id="end")

    start >> spark_job >> end