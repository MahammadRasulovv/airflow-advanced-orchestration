
import pendulum

from airflow.sdk import DAG, task
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


with DAG(
    dag_id="source_waiter",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:

    # TASK 1: source_ingestion DAG-indaki load_source-u gozleyir
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="source_ingestion",
        external_task_id="load_source",
        allowed_states=["success"],
        failed_states=["failed"],
        timeout=300,
        poke_interval=10,
        mode="reschedule",
        deferrable=False,
    )

    # TASK 2: MinIO-dan source faylini oxuyur
    @task
    def mart_build():
        hook = S3Hook(aws_conn_id="minio_s3")

        content = hook.read_key(
            key="source/source.txt",
            bucket_name="advance-airflow",
        )

        print(f"Mart build started: {content}")

    wait_for_ingestion >> mart_build()
