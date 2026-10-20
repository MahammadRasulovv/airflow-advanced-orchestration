
import pendulum

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator


with DAG(
    dag_id="source_ingestion",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:

    # TASK 1: Diger DAG-i avtomatik basladir
    trigger_waiter = TriggerDagRunOperator(
        task_id="trigger_waiter",
        trigger_dag_id="source_waiter",
        logical_date="{{ logical_date.isoformat() }}",
        wait_for_completion=False,
    )

    # TASK 2: MinIO-ya fayl yukleyir
    @task
    def load_source():
        hook = S3Hook(aws_conn_id="minio_s3")

        hook.load_string(
            string_data="Source data loaded!",
            key="source/source.txt",
            bucket_name="advance-airflow",
            replace=True,
        )

        print("Source file uploaded to MinIO")

    trigger_waiter >> load_source()
