
from datetime import datetime

from airflow.sdk import DAG, task


with DAG(
    dag_id="minio_connection_test",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["minio", "connection-test"],
):

    @task
    def test_minio():
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        hook = S3Hook(aws_conn_id="minio_s3")

        files = hook.list_keys(
            bucket_name="advance-airflow"
        ) or []

        print("MinIO files:", files)
        return files

    test_minio()