
from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


with DAG(
    dag_id="minio_dynamic_mapping",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["minio", "dynamic-mapping"],
) as dag:

    @task
    def discover_files():
        hook = S3Hook(aws_conn_id="minio_s3")

        files = hook.list_keys(
            bucket_name="advance-airflow"
        ) or []

        print("Discovered files:", files)
        return files

    @task
    def process_file(file_name: str):
        print(f"Processing file: {file_name}")

        if file_name.endswith(".csv"):
            print("CSV file detected")

        elif file_name.endswith(".json"):
            print("JSON file detected")

        else:
            raise ValueError(
                f"Unsupported file format: {file_name}"
            )

        print(f"Finished processing: {file_name}")

    process_file.expand(file_name=discover_files())