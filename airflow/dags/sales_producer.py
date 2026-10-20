
from airflow.sdk import DAG, task, Asset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pendulum


sales_asset = Asset("s3://advance-airflow/curated/sales.txt")


with DAG(
    dag_id="sales_producer",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
) as dag:

    @task(outlets=[sales_asset])
    def publish_sales():
        hook = S3Hook(aws_conn_id="minio_s3")

        hook.load_string(
            string_data="Sales data published successfully!",
            key="curated/sales.txt",
            bucket_name="advance-airflow",
            replace=True
        )

        print("sales.txt uploaded to MinIO")

    publish_sales()
