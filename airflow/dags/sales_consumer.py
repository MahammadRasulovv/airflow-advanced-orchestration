
from airflow.sdk import DAG, task, Asset
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pendulum


sales_asset = Asset("s3://advance-airflow/curated/sales.txt")


with DAG(
    dag_id="sales_consumer",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule=[sales_asset],
    catchup=False,
) as dag:

    @task
    def consume_sales():
        hook = S3Hook(aws_conn_id="minio_s3")

        content = hook.read_key(
            key="curated/sales.txt",
            bucket_name="advance-airflow"
        )

        print(f"Received sales data: {content}")

    consume_sales()
