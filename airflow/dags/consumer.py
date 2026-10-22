"""Asset ile avtomatik baslayan Consumer DAG (Airflow 3.1.0)."""
import csv
import io

import pendulum
from airflow.sdk import DAG, Asset, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.standard.sensors.external_task import ExternalTaskSensor

BUCKET = "advance-airflow"
CONN_ID = "minio_s3"
SALES_ASSET = Asset("s3://advance-airflow/curated/sales.csv")


def producer_logical_date(_logical_date, **kwargs):
    # Consumer Asset event ile basladigi ucun onun logical_date-i yoxdur.
    # Producer event-e oz tarixini extra kimi elave edib.
    events = kwargs["context"]["triggering_asset_events"]
    sales_events = events.get(SALES_ASSET) or events.get(SALES_ASSET.uri) or []

    if not sales_events:
        raise ValueError("Sales Asset event tapilmadi. Producer DAG-ini trigger et.")

    date_text = sales_events[-1].extra.get("producer_logical_date")
    if not date_text:
        raise ValueError("Asset event-de Producer tarixi yoxdur. Producer-i yeni kodla tekrar islet.")

    return pendulum.parse(date_text)


with DAG(
    dag_id="consumer",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=[SALES_ASSET],
    catchup=False,
    tags=["consumer", "asset", "external-sensor"],
) as dag:

    # TASK 1: Producer DAG-indaki load_source task-ini gozleyir
    wait_for_ingestion = ExternalTaskSensor(
        task_id="wait_for_ingestion",
        external_dag_id="producer",
        external_task_id="load_source",
        execution_date_fn=producer_logical_date,
        allowed_states=["success"],
        failed_states=["failed"],
        poke_interval=10,
        timeout=300,
        mode="reschedule",
        deferrable=False,
    )

    # TASK 2: Curated sales faylini MinIO-dan oxuyur
    @task
    def mart_build():
        hook = S3Hook(aws_conn_id=CONN_ID)
        content = hook.read_key(
            key="curated/sales.csv",
            bucket_name=BUCKET,
        )
        rows = list(csv.DictReader(io.StringIO(content)))
        print(f"Total sales rows: {len(rows)}")
        print(f"Sales data: {rows}")

    wait_for_ingestion >> mart_build()
