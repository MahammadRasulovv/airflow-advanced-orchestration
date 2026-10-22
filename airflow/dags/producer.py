"""Tasks 1-4: MinIO mapping, branching, trigger rules, asset publishing."""
import csv
import io
import json

import pendulum
from airflow.sdk import DAG, Asset, task, get_current_context
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.task.trigger_rule import TriggerRule

BUCKET = "advance-airflow"
CONN_ID = "minio_s3"
SALES_ASSET = Asset("s3://advance-airflow/curated/sales.csv")

with DAG(
    dag_id="producer",
    start_date=pendulum.datetime(2026, 10, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["producer", "mapping", "branching", "assets"],
) as dag:

    @task
    def load_source():
        """Ensure the expected source is available before publishing."""
        hook = S3Hook(aws_conn_id=CONN_ID)
        if not hook.check_for_key("sales.csv", bucket_name=BUCKET):
            raise FileNotFoundError(f"Missing {BUCKET}/sales.csv")
        print("Source sales.csv exists in MinIO")

    @task
    def discover_files():
        hook = S3Hook(aws_conn_id=CONN_ID)
        keys = hook.list_keys(bucket_name=BUCKET) or []
        files = sorted(k for k in keys if k.lower().endswith((".csv", ".json")))
        print(f"Files: {files}")
        return files

    @task
    def process_file(file_name):
        """A separate mapped task instance is created for each file."""
        kind = file_name.rsplit(".", 1)[-1].lower()
        print(f"Mapped processing: {file_name} ({kind})")
        return file_name

    @task.branch
    def select_branch(files):
        chosen = []
        if any(f.lower().endswith(".csv") for f in files):
            chosen.append("process_csv")
        if any(f.lower().endswith(".json") for f in files):
            chosen.append("process_json")
        print(f"Selected branches: {chosen}")
        return chosen

    @task
    def process_csv(files):
        hook = S3Hook(aws_conn_id=CONN_ID)
        for name in files:
            if name.lower().endswith(".csv"):
                text = hook.read_key(key=name, bucket_name=BUCKET)
                rows = list(csv.DictReader(io.StringIO(text)))
                print(f"CSV {name}: {len(rows)} rows")

    @task
    def process_json(files):
        hook = S3Hook(aws_conn_id=CONN_ID)
        for name in files:
            if name.lower().endswith(".json"):
                text = hook.read_key(key=name, bucket_name=BUCKET)
                data = json.loads(text)
                print(f"JSON {name}: {data}")

    @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    def join():
        print("Selected branches succeeded")

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def cleanup():
        # Demonstrate cleanup even if a branch was skipped or failed.
        print("Cleanup done")

    @task(outlets=[SALES_ASSET])
    def publish_sales():
        """Create an actual curated file, then emit its Airflow Asset event."""
        hook = S3Hook(aws_conn_id=CONN_ID)
        text = hook.read_key(key="sales.csv", bucket_name=BUCKET)
        rows = list(csv.DictReader(io.StringIO(text)))
        if not rows:
            raise ValueError("sales.csv has no data rows")
        hook.load_string(
            string_data=text,
            key="curated/sales.csv",
            bucket_name=BUCKET,
            replace=True,
        )
        # Consumer sensorunun dogru Producer run-ini tapmasi ucun
        context = get_current_context()
        context["outlet_events"][SALES_ASSET].extra = {
            "producer_logical_date": context["logical_date"].isoformat()
        }
        print(f"Published {len(rows)} rows to {SALES_ASSET.uri}")

    source = load_source()
    files = discover_files()
    mapped = process_file.expand(file_name=files)
    branch = select_branch(files)
    csv_task = process_csv(files)
    json_task = process_json(files)
    joined = join()
    cleaned = cleanup()
    published = publish_sales()

    source >> files
    mapped >> branch
    branch >> [csv_task, json_task]
    [csv_task, json_task] >> joined
    # Both branches feed cleanup; it runs when all chosen/skipped paths have ended.
    [csv_task, json_task, joined] >> cleaned
    # Publish only when JOIN actually succeeded AND cleanup finished.
    [joined, cleaned] >> published
