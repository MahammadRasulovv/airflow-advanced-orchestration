
import csv
import io
import json
from datetime import datetime

from airflow.sdk import DAG, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.utils.trigger_rule import TriggerRule


BUCKET = "advance-airflow"
CONN_ID = "minio_s3"


with DAG(
    dag_id="csv_json_branching",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["minio", "branching", "trigger-rules"],
) as dag:

    # TASK 1: MinIO-dan fayl siyahisini goturur
    @task
    def get_file_list():
        hook = S3Hook(aws_conn_id=CONN_ID)

        files = hook.list_keys(
            bucket_name=BUCKET
        ) or []

        files = sorted(
            file_name
            for file_name in files
            if file_name.lower().endswith((".csv", ".json"))
        )

        print(f"Discovered files: {files}")
        return files

    # TASK 2: CSV ve JSON branch-lerini secir
    @task.branch
    def select_branch(files):
        branches = []

        if any(
            file_name.lower().endswith(".csv")
            for file_name in files
        ):
            branches.append("process_csv")

        if any(
            file_name.lower().endswith(".json")
            for file_name in files
        ):
            branches.append("process_json")

        print(f"Selected branches: {branches}")

        return branches

    # TASK 3: Butun CSV fayllarini emal edir
    @task
    def process_csv(files):
        hook = S3Hook(aws_conn_id=CONN_ID)

        for file_name in files:
            if not file_name.lower().endswith(".csv"):
                continue

            content = hook.read_key(
                key=file_name,
                bucket_name=BUCKET
            )

            rows = list(
                csv.DictReader(io.StringIO(content))
            )

            print(f"CSV file: {file_name}")
            print(f"Total rows: {len(rows)}")
            print(f"Data: {rows}")

    # TASK 4: Butun JSON fayllarini emal edir
    @task
    def process_json(files):
        hook = S3Hook(aws_conn_id=CONN_ID)

        for file_name in files:
            if not file_name.lower().endswith(".json"):
                continue

            content = hook.read_key(
                key=file_name,
                bucket_name=BUCKET
            )

            data = json.loads(content)

            print(f"JSON file: {file_name}")
            print(f"Data: {data}")

    # TASK 5: JOIN
    @task(
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )
    def join():
        print("Selected branches completed successfully")

    # TASK 6: CLEANUP
    @task(
        trigger_rule=TriggerRule.ALL_DONE
    )
    def cleanup():
        print("Cleanup executed")
        print("All upstream tasks have finished")

    # TASK DEPENDENCIES

    files = get_file_list()

    branch = select_branch(files)

    csv_task = process_csv(files)
    json_task = process_json(files)

    join_task = join()
    cleanup_task = cleanup()

    branch >> [csv_task, json_task]

    [csv_task, json_task] >> join_task

    join_task >> cleanup_task