from datetime import datetime

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import get_current_context


@dag(
    dag_id="country_param_dag",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    params={
        "country": Param("AZ", type="string", enum=["AZ", "TR", "GE"]),
    },
)
def country_param_dag():

    @task
    def extract():
        context = get_current_context()
        print(f"ds={context['ds']}, country={context['params']['country']}")
        return {"sales": 1000}

    @task
    def transform(data):
        data["sales"] *= 1.1
        return data

    @task
    def load(data):
        print(f"Final data: {data}")

    # 5. BashOperator ilə Jinja
    show_dates = BashOperator(
        task_id="show_dates",
        bash_command=(
            "echo ds={{ ds }}, "
            "prev={{ macros.ds_add(ds, -1) }}, "
            "country={{ params.country }}"
        ),
    )

    # 5. Python task ilə macros (ayrı task)
    @task
    def show_macros():
        context = get_current_context()
        ds = context["ds"]
        prev_ds = context["macros"].ds_add(ds, -1)
        country = context["params"]["country"]
        print(f"ds={ds}, prev_ds={prev_ds}, country={country}")

    # 7. Bonus: Variable
    show_company = BashOperator(
        task_id="show_company_name",
        bash_command="echo Company: {{ var.value.company_name }}",
    )

    # 6. extract -> transform -> load -> show_dates -> show_macros -> show_company_name
    data = extract()
    transformed = transform(data)
    loaded = load(transformed)

    loaded >> show_dates >> show_macros() >> show_company


country_param_dag()