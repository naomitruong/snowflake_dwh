import re
from datetime import timedelta

import pendulum
from airflow.sdk import Param, dag, task

TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
DBT = "/opt/airflow/dbt-venv/bin/dbt"
DBT_DIR = "/opt/airflow/project/dbt"


@dag(
    dag_id="dbt_daily",
    schedule="0 6 * * 1-5",  # 1 hour after raw_stock_daily (05:00)
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=5)},
    params={
        "event_date": Param(
            None,
            type=["null", "string"],
            description="YYYYMMDD to rebuild a past date; empty = today (Asia/Ho_Chi_Minh)",
        )
    },
    tags=["vn_mms", "dbt"],
)
def dbt_daily():
    @task
    def resolve_date(params=None) -> str:
        date = params["event_date"] or pendulum.now(TZ).strftime("%Y%m%d")
        if not re.fullmatch(r"[0-9]{8}", date):
            raise ValueError(f"event_date must be YYYYMMDD, got: {date}")
        return date

    @task.bash(cwd=DBT_DIR)
    def dbt_build(date: str) -> str:
        return f"{DBT} build --vars '{{event_date: \"{date}\"}}'"

    dbt_build(resolve_date())


dbt_daily()
