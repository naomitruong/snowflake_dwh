from datetime import timedelta
import pendulum
from airflow.sdk import Param, dag, task
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

TZ = pendulum.timezone("Asia/Ho_Chi_Minh")
@dag(
    dag_id="raw_stock_daily",
    schedule = "0 5 * * 1-5",
    start_date=pendulum.datetime(2026, 11, 1, tz=TZ),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
    params={
        "target_date": Param(
            None,
            type=["null", "string"],
            description="running date",
        )
    },
    tags=["vn_mms", "raw"]
)

def raw_stock_daily():
    @task
    def resolve_date(params=None, dag_run=None) -> str:
        if params["target_date"]:
            return params["target_date"]
        run_time = dag_run.logical_date or dag_run.run_after
        return run_time.astimezone(TZ).strftime("%Y%m%d")
    
    @task.short_circuit
    def check_files(date: str) -> bool:
        from ingest.load_raw import files_ready

        return files_ready(date)

    @task
    def upload_to_stage(date: str) -> None:
        from ingest.load_raw import upload

        upload(date)

    @task
    def load_raw(date: str) -> None:
        from ingest.load_raw import load

        load(date)

    date = resolve_date()
    check_files(date) >> upload_to_stage(date) >> load_raw(date)

raw_stock_daily()