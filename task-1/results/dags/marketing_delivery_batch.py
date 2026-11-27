import csv
import shutil
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.email import send_email

DATA = "/opt/airflow/data"
STATUSES = ["created", "in_transit", "delivered", "returned"]
EMAIL = "marketing@marketing.local"


def read_orders(ds):
    rows = PostgresHook("orders_db").get_records("SELECT order_id::text FROM orders WHERE created_at::date = %s", parameters=(ds,))
    return [r[0] for r in rows]


def read_deliveries(ds):
    with open(f"{DATA}/inbox/{ds}.csv") as f:
        return list(csv.DictReader(f))


def check_quality(ti):
    orders = ti.xcom_pull(task_ids="read_orders")
    rows = ti.xcom_pull(task_ids="read_deliveries")
    clean = [r for r in rows if r["order_id"] in orders and r["status"] in STATUSES]
    ti.xcom_push(key="clean", value=clean)
    if len(clean) < len(rows) * 0.9:
        return "quarantine"
    return "load_mart"


def load_mart(ti, ds):
    rows = [(ds, r["order_id"], r["status"]) for r in ti.xcom_pull(task_ids="check_quality", key="clean")]
    hook = PostgresHook("orders_db")
    hook.run("DELETE FROM delivery_mart WHERE report_date = %s", parameters=(ds,))
    hook.insert_rows("delivery_mart", rows)


def quarantine(ds):
    shutil.move(f"{DATA}/inbox/{ds}.csv", f"{DATA}/quarantine/{ds}.csv")


def on_success(context):
    branch = context["ti"].xcom_pull(task_ids="check_quality")
    send_email(EMAIL, f"Пакет за {context['ds']} обработан", f"Ветка: {branch}")


def on_failure(context):
    send_email(EMAIL, f"Пакет за {context['ds']} не обработан", f"Запуск {context['run_id']} упал, подробности в Airflow")


with DAG(
    "marketing_delivery_batch",
    start_date=datetime(2026, 11, 1),
    schedule="0 3 * * *",
    catchup=False,
    default_args={"retries": 3, "retry_delay": timedelta(seconds=15), "retry_exponential_backoff": True},
    on_success_callback=on_success,
    on_failure_callback=on_failure,
):
    orders = PythonOperator(task_id="read_orders", python_callable=read_orders)
    deliveries = PythonOperator(task_id="read_deliveries", python_callable=read_deliveries)
    check = BranchPythonOperator(task_id="check_quality", python_callable=check_quality)
    mart = PythonOperator(task_id="load_mart", python_callable=load_mart)
    bad = PythonOperator(task_id="quarantine", python_callable=quarantine)

    [orders, deliveries] >> check >> [mart, bad]
