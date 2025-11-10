import sys
sys.path.append("/opt/airflow/src")

from datetime import datetime
from airflow import DAG  # type: ignore
from airflow.operators.python import PythonOperator  # type: ignore

from extract import extract_orders, extract_customers
from transform import clean_orders, clean_customers, build_monthly_country_sales
from load import (
    load_orders,
    load_customers,
    load_monthly_country_sales,
)

# ---- python callable'lar ----

def do_extract_orders():
    return extract_orders()

def do_transform_orders(**context):
    # önceki task'in return'ünü al
    ti = context["ti"]
    orders_df = ti.xcom_pull(task_ids="extract_orders")
    cleaned = clean_orders(orders_df)
    # temizlenmiş df'i bir sonrakine ver
    ti.xcom_push(key="orders_clean", value=cleaned.to_dict(orient="records"))

def do_load_orders(**context):
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="transform_orders", key="orders_clean")
    import pandas as pd
    df = pd.DataFrame(records)
    load_orders(df)

def do_build_analytics(**context):
    ti = context["ti"]
    records = ti.xcom_pull(task_ids="transform_orders", key="orders_clean")
    import pandas as pd
    df = pd.DataFrame(records)
    monthly = build_monthly_country_sales(df)
    load_monthly_country_sales(monthly)

def do_customers():
    customers = extract_customers()
    customers_clean = clean_customers(customers)
    if not customers_clean.empty:
        load_customers(customers_clean)

# ---- DAG ----

with DAG(
    dag_id="ecommerce_etl",
    start_date=datetime(2025, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "ecommerce"],
) as dag:

    extract_orders_task = PythonOperator(
        task_id="extract_orders",
        python_callable=do_extract_orders,
    )

    transform_orders_task = PythonOperator(
        task_id="transform_orders",
        python_callable=do_transform_orders,
        provide_context=True,
    )

    load_orders_task = PythonOperator(
        task_id="load_orders",
        python_callable=do_load_orders,
        provide_context=True,
    )

    build_analytics_task = PythonOperator(
        task_id="build_analytics",
        python_callable=do_build_analytics,
        provide_context=True,
    )

    customers_task = PythonOperator(
        task_id="etl_customers",
        python_callable=do_customers,
    )

    # sıra: extract -> transform -> load -> analytics
    extract_orders_task >> transform_orders_task >> load_orders_task >> build_analytics_task

    # customers bağımsız çalışsın istiyorsan:
    # customers_task
    # yok "en son customers da çalışsın" dersen:
    build_analytics_task >> customers_task
