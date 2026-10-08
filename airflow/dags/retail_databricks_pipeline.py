from datetime import datetime
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator

with DAG(
    dag_id="retail_databricks_pipeline",
    start_date=datetime(2026, 1, 1),
    schedule="@daily",
    catchup=False,
    tags=["azure", "databricks", "kafka", "retail"],
) as dag:

    run_databricks = DatabricksRunNowOperator(
        task_id="run_retail_databricks_job",
        databricks_conn_id="databricks_default",
        job_id="{{ var.value.retail_databricks_job_id }}",
    )

    run_databricks
