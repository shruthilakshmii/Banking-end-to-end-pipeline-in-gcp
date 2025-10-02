from airflow import DAG
from airflow.utils.dates import datetime, timedelta, days_ago
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator


# Default arguments for the DAG
default_args = {
    "Owner": "store_procedure",
    "email": "shruthilaksmi27@gmail.com",
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=1),
    "start_date": days_ago(1),
    "schedule_interval": "@daily",
    "catchup": False
}

with DAG(
    "Data_Ingestion_SP_SCD1_from_GCS_bqstgraw",
    default_args=default_args,
    description="Ingesting Customer Raw JSON Data From GCS to BigQuery Staging and Load it By Applying SCD-1 to BigQuery Raw",
    template_searchpath="/home/airflow/gcs/dags/script",
    max_active_runs=1,
    concurrency=1,
) as dag:



    # Step 1: Create external table from GCS JSON data
    create_external_table = BigQueryExecuteQueryOperator(
        task_id="create_external_table",
        sql="""
            CREATE OR REPLACE EXTERNAL TABLE `usecases-471314.bank_raw.customer_staging`
            OPTIONS (
                FORMAT='csv',
                uris=['gs://bank_project1/customer.csv']
            );
        """,
        use_legacy_sql=False
    )

    # Step 2: Run stored procedure to apply SCD1 logic
    run_scd1_sp = BigQueryInsertJobOperator(
    task_id='run_scd1_stored_procedure',
    configuration={
        "query": {
            "query": "CALL `usecases-471314.bank_raw.sp_scd1_customer_update`();",
            "useLegacySql": False,
        }
    },
    location='us',  
    project_id='usecases-471314',
)


    # Define task dependencies
    create_external_table >> run_scd1_sp
