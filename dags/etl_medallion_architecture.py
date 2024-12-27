from datetime import datetime
from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

default_args = {
    'owner': 'pet_project',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
}

database_connection_id = "POSTGRES_DEFAULT"

# Создание DAG
with DAG(
    dag_id='etl_medallion_architecture',
    default_args=default_args,
    description='A DAG to run ETL processes',
    schedule_interval='0 4 * * 7', #every sunday at 4am
    start_date=datetime(2024, 1, 1),
    catchup=False,
) as dag:

    # Выполнение первой процедуры
    from_external_to_bronze = PostgresOperator(
        task_id='from_external_to_bronze',
        postgres_conn_id=database_connection_id,
        sql="CALL bronze_layer.from_external_to_bronze();",
    )

    # Выполнение второй процедуры
    from_bronze_to_silver = PostgresOperator(
        task_id='from_bronze_to_silver',
        postgres_conn_id=database_connection_id,
        sql="CALL silver_layer.from_bronze_to_silver();",
    )

    # Выполнение третьей процедуры
    from_silver_to_gold = PostgresOperator(
        task_id='from_silver_to_gold',
        postgres_conn_id=database_connection_id,
        sql="CALL gold_layer.from_silver_to_gold();",
    )

    # Последовательность выполнения задач
    from_external_to_bronze >> from_bronze_to_silver >> from_silver_to_gold
