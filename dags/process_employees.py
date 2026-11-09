import datetime
import pendulum
import os

from airflow.sdk import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

@dag(
    dag_id="process_employees",
    schedule="0 0 * * *",
    start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
    catchup=False,
    dagrun_timeout=datetime.timedelta(minutes=60),
)
def ProcessEmployees():
    create_employees_table = SQLExecuteQueryOperator(
        task_id="create_employees_table",
        conn_id="miae_pg_conn",
        sql="""
        CREATE TABLE IF NOT EXISTS employees (
            "Serial Number" NUMERIC PRIMARY KEY,
            "Company Name" TEXT,
            "Employee Markme" TEXT,
            "Description" TEXT,
            "Leave" INTEGER
        );""",
    )

    create_employees_temp_table = SQLExecuteQueryOperator(
        task_id="create_employees_temp_table",
        conn_id="miae_pg_conn",
        sql="""
        DROP TABLE IF EXISTS employees_temp;
        CREATE TABLE employees_temp (
            "Serial Number" NUMERIC PRIMARY KEY,
            "Company Name" TEXT,
            "Employee Markme" TEXT,
            "Description" TEXT,
            "Leave" INTEGER
        );""",
    )

    @task
    def get_data():
        import psycopg2

        data_path = "/opt/airflow/dags/pipeline_example.csv"
        postgres_hook = PostgresHook(postgres_conn_id="miae_pg_conn")

        # Airflow 3: get_conn() returns a SQLAlchemy connection without
        # copy_expert, so we open a raw psycopg2 connection from the hook's URI.
        conn = psycopg2.connect(postgres_hook.get_uri().replace("+psycopg", ""))
        cur = conn.cursor()
        try:
            with open(data_path, "r") as file:
                cur.copy_expert(
                    "COPY employees_temp FROM STDIN WITH CSV HEADER DELIMITER AS ',' QUOTE '\"'",
                    file,
                )
            conn.commit()
        finally:
            cur.close()
            conn.close()

    @task
    def merge_data():
        import psycopg2

        query = """
        INSERT INTO employees
        SELECT *
        FROM (
            SELECT DISTINCT *
            FROM employees_temp
        ) t
        ON CONFLICT ("Serial Number") DO UPDATE
        SET
            "Employee Markme" = excluded."Employee Markme",
            "Description" = excluded."Description",
            "Leave" = excluded."Leave";
        """
        postgres_hook = PostgresHook(postgres_conn_id="miae_pg_conn")
        conn = psycopg2.connect(postgres_hook.get_uri().replace("+psycopg", ""))

        cur = conn.cursor()
        try:
            cur.execute(query)
            conn.commit()
            return 0
        except Exception:
            conn.rollback()
            return 1
        finally:
            cur.close()
            conn.close()

    [create_employees_table, create_employees_temp_table] >> get_data() >> merge_data()

dag = ProcessEmployees()

data_path = "/opt/airflow/dags/pipeline_example.csv"