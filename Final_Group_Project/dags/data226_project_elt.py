from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

DBT_DIR = "/opt/airflow/dbt/final_project"

# ---- pull connection + build safe env dict (never None) ----
conn = BaseHook.get_connection("snowflake_conn")
def _s(v):  # str-or-empty
    return "" if v is None else str(v)

default_env = {
    "DBT_USER": _s(conn.login),
    "DBT_PASSWORD": _s(conn.password),
    "DBT_ACCOUNT": _s(conn.extra_dejson.get("account")),
    "DBT_SCHEMA": _s(conn.schema or conn.extra_dejson.get("schema") or "ANALYTICS"),
    "DBT_DATABASE": _s(conn.extra_dejson.get("database")),
    "DBT_ROLE": _s(conn.extra_dejson.get("role")),
    "DBT_WAREHOUSE": _s(conn.extra_dejson.get("warehouse")),
    "DBT_TYPE": "snowflake",
}

with DAG(
    dag_id="eia_ELT_dbt",
    description="Run dbt models via Airflow on Snowflake, then merge into RTO_REGION_HOURLY_STAGING",
    start_date=datetime(2025, 11, 14),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "elt", "snowflake"],
) as dag:

    # quick sanity: print the env vars Airflow is passing to dbt
    show_env = BashOperator(
        task_id="show_env",
        bash_command="env | grep DBT_ || true",
        env=default_env,
    )

    # dbt run / test / snapshot
    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"set -euxo pipefail; "
            f"/home/airflow/.local/bin/dbt run "
            f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        env=default_env,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"set -euxo pipefail; "
            f"/home/airflow/.local/bin/dbt test "
            f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        env=default_env,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            f"set -euxo pipefail; "
            f"/home/airflow/.local/bin/dbt snapshot "
            f"--project-dir {DBT_DIR} --profiles-dir {DBT_DIR}"
        ),
        env=default_env,
    )
def merge_electricity_to_analytics(**_):
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_connection("snowflake_conn")
    sf = hook.get_conn()
    sf.autocommit = False
    cur = sf.cursor()
    try:
        role = (conn.extra_dejson.get("role") or "").upper()
        wh = (conn.extra_dejson.get("warehouse") or "").upper()
        db = (conn.extra_dejson.get("database") or "").upper()
        target_schema = (conn.extra_dejson.get("target_schema") or "ANALYTICS").upper()
        source_schema = "RAW"
        source_table = "RTO_REGION_HOURLY"

        if not db:
            raise ValueError("Snowflake 'database' must be set in the connection extras.")

        if role:
            cur.execute(f'USE ROLE "{role}"')
        if wh:
            cur.execute(f'USE WAREHOUSE "{wh}"')
        cur.execute(f'USE DATABASE "{db}"')

        # Ensure ANALYTICS schema and target table exist
        target_schema = "STAGING"
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{db}"."{target_schema}"')
        cur.execute(f"""
        CREATE TABLE IF NOT EXISTS "{db}"."{target_schema}".RTO_REGION_HOURLY_STAGING(
            REGION STRING,
            DATE TIMESTAMP_NTZ,
            SERIES STRING,
            VALUE FLOAT,
            CONSTRAINT PK_RTO_REGION PRIMARY KEY (REGION, DATE, SERIES)
        );
        """)

        # Merge from RAW 
        cur.execute(f"""
        MERGE INTO "{db}"."{target_schema}".RTO_REGION_HOURLY_STAGING AS tgt
        USING "{db}"."{source_schema}"."{source_table}" AS src
        ON  tgt.REGION = src.REGION
        AND tgt.DATE   = src.DATE
        AND tgt.SERIES = src.SERIES
        WHEN MATCHED THEN UPDATE SET
            VALUE = src.VALUE
        WHEN NOT MATCHED THEN INSERT (REGION, DATE, SERIES, VALUE)
        VALUES (src.REGION, src.DATE, src.SERIES, src.VALUE);
        """)
        sf.commit()
    except Exception:
        sf.rollback()
        raise
    finally:
        cur.close()
        sf.close()

merge_electricity = PythonOperator(
    task_id="merge_electricity_to_analytics",
    python_callable=merge_electricity_to_analytics,
)

show_env >> dbt_run >> merge_electricity >> dbt_test >> dbt_snapshot

