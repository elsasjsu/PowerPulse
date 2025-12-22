from pendulum import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook


# Where dbt project is mounted inside the container
DBT_DIR = "/opt/airflow/dbt"

# Where the dbt binary lives inside the container
DBT_BIN = "/home/airflow/.local/bin/dbt"


# Helper to avoid None values when building env
def _s(v):
    return "" if v is None else str(v)


# Build dbt env variables from Snowflake connection
conn = BaseHook.get_connection("snowflake_conn")
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


def merge_electricity_to_analytics():
    """Merge RAW.RTO_REGION_HOURLY into STAGING.RTO_REGION_HOURLY_STAGING in Snowflake."""
    hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
    conn = hook.get_connection("snowflake_conn")
    sf = hook.get_conn()
    sf.autocommit = False
    cur = sf.cursor()

    try:
        role = (conn.extra_dejson.get("role") or "").upper()
        wh = (conn.extra_dejson.get("warehouse") or "").upper()
        db = (conn.extra_dejson.get("database") or "").upper()
        source_schema = "RAW"
        source_table = "RTO_REGION_HOURLY"

        # You want the merged data in STAGING
        target_schema = "STAGING"

        if not db:
            raise ValueError("Snowflake 'database' must be set in the connection extras.")

        if role:
            cur.execute(f'USE ROLE "{role}"')
        if wh:
            cur.execute(f'USE WAREHOUSE "{wh}"')
        cur.execute(f'USE DATABASE "{db}"')

        # Ensure STAGING schema and target table exist
        cur.execute(f'CREATE SCHEMA IF NOT EXISTS "{db}"."{target_schema}"')
        cur.execute(
            f"""
            CREATE TABLE IF NOT EXISTS "{db}"."{target_schema}".RTO_REGION_HOURLY_STAGING(
                REGION STRING,
                DATE TIMESTAMP_NTZ,
                SERIES STRING,
                VALUE FLOAT,
                CONSTRAINT PK_RTO_REGION PRIMARY KEY (REGION, DATE, SERIES)
            );
            """
        )

        # Merge from RAW into STAGING
        cur.execute(
            f"""
            MERGE INTO "{db}"."{target_schema}".RTO_REGION_HOURLY_STAGING AS tgt
            USING "{db}"."{source_schema}"."{source_table}" AS src
            ON  tgt.REGION = src.REGION
            AND tgt.DATE   = src.DATE
            AND tgt.SERIES = src.SERIES
            WHEN MATCHED THEN UPDATE SET
                VALUE = src.VALUE
            WHEN NOT MATCHED THEN INSERT (REGION, DATE, SERIES, VALUE)
            VALUES (src.REGION, src.DATE, src.SERIES, src.VALUE);
            """
        )

        sf.commit()

    except Exception:
        sf.rollback()
        raise
    finally:
        cur.close()
        sf.close()


with DAG(
    dag_id="eia_ELT_dbt",
    description="Run dbt models via Airflow on Snowflake, then merge into RTO_REGION_HOURLY_STAGING",
    start_date=datetime(2025, 11, 14),
    schedule="@daily",
    catchup=False,
    tags=["dbt", "elt", "snowflake"],
) as dag:

    show_env = BashOperator(
        task_id="show_env",
        bash_command=(
            "set -euxo pipefail\n"
            "echo PATH is: $PATH\n"
            "env | grep DBT_ || true\n"
        ),
        env=default_env,
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            "set -euxo pipefail\n"
            'export PATH="$PATH:/home/airflow/.local/bin"\n'
            f"cd {DBT_DIR}\n"
            "ls -la\n"
            f"{DBT_BIN} run --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}\n"
        ),
        env=default_env,
    )

    merge_electricity = PythonOperator(
        task_id="merge_electricity_to_analytics",
        python_callable=merge_electricity_to_analytics,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            "set -euxo pipefail\n"
            'export PATH="$PATH:/home/airflow/.local/bin"\n'
            f"cd {DBT_DIR}\n"
            f"{DBT_BIN} test --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}\n"
        ),
        env=default_env,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=(
            "set -euxo pipefail\n"
            'export PATH="$PATH:/home/airflow/.local/bin"\n'
            f"cd {DBT_DIR}\n"
            f"{DBT_BIN} snapshot --project-dir {DBT_DIR} --profiles-dir {DBT_DIR}\n"
        ),
        env=default_env,
    )

    show_env >> dbt_run >> merge_electricity >> dbt_test >> dbt_snapshot
