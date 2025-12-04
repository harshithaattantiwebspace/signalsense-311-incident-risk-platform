from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

PROJECT_ID = "signalsense-project"
DATASET = "signalsense"
DBT_PROJECT_DIR = "/opt/airflow/dags/signalsense_dbt"  # will mount this

with DAG(
    dag_id="signalsense_daily_pipeline",
    default_args=default_args,
    description="Daily pipeline: score new streamed issues + refresh marts",
    schedule_interval="@daily",   # change to None if you prefer manual only
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["signalsense"],
) as dag:

    bq_new_stream_issues = BigQueryInsertJobOperator(
    task_id="bq_new_stream_issues",
    gcp_conn_id="google_cloud_default",
    configuration={
        "query": {
            "query": """
                CREATE OR REPLACE TABLE `signalsense-project.signalsense.new_stream_issues` AS
                SELECT
                  s.*
                FROM `signalsense-project.signalsense.issues_enriched` AS s
                LEFT JOIN `signalsense-project.signalsense.issues_scored` AS sc
                  USING (issue_id)
                WHERE sc.issue_id IS NULL;
            """,
            "useLegacySql": False,
        }
    },
)


    bq_score_new_stream = BigQueryInsertJobOperator(
        task_id="bq_score_new_stream",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                CREATE OR REPLACE TABLE `{PROJECT_ID}.{DATASET}.new_stream_scored` AS
                SELECT
                  n.*,
                  p.predicted_sla_breached AS predicted_label,
                  p.predicted_sla_breached_probs[OFFSET(1)] AS risk_score
                FROM ML.PREDICT(
                  MODEL `{PROJECT_ID}.{DATASET}.sla_dnn`,
                  (
                    SELECT
                      issue_id,
                      issue_text,
                      region,
                      channel,
                      raw_category,
                      topic_id,
                      EXTRACT(HOUR      FROM created_at) AS hour,
                      EXTRACT(DAYOFWEEK FROM created_at) AS dow
                    FROM `{PROJECT_ID}.{DATASET}.new_stream_issues`
                  )
                ) AS p
                JOIN `{PROJECT_ID}.{DATASET}.new_stream_issues` AS n
                USING (issue_id);
                """,
                "useLegacySql": False,
            }
        },
    )

    bq_append_to_issues_scored = BigQueryInsertJobOperator(
        task_id="bq_append_to_issues_scored",
        gcp_conn_id="google_cloud_default",
        configuration={
            "query": {
                "query": f"""
                INSERT INTO `{PROJECT_ID}.{DATASET}.issues_scored`
                SELECT *
                FROM `{PROJECT_ID}.{DATASET}.new_stream_scored`;
                """,
                "useLegacySql": False,
            }
        },
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"""
        cd {DBT_PROJECT_DIR} && dbt run
        """,
    )

    bq_new_stream_issues >> bq_score_new_stream >> bq_append_to_issues_scored >> dbt_run
