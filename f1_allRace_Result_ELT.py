from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import datetime


sql_query = """
    CREATE OR REPLACE TABLE `{{ params.target_dataset }}.allRace_result` AS
    WITH QualifyingResults AS (
        SELECT 
            p.driver_number, 
            p.position, 
            p.session_key, 
            s.session_name, 
            FORMAT_TIMESTAMP('%Y-%m-%d', p.date) AS date
        FROM 
            `{{ params.bigquery_project_dataset }}.position` AS p
        JOIN 
            (
            SELECT session_key, session_name
            FROM `{{ params.bigquery_project_dataset }}.session`
            WHERE session_name = 'Race'
            ORDER BY date_end
            ) AS s
        ON 
            p.session_key = s.session_key
    )
    SELECT 
        qr.driver_number, 
        qr.position, 
        qr.date,
        d.broadcast_name,
        d.team_name
    FROM 
        QualifyingResults AS qr
    LEFT JOIN 
        `{{ params.bigquery_project_dataset }}.driver` AS d
    ON 
        qr.driver_number = d.driver_number AND qr.session_key = d.session_key
    ORDER BY 
        qr.date, qr.position;
"""

# DAG 정의
with DAG(
    dag_id="f1_allRace_Result_ELT",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    schedule=None,
) as dag:
    
    # 변수 가져오기
    target_dataset = Variable.get("target_dataset")
    bigquery_project_dataset = Variable.get("bigquery_project_dataset")

    # BigQuery SQL 실행 태스크
    execute_query_task = BigQueryExecuteQueryOperator(
        task_id='execute_query_task',
        sql=sql_query,
        use_legacy_sql=False,
        params={'target_dataset': target_dataset, 'bigquery_project_dataset': bigquery_project_dataset},
    )

    execute_query_task

