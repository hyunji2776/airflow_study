# aws 테이블 확인
# qna 게시판에 있는 글을 가져오기
from datetime import datetime, timedelta
from email.policy import default
from textwrap import dedent
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.exceptions import AirflowException
from airflow.providers.amazon.aws.transfers.sql_to_s3 import SqlToS3Operator

# Define the DAG
dag = DAG(
    'mysql_to_s3_v2',
    default_args={
        'owner': 'woori-fisa',
        'start_date': datetime(2023, 8, 24),
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
    },
    schedule_interval=timedelta(minutes=5),  # Run every 5 minutes
    catchup=False,  # Do not backfill if the DAG is paused and resumed
    tags=['MySQLtoS3_v2'],
)

# 멱등성을 고려한 쿼리 작성
# Define the task
mysql_to_s3_task = SqlToS3Operator(
    task_id='mysql_to_s3_qna_task',
    query="""SELECT
                        q.id AS question_id,
                        q.subject AS question_subject,
                        q.content AS question_content,
                        q.create_date AS question_create_date,
                        u.id AS user_id,
                        u.username AS user_username
                    FROM
                        question q
                    JOIN
                        user u ON q.user_id = u.id ORDER BY q.create_date ASC;
            """, 
    sql_conn_id='AWS_RDB2',  # Replace with your MySQL connection ID
    aws_conn_id='AWS_S3',        # Replace with your AWS connection ID
    s3_bucket='woori-fisa',           # Replace with your S3 bucket name
    s3_key='yeonji/qna_test.csv',
    
    dag=dag,
)

# s3_bucket: 데이터가 저장될 장소
# s3_key: 이름. 스키마의 table 명과 비슷한 개념
# sql_conn_id, aws_conn_id: sql, aws(s3) connection. Connection 파트 참조.
# verify: S3에 대한 SSL 증명 확인
# replace: S3 내부에 파일이 있다면 대체할 지에 대한 설정. sql의 if exist와 비슷한 맥락
# pd_kwargs: dataframe에 대한 설정값

mysql_to_s3_task