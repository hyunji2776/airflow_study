# AKIA4J3RLQZ2W7TICRWW
# /at7AARjoprksHVPtowh6jnJmwm/MWmYSEoZ+9ob

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook



def upload_to_s3(filename: str, key: str, bucket_name: str) -> None:
    hook = S3Hook('AWS_S3')
    hook.load_file(filename=filename, key=key, bucket_name=bucket_name)


with DAG(
    'upload_to_s3',
    schedule_interval = timedelta(minutes=5),
    start_date = datetime(2023, 1, 1),
    catchup = False
) as dag:

    upload = PythonOperator(
        task_id = 'upload',
        python_callable = upload_to_s3,
        op_kwargs = {
            'filename' : '/opt/airflow/dags/0_HelloWorld.py', # 현재 내 Airflow는 Docker 위에서 동작하므로 파일은 Airflow가 동작하는 Container의 파일시스템에 기준으로 작성합니다.
            'key' : 'yeonji/from_airflow.py',
            'bucket_name' : 'woori-fisa'
        }
    )