# hello_world.py
from datetime import timedelta
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
import pendulum # python의 datetime을 좀더 편하게 사용할 수 있게 돕는 모델
from datetime import datetime

local_tz = pendulum.timezone("Asia/Seoul")

init_args = {
'start_date' : datetime(2022, 8, 22, 2, tzinfo=local_tz)
}

def print_world() -> None:
    print("world")

# with 구문으로 DAG 정의를 시작합니다.
with DAG(
    dag_id="hello_world", # DAG의 식별자용 아이디입니다.
    description="My First DAG", # DAG에 대해 설명합니다.

    start_date=days_ago(2), # DAG 정의 기준 2일 전부터 시작합니다.
	  # start_date=datetime(2023, 8, 7, hour=12, minute=30), # DAG 정의 기준 시간부터 시작합니다
    # start_date=airflow.utils.dates.days_ago(14),  
			# 스케쥴의 간격과 함께 DAG 첫 실행 시작 날짜를 지정해줍니다.
      # 주의: 1월 1일에 DAG를 작성하고 자정마다 실행하도록 schedule_interval을 지정한다면 태스크는 1월 2일 자정부터 수행됩니다	
    end_date=dt.datetime(year=2023, month=8, day=19),

		# schedule_interval="0 6 * * *", # 매일 06:00에 실행합니다.
		schedule_interval="@daily", # DAG 실행 간격 - 매일 자정에 실행
    tags=["my_dags"],
    ) as dag:
# 태스크를 정의합니다.
# bash 커맨드로 echo hello 를 실행합니다.
t1 = BashOperator(
    task_id="print_hello",
    bash_command="echo Hello",
    owner="fisa", # 이 작업의 오너입니다. 보통 작업을 담당하는 사람 이름을 넣습니다.
    retries=3, # 이 태스크가 실패한 경우, 3번 재시도 합니다.
    retry_delay=timedelta(minutes=1), # 재시도하는 시간 간격은 1분입니다.
)

# 태스크를 정의합니다.
# python 함수인 print_world를 실행합니다.
t2 = PythonOperator(
    task_id="print_world",
    python_callable=print_world,
    depends_on_past=True,
    owner="fisa",
    retries=3,
    retry_delay=timedelta(minutes=5),
)

# 태스크 순서를 정합니다.
# t1 실행 후 t2를 실행합니다.
t1 >> t2