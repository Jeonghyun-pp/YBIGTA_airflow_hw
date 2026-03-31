# solution/dags/stock_pipeline_v1.py
"""
Phase 1: 첫 번째 DAG -- 뼈대 만들기
배우는 것: DAG 구조, 스케줄링, Task 의존성, Catchup
"""

# ── TODO: 필요한 모듈을 import 하세요 ──────────────────────────────
# 힌트: os, yfinance(yf), pandas(pd), datetime, timedelta
#       airflow의 DAG, PythonOperator
import os

TICKERS = ["005930.KS", "000660.KS", "035420.KS", "005380.KS", "051910.KS"]
PROJECT_DIR = "/opt/airflow"


# ── TODO: default_args를 정의하세요 ────────────────────────────────
# 힌트: owner, retries=2, retry_delay=timedelta(minutes=3)
default_args = {
    # ...
}


# ── TODO: DAG를 정의하세요 ─────────────────────────────────────────
# 힌트:
#   dag_id = "stock_pipeline_v1"
#   schedule = "0 17 * * 1-5"  (평일 오후 5시)
#   start_date = datetime.now() - timedelta(days=10)
#   catchup = True
#
# with DAG(...) as dag:


    # ── TODO: extract_prices 함수를 작성하세요 ─────────────────────
    # 1. context["ds"]로 날짜(str)를 받음
    # 2. yf.download()로 TICKERS 5종목 데이터 다운로드
    #    - start=ds, end=ds+1일, group_by="ticker"
    # 3. df.empty이면 "[SKIP] 휴장일" 출력 후 return
    # 4. 종목별로 OHLCV 추출하여 rows 리스트에 추가
    # 5. change_pct 계산: 7일 이내 이전 종가를 yf.Ticker(ticker).history()로 가져옴
    # 6. data/prices/{ds}.csv로 저장
    def extract_prices(**context):
        pass


    # ── TODO: validate_file 함수를 작성하세요 ──────────────────────
    # 1. data/prices/{ds}.csv 파일이 존재하는지 확인
    #    - 없으면 FileNotFoundError 발생
    # 2. CSV 행 수가 5인지 확인
    #    - 아니면 ValueError 발생
    def validate_file(**context):
        pass


    # ── TODO: log_summary 함수를 작성하세요 ────────────────────────
    # 1. data/prices/{ds}.csv를 읽어서
    # 2. 처리 날짜, 종목 수(nunique), 총 행 수를 출력
    def log_summary(**context):
        pass


    # ── TODO: PythonOperator로 Task 3개를 생성하세요 ───────────────
    # t1 = PythonOperator(task_id="extract_prices", ...)
    # t2 = PythonOperator(task_id="validate_file", ...)
    # t3 = PythonOperator(task_id="log_summary", ...)


    # ── TODO: Task 의존성을 연결하세요 ─────────────────────────────
    # t1 >> t2 >> t3
    pass
