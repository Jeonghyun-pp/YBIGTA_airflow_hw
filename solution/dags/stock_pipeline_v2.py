# solution/dags/stock_pipeline_v2.py
"""
Phase 2: ETL 파이프라인 -- 데이터 저장과 검증
배우는 것: TaskFlow API (@task), XCom, FileSensor, SQLite 적재, 멱등성
"""

# ── TODO: 필요한 모듈을 import 하세요 ──────────────────────────────
# 힌트: os, sqlite3, yfinance(yf), pandas(pd), datetime, timedelta
#       airflow.decorators의 dag, task
#       airflow.sensors.filesystem의 FileSensor
import os

TICKERS = ["005930.KS", "000660.KS", "035420.KS", "005380.KS", "051910.KS"]
PROJECT_DIR = "/opt/airflow"
DB_PATH = "/opt/airflow/data/stock.db"


# ── TODO: @dag 데코레이터로 DAG를 정의하세요 ───────────────────────
# 힌트:
#   dag_id = "stock_pipeline_v2"
#   schedule = "0 17 * * 1-5"
#   start_date = datetime(2025, 1, 6)
#   catchup = True
#   default_args = {"retries": 2, "retry_delay": timedelta(minutes=3)}
#
# @dag(...)
# def stock_pipeline_v2():


    # ── TODO: extract_prices Task (@task) ──────────────────────────
    # Phase 1과 동일한 로직이지만:
    #   - @task 데코레이터 사용
    #   - CSV 파일 경로(str)를 return (XCom으로 자동 전달)
    #   - 휴장일이면 빈 문자열("") return
    # @task
    # def extract_prices(**context) -> str:


    # ── TODO: FileSensor를 생성하세요 ──────────────────────────────
    # 힌트:
    #   task_id = "wait_for_csv"
    #   filepath = data/prices/ 디렉토리 경로
    #   mode = "reschedule"  (슬롯 반납)
    #   poke_interval = 10
    #   timeout = 120
    #   soft_fail = True  (timeout 시 skip)
    #
    # wait_for_csv = FileSensor(...)


    # ── TODO: validate_data Task (@task) ───────────────────────────
    # 1. csv_path가 빈 문자열이면 {"skipped": True} return
    # 2. CSV를 읽어서 검증 결과 dict return:
    #    - csv_path, row_count, anomaly_count(change_pct 절대값>30), null_count
    # @task
    # def validate_data(csv_path: str) -> dict:


    # ── TODO: load_to_db Task (@task) ──────────────────────────────
    # 1. quality.get("skipped")이면 return
    # 2. CSV를 읽어서 loaded_at 컬럼 추가
    # 3. SQLite daily_prices 테이블에 INSERT OR REPLACE (멱등성!)
    # 힌트: CREATE TABLE IF NOT EXISTS로 테이블 생성 후 INSERT OR REPLACE
    # @task
    # def load_to_db(csv_path: str, quality: dict):


    # ── TODO: report_summary Task (@task) ──────────────────────────
    # 1. quality.get("skipped")이면 "[SKIP]" 출력
    # 2. 아니면 날짜, 행 수, 이상값 수, 결측 수 출력
    # @task
    # def report_summary(quality: dict, **context):


    # ── TODO: Task 의존성을 연결하세요 ─────────────────────────────
    # 1. TaskFlow 함수 호출로 XCom 의존성 생성:
    #    csv_path = extract_prices()
    #    quality = validate_data(csv_path)
    #    load_to_db(csv_path, quality)
    #    report_summary(quality)
    # 2. 명시적 의존성 연결:
    #    csv_path >> wait_for_csv >> quality
    pass


# ── TODO: DAG 함수를 호출하세요 ────────────────────────────────────
# stock_pipeline_v2()
