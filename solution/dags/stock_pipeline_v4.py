# solution/dags/stock_pipeline_v4.py
"""
Phase 4: 프로덕션 수준 완성 -- 알림과 소급 처리
배우는 것: on_failure_callback, on_success_callback, Backfill

Phase 3의 전체 코드를 가져온 뒤, 콜백 함수만 추가합니다.
"""

# ── TODO: 필요한 모듈을 import 하세요 ──────────────────────────────
# Phase 3과 동일
import os

TICKERS = ["005930.KS", "000660.KS", "035420.KS", "005380.KS", "051910.KS"]
PROJECT_DIR = "/opt/airflow"
DB_PATH = "/opt/airflow/data/stock.db"


# ── 아래 두 헬퍼 함수는 제공됩니다 (Phase 3과 동일, 수정 불필요) ──
def _ensure_table(db_path):
    import sqlite3
    conn = sqlite3.connect(db_path)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            date        TEXT NOT NULL,
            ticker      TEXT NOT NULL,
            open        REAL,
            high        REAL,
            low         REAL,
            close       REAL,
            volume      INTEGER,
            change_pct  REAL,
            loaded_at   TEXT,
            PRIMARY KEY (date, ticker)
        )
    """)
    conn.commit()
    return conn


def _upsert_rows(conn, df):
    from datetime import datetime
    df["loaded_at"] = datetime.now().isoformat()
    for _, row in df.iterrows():
        conn.execute("""
            INSERT OR REPLACE INTO daily_prices
            (date, ticker, open, high, low, close, volume, change_pct, loaded_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row["date"], row["ticker"], row["open"], row["high"],
            row["low"], row["close"], row["volume"], row["change_pct"],
            row["loaded_at"],
        ))
    conn.commit()
    conn.close()


# ── TODO: on_load_failure 콜백 함수를 작성하세요 ──────────────────
# Task 실패 시 호출되는 함수입니다.
# 1. context에서 ds, task_id, dag_id, exception 추출
# 2. data/alerts/{ds}_failure.txt 파일에 장애 리포트 작성
#    - [FAILURE] DAG, Task, Date, Error 정보 기록
#
# def on_load_failure(context: dict):


# ── TODO: on_pipeline_success 콜백 함수를 작성하세요 ──────────────
# DAG 전체 성공 시 호출되는 함수입니다.
# 1. "[SUCCESS] stock_pipeline_v4 완료 — {ds}" 출력
#
# def on_pipeline_success(context: dict):


# ── TODO: @dag 데코레이터로 DAG를 정의하세요 ───────────────────────
# Phase 3과 거의 동일하지만:
#   dag_id = "stock_pipeline_v4"
#   on_success_callback = on_pipeline_success  <-- 추가!
#
# @dag(...)
# def stock_pipeline_v4():


    # ── TODO: Phase 3의 모든 Task를 그대로 가져오세요 ──────────────
    # fetch_one_ticker, aggregate, decide_path,
    # clean_load, filtered_load, quarantine, skip_day, notify_result
    #
    # 단, 다음 2개 Task에 콜백을 추가합니다:
    #   @task(on_failure_callback=on_load_failure)   <-- 추가!
    #   def clean_load(quality, **context):
    #
    #   @task(on_failure_callback=on_load_failure)   <-- 추가!
    #   def filtered_load(quality, **context):
    #
    # 나머지 Task와 의존성 연결은 Phase 3과 동일합니다.
    pass


# ── TODO: DAG 함수를 호출하세요 ────────────────────────────────────
# stock_pipeline_v4()
