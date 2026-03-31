# solution/dags/stock_pipeline_v3.py
"""
Phase 3: 병렬 처리와 데이터 품질 분기
배우는 것: Dynamic Task Mapping, Pool, Branching, Trigger Rules
"""

# ── TODO: 필요한 모듈을 import 하세요 ──────────────────────────────
# 힌트: os, sqlite3, shutil, yfinance(yf), pandas(pd), datetime, timedelta
#       airflow.decorators의 dag, task
#       airflow.utils.trigger_rule의 TriggerRule
import os

TICKERS = ["005930.KS", "000660.KS", "035420.KS", "005380.KS", "051910.KS"]
PROJECT_DIR = "/opt/airflow"
DB_PATH = "/opt/airflow/data/stock.db"


# ── 아래 두 헬퍼 함수는 제공됩니다 (수정 불필요) ──────────────────
def _ensure_table(db_path):
    """SQLite daily_prices 테이블이 없으면 생성하고 connection을 반환합니다."""
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
    """DataFrame의 각 행을 daily_prices 테이블에 INSERT OR REPLACE합니다."""
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


# ── TODO: @dag 데코레이터로 DAG를 정의하세요 ───────────────────────
# 힌트:
#   dag_id = "stock_pipeline_v3"
#   schedule = "0 17 * * 1-5"
#   start_date = datetime(2025, 1, 6)
#   catchup = True
#
# @dag(...)
# def stock_pipeline_v3():


    # ── TODO: fetch_one_ticker Task ────────────────────────────────
    # 핵심: @task(pool="yfinance_pool")로 API 호출 동시성 제한
    #
    # 1. context["ds"]로 날짜를 받음
    # 2. yf.Ticker(ticker).history()로 단일 종목 데이터 수집
    # 3. df.empty이면 {"ticker": ticker, "skipped": True, "anomaly": False} return
    # 4. OHLCV 추출 + 7일 이내 이전 종가로 change_pct 계산
    # 5. anomaly = abs(change_pct) > 30
    # 6. dict return (ticker, date, open, high, low, close, volume, change_pct, anomaly, skipped)
    #
    # @task(pool="yfinance_pool")
    # def fetch_one_ticker(ticker: str, **context) -> dict:


    # ── TODO: aggregate Task ───────────────────────────────────────
    # 1. results 리스트에서 skipped=False인 것만 필터링 (valid)
    # 2. valid가 없으면 {"skipped": True} return
    # 3. anomaly_count, anomaly_rate 계산
    # 4. valid를 DataFrame으로 만들어 data/prices/{ds}.csv 저장
    # 5. dict return (csv_path, ds, total, anomaly_count, anomaly_rate)
    #
    # @task
    # def aggregate(results: list) -> dict:


    # ── TODO: decide_path Task (Branching) ─────────────────────────
    # @task.branch 데코레이터 사용!
    # quality["skipped"]이면 -> "skip_day"
    # anomaly_rate == 0     -> "clean_load"
    # anomaly_rate <= 0.20  -> "filtered_load"
    # anomaly_rate > 0.20   -> "quarantine"
    #
    # @task.branch
    # def decide_path(quality: dict) -> str:


    # ── TODO: 4개 분기 Task를 작성하세요 ───────────────────────────
    #
    # clean_load(quality):
    #   - CSV 읽어서 그대로 DB 적재 (_ensure_table, _upsert_rows 사용)
    #
    # filtered_load(quality):
    #   - CSV 읽은 뒤 change_pct 절대값 > 30인 행 제거 후 DB 적재
    #
    # quarantine(quality):
    #   - shutil.copy2로 CSV를 data/quarantine/{ds}.csv에 복사 (원본 보존)
    #   - DB 적재 안 함
    #
    # skip_day():
    #   - "[SKIP] 휴장일" 출력


    # ── TODO: notify_result Task ───────────────────────────────────
    # 핵심: trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    # 이 설정 없으면 skip된 upstream 때문에 이 Task도 실행되지 않음!
    #
    # @task(trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS)
    # def notify_result(**context):


    # ── TODO: Task 의존성을 연결하세요 ─────────────────────────────
    # 1. Dynamic Task Mapping:
    #    results = fetch_one_ticker.expand(ticker=TICKERS)
    # 2. 집계:
    #    quality = aggregate(results)
    # 3. 분기:
    #    branch = decide_path(quality)
    # 4. 각 분기 Task 생성:
    #    cl = clean_load(quality)
    #    fl = filtered_load(quality)
    #    qt = quarantine(quality)
    #    sd = skip_day()
    #    nr = notify_result()
    # 5. 의존성:
    #    branch >> [cl, fl, qt, sd]
    #    [cl, fl, qt, sd] >> nr
    pass


# ── TODO: DAG 함수를 호출하세요 ────────────────────────────────────
# stock_pipeline_v3()
