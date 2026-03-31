# airflow_home/dags/stock_pipeline_v4.py  (로컬 환경용)
"""
Phase 4: 프로덕션 수준 완성 -- 알림과 소급 처리
배우는 것: on_failure_callback, on_success_callback, Backfill
"""

# ── TODO: 필요한 모듈을 import 하세요 ──────────────────────────────
import os

TICKERS = ["005930.KS", "000660.KS", "035420.KS", "005380.KS", "051910.KS"]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", ".."))
DB_PATH = os.path.join(PROJECT_DIR, "data", "stock.db")


# ── 헬퍼 함수는 solution/dags/stock_pipeline_v4.py에서 제공됩니다 ──
# _ensure_table(db_path), _upsert_rows(conn, df) 그대로 복사해서 사용하세요.

# ── TODO: solution/dags/stock_pipeline_v4.py 스켈레톤을 참고하여 작성하세요 ──
# 차이점: PROJECT_DIR, DB_PATH가 로컬 경로 기반 (위에서 이미 정의됨)
# 나머지 로직은 동일합니다.
pass

# stock_pipeline_v4()
