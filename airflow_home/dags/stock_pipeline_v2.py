# airflow_home/dags/stock_pipeline_v2.py  (로컬 환경용)
"""
Phase 2: ETL 파이프라인 -- 데이터 저장과 검증
배우는 것: TaskFlow API (@task), XCom, FileSensor, SQLite 적재, 멱등성
"""

# ── TODO: 필요한 모듈을 import 하세요 ──────────────────────────────
import os

TICKERS = ["005930.KS", "000660.KS", "035420.KS", "005380.KS", "051910.KS"]
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_DIR = os.path.normpath(os.path.join(BASE_DIR, "..", ".."))
DB_PATH = os.path.join(PROJECT_DIR, "data", "stock.db")


# ── TODO: solution/dags/stock_pipeline_v2.py 스켈레톤을 참고하여 작성하세요 ──
# 차이점: PROJECT_DIR, DB_PATH가 로컬 경로 기반 (위에서 이미 정의됨)
# 나머지 로직은 동일합니다.
pass

# stock_pipeline_v2()
