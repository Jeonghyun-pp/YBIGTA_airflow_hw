# Apache Airflow 과제

**주식 시장 데이터 파이프라인 구축 및 운영 최적화**

---

## 과제 개요

매일 코스피 대표 종목 5개의 주가 데이터를 자동 수집하고, 검증/적재/이상값 분기/알림까지 수행하는 Airflow 파이프라인을 4단계(Phase)에 걸쳐 만듭니다.

**상세 과제 설명서:** `Apache_Airflow_과제.pdf`

---

## 빠른 시작

### Docker 환경 (권장)

```bash
docker compose up -d
# http://localhost:5050 (admin / admin)
```

### Windows 로컬 (Docker 없이)

```bash
pip install "apache-airflow==2.10.0" yfinance pandas flask
python e2e_test.py          # 자동 채점
python web_dashboard.py     # 웹 대시보드
```

---

## 수강생이 해야 할 것

`solution/dags/` 안의 **4개 스켈레톤 파일**에서 `# TODO` 부분을 채워 넣으세요.

### Phase 1 — `stock_pipeline_v1.py`

| 할 일 | 힌트 |
|---|---|
| 필요한 모듈 import | `os`, `yfinance`, `pandas`, `datetime`, `airflow.DAG`, `PythonOperator` |
| `default_args` 정의 | `retries=2`, `retry_delay=timedelta(minutes=3)` |
| DAG 정의 | `dag_id="stock_pipeline_v1"`, `schedule="0 17 * * 1-5"`, `catchup=True` |
| `extract_prices` 함수 작성 | `context["ds"]`로 날짜 받기, `yf.download()`, CSV 저장 |
| `validate_file` 함수 작성 | CSV 존재 확인 + 행 수 == 5 검증 |
| `log_summary` 함수 작성 | 날짜, 종목 수, 행 수 출력 |
| PythonOperator 3개 생성 | `task_id` 지정 |
| Task 의존성 연결 | `t1 >> t2 >> t3` |

### Phase 2 — `stock_pipeline_v2.py`

| 할 일 | 힌트 |
|---|---|
| `@dag`, `@task` 데코레이터로 전환 | `from airflow.decorators import dag, task` |
| `extract_prices` → CSV 경로를 `return` | XCom으로 자동 전달 |
| `FileSensor` 생성 | `mode="reschedule"`, `soft_fail=True` |
| `validate_data` → 검증 dict `return` | `row_count`, `anomaly_count`, `null_count` |
| `load_to_db` → SQLite 적재 | `INSERT OR REPLACE` (멱등성!) |
| `report_summary` 작성 | quality dict 기반 출력 |
| TaskFlow 의존성 연결 | 함수 호출 + `>>` 연산자 |

### Phase 3 — `stock_pipeline_v3.py`

| 할 일 | 힌트 |
|---|---|
| `fetch_one_ticker` 작성 | `@task(pool="yfinance_pool")`, 단일 종목 수집 |
| `aggregate` 작성 | 결과 취합, CSV 저장, `anomaly_rate` 계산 |
| `decide_path` 작성 | `@task.branch`, 4가지 분기 반환 |
| `clean_load` 작성 | 그대로 DB 적재 |
| `filtered_load` 작성 | `change_pct` 이상값 행 제거 후 DB 적재 |
| `quarantine` 작성 | `shutil.copy2`로 격리 복사 |
| `skip_day` 작성 | 로그 출력 |
| `notify_result` 작성 | `trigger_rule=NONE_FAILED_MIN_ONE_SUCCESS` |
| Dynamic Task Mapping | `fetch_one_ticker.expand(ticker=TICKERS)` |
| 분기 의존성 연결 | `branch >> [cl, fl, qt, sd]` → `nr` |

> `_ensure_table()`, `_upsert_rows()` 헬퍼 함수는 제공됩니다.

### Phase 4 — `stock_pipeline_v4.py`

| 할 일 | 힌트 |
|---|---|
| `on_load_failure` 콜백 작성 | `data/alerts/{ds}_failure.txt` 장애 리포트 |
| `on_pipeline_success` 콜백 작성 | `[SUCCESS]` 로그 출력 |
| DAG에 `on_success_callback` 추가 | DAG 레벨 콜백 |
| `clean_load`, `filtered_load`에 `on_failure_callback` 추가 | Task 레벨 콜백 |
| Phase 3의 나머지 코드 그대로 가져오기 | `dag_id`만 v4로 변경 |

---

## 디렉터리 구조

```
airflow-hw/
├── docker-compose.yaml          # Docker Compose (PostgreSQL + Airflow)
├── e2e_test.py                  # 자동 채점 스크립트
├── web_dashboard.py             # 웹 대시보드 (로컬 테스트용)
├── Apache_Airflow_과제.pdf      # 과제 설명서
├── solution/                    # ★ 이 안에서만 작업 ★
│   └── dags/
│       ├── stock_pipeline_v1.py # Phase 1 (TODO 채우기)
│       ├── stock_pipeline_v2.py # Phase 2 (TODO 채우기)
│       ├── stock_pipeline_v3.py # Phase 3 (TODO 채우기)
│       └── stock_pipeline_v4.py # Phase 4 (TODO 채우기)
├── airflow_home/                # 로컬 Airflow 환경용
│   └── dags/                    # 로컬용 스켈레톤
├── data/                        # 실행 시 자동 생성
│   ├── prices/                  # CSV 저장 위치
│   ├── quarantine/              # 이상 데이터 격리 (Phase 3)
│   ├── alerts/                  # 장애 알림 (Phase 4)
│   └── stock.db                 # SQLite DB
├── baseline/                    # (플레이스홀더)
└── scripts/                     # (플레이스홀더)
```

---

## 채점 기준

```bash
python e2e_test.py
```

| Phase | 검증 항목 |
|---|---|
| 1 | yfinance 수신, 5종목 CSV 생성, 행 수/컬럼 검증 |
| 2 | SQLite 적재, 멱등성 (재실행 후 5행 유지) |
| 3 | 종목별 fetch, 분기 결정, 경로별 처리 |
| 4 | 장애 리포트 생성, 5영업일 Backfill, 멱등성 최종 확인 |

---

## 제출 방법

`solution/dags/` 안의 4개 파일을 제출하세요.
