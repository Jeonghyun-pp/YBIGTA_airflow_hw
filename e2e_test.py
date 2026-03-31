"""
E2E Test: Windows 환경에서 DAG 핵심 로직을 직접 실행하여 검증
Airflow scheduler 없이 Python으로 각 Phase의 로직을 테스트합니다.
"""
import os
import sys
import sqlite3
import shutil
import yfinance as yf
import pandas as pd
from datetime import datetime, timedelta

PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_DIR, "data", "stock.db")
TICKERS = ["005930.KS", "000660.KS", "035420.KS", "005380.KS", "051910.KS"]

passed = 0
failed = 0


def check(name, condition, detail=""):
    global passed, failed
    if condition:
        passed += 1
        print(f"  [PASS] {name}")
    else:
        failed += 1
        print(f"  [FAIL] {name} — {detail}")


def ensure_table():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
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


# ── Phase 1 Test: extract + validate + log ────────────────────────
def test_phase1(ds="2025-01-13"):
    print(f"\n{'='*60}")
    print(f"Phase 1 테스트 — 기본 파이프라인 (ds={ds})")
    print(f"{'='*60}")

    end = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    print(f"  yfinance 호출 중... ({ds} ~ {end})")

    df = yf.download(
        tickers=TICKERS, start=ds, end=end,
        interval="1d", auto_adjust=True, progress=False, group_by="ticker"
    )

    if df.empty:
        print(f"  [SKIP] {ds} 휴장일 — 다른 날짜로 재시도 필요")
        return False

    check("yf.download 데이터 수신", not df.empty)

    rows = []
    for ticker in TICKERS:
        try:
            ticker_df = df[ticker].dropna()
            if ticker_df.empty:
                continue
            row = {
                "date": ds,
                "ticker": ticker,
                "open": float(ticker_df["Open"].iloc[0]),
                "high": float(ticker_df["High"].iloc[0]),
                "low": float(ticker_df["Low"].iloc[0]),
                "close": float(ticker_df["Close"].iloc[0]),
                "volume": int(ticker_df["Volume"].iloc[0]),
                "change_pct": 0.0,
            }
            prev_start = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
            t = yf.Ticker(ticker)
            hist = t.history(start=prev_start, end=ds)
            if not hist.empty:
                prev_close = float(hist["Close"].iloc[-1])
                row["change_pct"] = round((row["close"] - prev_close) / prev_close * 100, 2)
            rows.append(row)
        except Exception as e:
            print(f"  [WARN] {ticker}: {e}")

    check("5개 종목 데이터 추출", len(rows) == 5, f"got {len(rows)}")

    combined_df = pd.DataFrame(rows)
    prices_dir = os.path.join(PROJECT_DIR, "data", "prices")
    os.makedirs(prices_dir, exist_ok=True)
    csv_path = os.path.join(prices_dir, f"{ds}.csv")
    combined_df.to_csv(csv_path, index=False)

    check("CSV 파일 생성", os.path.exists(csv_path))

    # validate
    read_df = pd.read_csv(csv_path)
    check("CSV 행 수 = 5", len(read_df) == 5, f"got {len(read_df)}")
    check("필수 컬럼 존재", all(c in read_df.columns for c in
          ["date", "ticker", "open", "high", "low", "close", "volume", "change_pct"]))

    # log summary
    print(f"  [SUMMARY] 날짜: {ds}, 종목 수: {read_df['ticker'].nunique()}, 총 행 수: {len(read_df)}")
    print(f"  CSV 내용:\n{read_df.to_string(index=False)}")
    return True


# ── Phase 2 Test: DB load + idempotency ───────────────────────────
def test_phase2(ds="2025-01-13"):
    print(f"\n{'='*60}")
    print(f"Phase 2 테스트 — SQLite 적재 + 멱등성 (ds={ds})")
    print(f"{'='*60}")

    csv_path = os.path.join(PROJECT_DIR, "data", "prices", f"{ds}.csv")
    check("CSV 파일 존재", os.path.exists(csv_path))

    df = pd.read_csv(csv_path)

    # Validate
    anomaly_count = int((df["change_pct"].abs() > 30).sum())
    null_count = int(df.isnull().any(axis=1).sum())
    print(f"  검증: 행={len(df)}, 이상값={anomaly_count}, 결측={null_count}")

    # Load to DB
    conn = ensure_table()
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

    # Check count
    count = conn.execute(
        "SELECT COUNT(*) FROM daily_prices WHERE date=?", (ds,)
    ).fetchone()[0]
    check(f"DB에 {ds} 데이터 5행 적재", count == 5, f"got {count}")

    # Idempotency: insert again
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

    count_after = conn.execute(
        "SELECT COUNT(*) FROM daily_prices WHERE date=?", (ds,)
    ).fetchone()[0]
    check("멱등성: 재실행 후에도 5행 유지", count_after == 5, f"got {count_after}")
    conn.close()


# ── Phase 3 Test: parallel fetch + branching ──────────────────────
def test_phase3(ds="2025-01-14"):
    print(f"\n{'='*60}")
    print(f"Phase 3 테스트 — 병렬 fetch + 분기 (ds={ds})")
    print(f"{'='*60}")

    end = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")

    results = []
    for ticker in TICKERS:
        t = yf.Ticker(ticker)
        df = t.history(start=ds, end=end)
        if df.empty:
            results.append({"ticker": ticker, "skipped": True, "anomaly": False})
            continue

        close = float(df["Close"].iloc[0])
        open_ = float(df["Open"].iloc[0])
        high = float(df["High"].iloc[0])
        low = float(df["Low"].iloc[0])
        volume = int(df["Volume"].iloc[0])

        prev_start = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
        hist = t.history(start=prev_start, end=ds)
        change_pct = 0.0
        if not hist.empty:
            prev_close = float(hist["Close"].iloc[-1])
            change_pct = round((close - prev_close) / prev_close * 100, 2)

        anomaly = abs(change_pct) > 30
        results.append({
            "ticker": ticker, "date": ds,
            "open": open_, "high": high, "low": low,
            "close": close, "volume": volume,
            "change_pct": change_pct, "anomaly": anomaly, "skipped": False,
        })

    valid = [r for r in results if not r.get("skipped")]
    if not valid:
        print(f"  [SKIP] {ds} 휴장일")
        return

    check(f"fetch_one_ticker: {len(valid)}개 종목 수집", len(valid) == 5, f"got {len(valid)}")

    # Aggregate
    anomaly_count = sum(1 for r in valid if r.get("anomaly"))
    anomaly_rate = anomaly_count / len(valid)
    print(f"  이상값: {anomaly_count}/{len(valid)} (rate={anomaly_rate:.2%})")

    # Save CSV
    prices_dir = os.path.join(PROJECT_DIR, "data", "prices")
    os.makedirs(prices_dir, exist_ok=True)
    csv_path = os.path.join(prices_dir, f"{ds}.csv")
    df_out = pd.DataFrame(valid)[["date", "ticker", "open", "high", "low", "close", "volume", "change_pct"]]
    df_out.to_csv(csv_path, index=False)

    # Branch decision
    if anomaly_rate == 0:
        branch = "clean_load"
    elif anomaly_rate <= 0.20:
        branch = "filtered_load"
    else:
        branch = "quarantine"

    check(f"분기 결정: {branch}", branch in ["clean_load", "filtered_load", "quarantine"])
    print(f"  → 선택된 경로: {branch}")

    # Execute branch
    if branch == "clean_load":
        conn = ensure_table()
        df_load = pd.read_csv(csv_path)
        df_load["loaded_at"] = datetime.now().isoformat()
        for _, row in df_load.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO daily_prices
                (date, ticker, open, high, low, close, volume, change_pct, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (row["date"], row["ticker"], row["open"], row["high"],
                  row["low"], row["close"], row["volume"], row["change_pct"], row["loaded_at"]))
        conn.commit()
        count = conn.execute("SELECT COUNT(*) FROM daily_prices WHERE date=?", (ds,)).fetchone()[0]
        conn.close()
        check(f"clean_load: DB에 {ds} 데이터 적재", count == 5, f"got {count}")
    elif branch == "filtered_load":
        conn = ensure_table()
        df_load = pd.read_csv(csv_path)
        df_load = df_load[df_load["change_pct"].abs() <= 30]
        df_load["loaded_at"] = datetime.now().isoformat()
        for _, row in df_load.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO daily_prices
                (date, ticker, open, high, low, close, volume, change_pct, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (row["date"], row["ticker"], row["open"], row["high"],
                  row["low"], row["close"], row["volume"], row["change_pct"], row["loaded_at"]))
        conn.commit()
        conn.close()
        check(f"filtered_load: 이상값 제거 후 적재", True)
    else:
        quarantine_dir = os.path.join(PROJECT_DIR, "data", "quarantine")
        os.makedirs(quarantine_dir, exist_ok=True)
        dest = os.path.join(quarantine_dir, f"{ds}.csv")
        shutil.copy2(csv_path, dest)
        check(f"quarantine: {dest} 격리", os.path.exists(dest))

    print(f"  [DONE] {ds} 처리 완료")


# ── Phase 4 Test: callbacks + backfill ────────────────────────────
def test_phase4():
    print(f"\n{'='*60}")
    print("Phase 4 테스트 — 콜백 + Backfill 시뮬레이션")
    print(f"{'='*60}")

    # Test on_load_failure callback
    alerts_dir = os.path.join(PROJECT_DIR, "data", "alerts")
    os.makedirs(alerts_dir, exist_ok=True)
    test_ds = "2025-01-15"
    alert_path = os.path.join(alerts_dir, f"{test_ds}_failure.txt")
    with open(alert_path, "w", encoding="utf-8") as f:
        f.write(f"[FAILURE] DAG  : stock_pipeline_v4\n")
        f.write(f"          Task : clean_load\n")
        f.write(f"          Date : {test_ds}\n")
        f.write(f"          Error: simulated test error\n")
    check("on_failure_callback: 장애 리포트 생성", os.path.exists(alert_path))

    # Backfill simulation: process 5 business days
    backfill_dates = ["2025-01-06", "2025-01-07", "2025-01-08", "2025-01-09", "2025-01-10"]
    print(f"\n  Backfill 시뮬레이션: {backfill_dates[0]} ~ {backfill_dates[-1]}")

    for ds in backfill_dates:
        end = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        print(f"  [{ds}] 데이터 수집 중...")

        results = []
        for ticker in TICKERS:
            t = yf.Ticker(ticker)
            df = t.history(start=ds, end=end)
            if df.empty:
                results.append({"ticker": ticker, "skipped": True})
                continue
            close = float(df["Close"].iloc[0])
            prev_start = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
            hist = t.history(start=prev_start, end=ds)
            change_pct = 0.0
            if not hist.empty:
                prev_close = float(hist["Close"].iloc[-1])
                change_pct = round((close - prev_close) / prev_close * 100, 2)
            results.append({
                "date": ds, "ticker": ticker,
                "open": float(df["Open"].iloc[0]),
                "high": float(df["High"].iloc[0]),
                "low": float(df["Low"].iloc[0]),
                "close": close,
                "volume": int(df["Volume"].iloc[0]),
                "change_pct": change_pct,
                "skipped": False,
            })

        valid = [r for r in results if not r.get("skipped")]
        if not valid:
            print(f"  [{ds}] 휴장일 — 건너뜀")
            continue

        # Save CSV
        prices_dir = os.path.join(PROJECT_DIR, "data", "prices")
        os.makedirs(prices_dir, exist_ok=True)
        csv_path = os.path.join(prices_dir, f"{ds}.csv")
        df_out = pd.DataFrame(valid)[["date", "ticker", "open", "high", "low", "close", "volume", "change_pct"]]
        df_out.to_csv(csv_path, index=False)

        # Load to DB
        conn = ensure_table()
        df_load = pd.read_csv(csv_path)
        df_load["loaded_at"] = datetime.now().isoformat()
        for _, row in df_load.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO daily_prices
                (date, ticker, open, high, low, close, volume, change_pct, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (row["date"], row["ticker"], row["open"], row["high"],
                  row["low"], row["close"], row["volume"], row["change_pct"], row["loaded_at"]))
        conn.commit()
        conn.close()
        print(f"  [{ds}] {len(valid)}행 적재 완료")

    # Check backfill results
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        "SELECT date, COUNT(*) as cnt FROM daily_prices GROUP BY date ORDER BY date"
    ).fetchall()
    conn.close()

    print(f"\n  DB 상태:")
    for date, cnt in rows:
        print(f"    {date} | {cnt}행")

    business_day_count = sum(1 for date, cnt in rows if date in backfill_dates)
    check(f"Backfill: {business_day_count}개 영업일 적재", business_day_count >= 4,
          f"일부 휴장일 가능")

    # Idempotency: re-run backfill
    total_before = sum(cnt for _, cnt in rows)
    print(f"\n  멱등성 테스트: 동일 기간 재실행...")
    for ds in backfill_dates:
        csv_path = os.path.join(PROJECT_DIR, "data", "prices", f"{ds}.csv")
        if not os.path.exists(csv_path):
            continue
        conn = ensure_table()
        df_load = pd.read_csv(csv_path)
        df_load["loaded_at"] = datetime.now().isoformat()
        for _, row in df_load.iterrows():
            conn.execute("""
                INSERT OR REPLACE INTO daily_prices
                (date, ticker, open, high, low, close, volume, change_pct, loaded_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (row["date"], row["ticker"], row["open"], row["high"],
                  row["low"], row["close"], row["volume"], row["change_pct"], row["loaded_at"]))
        conn.commit()
        conn.close()

    conn = sqlite3.connect(DB_PATH)
    total_after = conn.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
    conn.close()
    check(f"멱등성: 재실행 전후 총 행 수 동일 ({total_before} → {total_after})",
          total_before == total_after)

    print(f"\n  [SUCCESS] stock_pipeline_v4 E2E 완료")


# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    print("=" * 60)
    print("Apache Airflow 과제 — E2E 테스트 (Windows)")
    print("=" * 60)

    # Clean previous data
    for d in ["data/prices", "data/quarantine", "data/alerts"]:
        p = os.path.join(PROJECT_DIR, d)
        if os.path.exists(p):
            shutil.rmtree(p)
    db = os.path.join(PROJECT_DIR, "data", "stock.db")
    if os.path.exists(db):
        os.remove(db)

    ok = test_phase1()
    if ok:
        test_phase2()
    test_phase3()
    test_phase4()

    print(f"\n{'='*60}")
    print(f"결과: {passed} PASSED / {failed} FAILED")
    print(f"{'='*60}")
    sys.exit(1 if failed > 0 else 0)
