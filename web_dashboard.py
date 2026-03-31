"""
Airflow 과제 — Windows용 웹 대시보드
http://localhost:5000 에서 DAG 파이프라인을 직접 실행하고 결과를 확인할 수 있습니다.
"""
import os
import sys
import sqlite3
import shutil
import json
import threading
import traceback
from datetime import datetime, timedelta

import yfinance as yf
import pandas as pd
from flask import Flask, render_template_string, request, jsonify, redirect, url_for

# ── Config ────────────────────────────────────────────────────────
PROJECT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(PROJECT_DIR, "data", "stock.db")
TICKERS = ["005930.KS", "000660.KS", "035420.KS", "005380.KS", "051910.KS"]
TICKER_NAMES = {
    "005930.KS": "Samsung",
    "000660.KS": "SK Hynix",
    "035420.KS": "NAVER",
    "005380.KS": "Hyundai",
    "051910.KS": "LG Chem",
}

app = Flask(__name__)

# ── Run history (in-memory) ───────────────────────────────────────
run_history = []  # list of dicts

# ── DB helpers ────────────────────────────────────────────────────
def ensure_table():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_prices (
            date TEXT NOT NULL, ticker TEXT NOT NULL,
            open REAL, high REAL, low REAL, close REAL,
            volume INTEGER, change_pct REAL, loaded_at TEXT,
            PRIMARY KEY (date, ticker)
        )
    """)
    conn.commit()
    return conn


def get_db_stats():
    if not os.path.exists(DB_PATH):
        return {"total_rows": 0, "dates": 0, "tickers": 0, "rows": []}
    conn = sqlite3.connect(DB_PATH)
    try:
        total = conn.execute("SELECT COUNT(*) FROM daily_prices").fetchone()[0]
        dates = conn.execute("SELECT COUNT(DISTINCT date) FROM daily_prices").fetchone()[0]
        tickers = conn.execute("SELECT COUNT(DISTINCT ticker) FROM daily_prices").fetchone()[0]
        rows = conn.execute(
            "SELECT date, ticker, close, change_pct, loaded_at FROM daily_prices ORDER BY date DESC, ticker LIMIT 100"
        ).fetchall()
        date_summary = conn.execute(
            "SELECT date, COUNT(*) as cnt FROM daily_prices GROUP BY date ORDER BY date"
        ).fetchall()
        return {"total_rows": total, "dates": dates, "tickers": tickers,
                "rows": rows, "date_summary": date_summary}
    finally:
        conn.close()


# ── Pipeline logic (reused from DAGs) ─────────────────────────────
def run_pipeline(ds, phase="v4"):
    """Execute the pipeline logic for a given date. Returns result dict."""
    result = {"ds": ds, "phase": phase, "status": "running", "log": [], "started": datetime.now().isoformat()}

    try:
        end = (datetime.strptime(ds, "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
        result["log"].append(f"[{phase}] Fetching data for {ds}...")

        if phase in ("v1", "v2"):
            # Bulk download
            df = yf.download(
                tickers=TICKERS, start=ds, end=end,
                interval="1d", auto_adjust=True, progress=False, group_by="ticker"
            )
            if df.empty:
                result["log"].append(f"[SKIP] {ds} market closed")
                result["status"] = "skipped"
                return result

            rows = []
            for ticker in TICKERS:
                try:
                    ticker_df = df[ticker].dropna()
                    if ticker_df.empty:
                        continue
                    row = {
                        "date": ds, "ticker": ticker,
                        "open": float(ticker_df["Open"].iloc[0]),
                        "high": float(ticker_df["High"].iloc[0]),
                        "low": float(ticker_df["Low"].iloc[0]),
                        "close": float(ticker_df["Close"].iloc[0]),
                        "volume": int(ticker_df["Volume"].iloc[0]),
                        "change_pct": 0.0,
                    }
                    t = yf.Ticker(ticker)
                    prev_start = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
                    hist = t.history(start=prev_start, end=ds)
                    if not hist.empty:
                        prev_close = float(hist["Close"].iloc[-1])
                        row["change_pct"] = round((row["close"] - prev_close) / prev_close * 100, 2)
                    rows.append(row)
                except Exception as e:
                    result["log"].append(f"[WARN] {ticker}: {e}")

        else:
            # v3/v4: fetch per ticker (simulating Dynamic Task Mapping)
            rows = []
            for ticker in TICKERS:
                t = yf.Ticker(ticker)
                df = t.history(start=ds, end=end)
                if df.empty:
                    result["log"].append(f"[SKIP] {ticker} no data")
                    continue
                close = float(df["Close"].iloc[0])
                prev_start = (datetime.strptime(ds, "%Y-%m-%d") - timedelta(days=7)).strftime("%Y-%m-%d")
                hist = t.history(start=prev_start, end=ds)
                change_pct = 0.0
                if not hist.empty:
                    prev_close = float(hist["Close"].iloc[-1])
                    change_pct = round((close - prev_close) / prev_close * 100, 2)
                rows.append({
                    "date": ds, "ticker": ticker,
                    "open": float(df["Open"].iloc[0]),
                    "high": float(df["High"].iloc[0]),
                    "low": float(df["Low"].iloc[0]),
                    "close": close,
                    "volume": int(df["Volume"].iloc[0]),
                    "change_pct": change_pct,
                })
                result["log"].append(f"  [OK] {ticker}: close={close:,.0f}, chg={change_pct:+.2f}%")

        if not rows:
            result["log"].append(f"[SKIP] {ds} no valid data")
            result["status"] = "skipped"
            return result

        combined_df = pd.DataFrame(rows)
        result["log"].append(f"Collected {len(rows)} tickers")
        result["row_count"] = len(rows)

        # Save CSV
        prices_dir = os.path.join(PROJECT_DIR, "data", "prices")
        os.makedirs(prices_dir, exist_ok=True)
        csv_path = os.path.join(prices_dir, f"{ds}.csv")
        combined_df.to_csv(csv_path, index=False)
        result["log"].append(f"CSV saved: {csv_path}")
        result["csv_path"] = csv_path

        # Validate
        anomaly_count = int((combined_df["change_pct"].abs() > 30).sum())
        anomaly_rate = anomaly_count / len(combined_df) if len(combined_df) > 0 else 0
        result["anomaly_count"] = anomaly_count
        result["anomaly_rate"] = anomaly_rate

        # Branch decision (v3/v4)
        if phase in ("v3", "v4"):
            if anomaly_rate == 0:
                branch = "clean_load"
            elif anomaly_rate <= 0.20:
                branch = "filtered_load"
            else:
                branch = "quarantine"
            result["branch"] = branch
            result["log"].append(f"Branch: {branch} (anomaly_rate={anomaly_rate:.2%})")

            if branch == "quarantine":
                qdir = os.path.join(PROJECT_DIR, "data", "quarantine")
                os.makedirs(qdir, exist_ok=True)
                shutil.copy2(csv_path, os.path.join(qdir, f"{ds}.csv"))
                result["log"].append(f"[QUARANTINE] Data quarantined")
                result["status"] = "quarantined"
                return result
            elif branch == "filtered_load":
                combined_df = combined_df[combined_df["change_pct"].abs() <= 30]
                result["log"].append(f"Filtered: {len(combined_df)} rows remain")

        # Load to DB (v2/v3/v4)
        if phase != "v1":
            conn = ensure_table()
            combined_df["loaded_at"] = datetime.now().isoformat()
            for _, row in combined_df.iterrows():
                conn.execute("""
                    INSERT OR REPLACE INTO daily_prices
                    (date, ticker, open, high, low, close, volume, change_pct, loaded_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (row["date"], row["ticker"], row["open"], row["high"],
                      row["low"], row["close"], row["volume"], row["change_pct"], row["loaded_at"]))
            conn.commit()
            count = conn.execute("SELECT COUNT(*) FROM daily_prices WHERE date=?", (ds,)).fetchone()[0]
            conn.close()
            result["log"].append(f"[DB] {count} rows in DB for {ds}")
            result["db_rows"] = count

        # Callback (v4)
        if phase == "v4":
            result["log"].append(f"[SUCCESS] stock_pipeline_v4 completed - {ds}")

        result["status"] = "success"
        result["finished"] = datetime.now().isoformat()

    except Exception as e:
        result["status"] = "failed"
        result["error"] = str(e)
        result["log"].append(f"[ERROR] {e}")
        result["log"].append(traceback.format_exc())

        # on_failure_callback (v4)
        if phase == "v4":
            alerts_dir = os.path.join(PROJECT_DIR, "data", "alerts")
            os.makedirs(alerts_dir, exist_ok=True)
            path = os.path.join(alerts_dir, f"{ds}_failure.txt")
            with open(path, "w", encoding="utf-8") as f:
                f.write(f"[FAILURE] DAG: stock_pipeline_{phase}\n")
                f.write(f"          Date: {ds}\n")
                f.write(f"          Error: {e}\n")
            result["log"].append(f"[ALERT] Failure report: {path}")

    return result


# ── HTML Template ─────────────────────────────────────────────────
HTML = """
<!DOCTYPE html>
<html lang="ko">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Airflow HW Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { font-family: 'Segoe UI', sans-serif; background: #0f1117; color: #e1e4e8; }
        .header { background: linear-gradient(135deg, #1a1f36, #2d1b69); padding: 20px 30px;
                   border-bottom: 1px solid #30363d; }
        .header h1 { font-size: 22px; color: #58a6ff; }
        .header p { font-size: 13px; color: #8b949e; margin-top: 4px; }
        .container { max-width: 1200px; margin: 0 auto; padding: 20px; }

        /* Cards */
        .stats { display: grid; grid-template-columns: repeat(4, 1fr); gap: 15px; margin: 20px 0; }
        .stat-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                     padding: 16px; text-align: center; }
        .stat-card .value { font-size: 28px; font-weight: 700; color: #58a6ff; }
        .stat-card .label { font-size: 12px; color: #8b949e; margin-top: 4px; }

        /* Trigger form */
        .trigger-panel { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                         padding: 20px; margin: 20px 0; }
        .trigger-panel h2 { font-size: 16px; margin-bottom: 12px; color: #c9d1d9; }
        .form-row { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
        input, select { background: #0d1117; border: 1px solid #30363d; color: #c9d1d9;
                       padding: 8px 12px; border-radius: 6px; font-size: 14px; }
        button { background: #238636; color: white; border: none; padding: 8px 20px;
                 border-radius: 6px; cursor: pointer; font-size: 14px; font-weight: 600; }
        button:hover { background: #2ea043; }
        button.danger { background: #da3633; }
        button.danger:hover { background: #f85149; }
        button.secondary { background: #30363d; }
        button.secondary:hover { background: #484f58; }

        /* Run history */
        .history { margin: 20px 0; }
        .history h2 { font-size: 16px; margin-bottom: 12px; color: #c9d1d9; }
        .run-card { background: #161b22; border: 1px solid #30363d; border-radius: 8px;
                    padding: 14px; margin-bottom: 10px; }
        .run-header { display: flex; justify-content: space-between; align-items: center; }
        .run-header .date { font-weight: 600; color: #c9d1d9; }
        .badge { padding: 2px 8px; border-radius: 10px; font-size: 12px; font-weight: 600; }
        .badge.success { background: #238636; color: white; }
        .badge.failed { background: #da3633; color: white; }
        .badge.skipped { background: #6e7681; color: white; }
        .badge.running { background: #d29922; color: black; }
        .badge.quarantined { background: #f0883e; color: black; }
        .run-log { margin-top: 8px; font-family: 'Consolas', monospace; font-size: 12px;
                   background: #0d1117; border-radius: 4px; padding: 10px; max-height: 200px;
                   overflow-y: auto; white-space: pre-wrap; color: #8b949e; }

        /* DB table */
        .db-section { margin: 20px 0; }
        .db-section h2 { font-size: 16px; margin-bottom: 12px; color: #c9d1d9; }
        table { width: 100%; border-collapse: collapse; font-size: 13px; }
        th { background: #161b22; color: #8b949e; padding: 8px 10px; text-align: left;
             border-bottom: 1px solid #30363d; }
        td { padding: 8px 10px; border-bottom: 1px solid #21262d; }
        tr:hover td { background: #161b22; }
        .positive { color: #3fb950; }
        .negative { color: #f85149; }

        /* Date summary bar chart */
        .date-bar { display: flex; align-items: center; gap: 8px; margin: 3px 0; }
        .date-bar .label { width: 90px; font-size: 12px; color: #8b949e; }
        .date-bar .bar { height: 18px; background: #238636; border-radius: 3px;
                         display: flex; align-items: center; padding-left: 6px;
                         font-size: 11px; color: white; font-weight: 600; }

        .spinner { display: inline-block; width: 16px; height: 16px;
                   border: 2px solid #30363d; border-top: 2px solid #58a6ff;
                   border-radius: 50%; animation: spin 0.8s linear infinite; }
        @keyframes spin { to { transform: rotate(360deg); } }
    </style>
</head>
<body>
    <div class="header">
        <h1>Apache Airflow HW &mdash; Pipeline Dashboard</h1>
        <p>Windows localhost | Phase 1-4 pipeline runner | DB: data/stock.db</p>
    </div>
    <div class="container">

        <!-- Stats -->
        <div class="stats">
            <div class="stat-card">
                <div class="value">{{ stats.total_rows }}</div>
                <div class="label">Total DB Rows</div>
            </div>
            <div class="stat-card">
                <div class="value">{{ stats.dates }}</div>
                <div class="label">Dates</div>
            </div>
            <div class="stat-card">
                <div class="value">{{ stats.tickers }}</div>
                <div class="label">Tickers</div>
            </div>
            <div class="stat-card">
                <div class="value">{{ runs|length }}</div>
                <div class="label">Runs</div>
            </div>
        </div>

        <!-- Trigger -->
        <div class="trigger-panel">
            <h2>Run Pipeline</h2>
            <form method="POST" action="/trigger" class="form-row">
                <label>Date:</label>
                <input type="date" name="ds" value="{{ default_date }}" required>
                <label>Phase:</label>
                <select name="phase">
                    <option value="v1">v1 - Basic</option>
                    <option value="v2">v2 - ETL + DB</option>
                    <option value="v3">v3 - Parallel + Branch</option>
                    <option value="v4" selected>v4 - Production</option>
                </select>
                <button type="submit">Trigger</button>
            </form>
            <br>
            <form method="POST" action="/backfill" class="form-row" style="margin-top:10px">
                <label>Backfill:</label>
                <input type="date" name="start_date" value="2025-01-06">
                <span style="color:#8b949e">~</span>
                <input type="date" name="end_date" value="2025-01-10">
                <select name="phase">
                    <option value="v4" selected>v4</option>
                    <option value="v3">v3</option>
                    <option value="v2">v2</option>
                </select>
                <button type="submit" class="secondary">Backfill</button>
                <button type="submit" formaction="/reset" class="danger">Reset DB</button>
            </form>
        </div>

        <!-- Date Summary -->
        {% if stats.date_summary %}
        <div class="db-section">
            <h2>Date Summary</h2>
            {% for date, cnt in stats.date_summary %}
            <div class="date-bar">
                <span class="label">{{ date }}</span>
                <div class="bar" style="width: {{ cnt * 40 }}px;">{{ cnt }}</div>
            </div>
            {% endfor %}
        </div>
        {% endif %}

        <!-- Run History -->
        <div class="history">
            <h2>Run History</h2>
            {% for run in runs|reverse %}
            <div class="run-card">
                <div class="run-header">
                    <span class="date">{{ run.ds }} &mdash; Phase {{ run.phase }}</span>
                    <span class="badge {{ run.status }}">{{ run.status }}</span>
                </div>
                {% if run.branch %}<div style="font-size:12px;color:#8b949e;margin-top:4px">Branch: {{ run.branch }}</div>{% endif %}
                <div class="run-log">{{ run.log | join('\\n') }}</div>
            </div>
            {% endfor %}
            {% if not runs %}
            <p style="color:#8b949e">No runs yet. Trigger a pipeline above.</p>
            {% endif %}
        </div>

        <!-- DB Data -->
        {% if stats.rows %}
        <div class="db-section">
            <h2>DB Data (latest 100 rows)</h2>
            <table>
                <tr><th>Date</th><th>Ticker</th><th>Close</th><th>Change %</th><th>Loaded At</th></tr>
                {% for date, ticker, close, change_pct, loaded_at in stats.rows %}
                <tr>
                    <td>{{ date }}</td>
                    <td>{{ ticker }}</td>
                    <td>{{ "{:,.0f}".format(close) if close else "-" }}</td>
                    <td class="{{ 'positive' if change_pct and change_pct > 0 else 'negative' }}">
                        {{ "{:+.2f}%".format(change_pct) if change_pct else "-" }}
                    </td>
                    <td style="color:#8b949e;font-size:12px">{{ loaded_at[:19] if loaded_at else "-" }}</td>
                </tr>
                {% endfor %}
            </table>
        </div>
        {% endif %}
    </div>
</body>
</html>
"""


# ── Routes ────────────────────────────────────────────────────────
@app.route("/")
def index():
    stats = get_db_stats()
    default_date = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")
    return render_template_string(HTML, stats=stats, runs=run_history, default_date=default_date)


@app.route("/trigger", methods=["POST"])
def trigger():
    ds = request.form.get("ds", "2025-01-13")
    phase = request.form.get("phase", "v4")
    result = run_pipeline(ds, phase)
    run_history.append(result)
    return redirect(url_for("index"))


@app.route("/backfill", methods=["POST"])
def backfill():
    start = request.form.get("start_date", "2025-01-06")
    end = request.form.get("end_date", "2025-01-10")
    phase = request.form.get("phase", "v4")

    current = datetime.strptime(start, "%Y-%m-%d")
    end_dt = datetime.strptime(end, "%Y-%m-%d")
    while current <= end_dt:
        if current.weekday() < 5:  # weekdays only
            ds = current.strftime("%Y-%m-%d")
            result = run_pipeline(ds, phase)
            run_history.append(result)
        current += timedelta(days=1)
    return redirect(url_for("index"))


@app.route("/reset", methods=["POST"])
def reset():
    # Reset DB and data
    for d in ["data/prices", "data/quarantine", "data/alerts"]:
        p = os.path.join(PROJECT_DIR, d)
        if os.path.exists(p):
            shutil.rmtree(p)
            os.makedirs(p, exist_ok=True)
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    run_history.clear()
    return redirect(url_for("index"))


@app.route("/health")
def health():
    return jsonify({"status": "ok"})


# ── Main ──────────────────────────────────────────────────────────
if __name__ == "__main__":
    os.makedirs(os.path.join(PROJECT_DIR, "data", "prices"), exist_ok=True)
    os.makedirs(os.path.join(PROJECT_DIR, "data", "quarantine"), exist_ok=True)
    os.makedirs(os.path.join(PROJECT_DIR, "data", "alerts"), exist_ok=True)
    print("=" * 50)
    print("  Airflow HW Dashboard")
    print("  http://localhost:5050")
    print("=" * 50)
    app.run(host="0.0.0.0", port=5050, debug=False)
