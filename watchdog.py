from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import shutil
import socket
import sqlite3
import subprocess
import sys
import time
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse
from xml.etree import ElementTree as ET

import requests
from bs4 import BeautifulSoup

CREATE_NO_WINDOW = 0x08000000 if os.name == "nt" else 0

APP_NAME = "Watchdog"
RUNTIME_ROOT = Path(__file__).resolve().parent
APP_DIR = Path(os.getenv("APPDATA") or str(Path.home())) / APP_NAME
CONFIG_PATH = APP_DIR / "config.json"
STATE_PATH = APP_DIR / "state.json"
HEALTH_PATH = APP_DIR / "health.json"
LOG_PATH = APP_DIR / "watchdog.log"
NOTIFICATION_LOG_PATH = APP_DIR / "notification_history.jsonl"
DB_PATH = APP_DIR / "watchdog.sqlite3"
CACHE_DIR = APP_DIR / "cache"
REPORTS_DIR = APP_DIR / "reports"
SNAPSHOT_DIR = APP_DIR / "snapshots"
SUMMARY_DIR = APP_DIR / "summaries"
LOCK_PORT = 54137

SEC_TICKER_MAP_URL = "https://www.sec.gov/files/company_tickers.json"
SEC_SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
USASPENDING_BASE = "https://api.usaspending.gov"
NEWS_RSS = "https://news.google.com/rss/search?q={query}&hl=en-US&gl=US&ceid=US:en"

DEFAULT_CONFIG: dict[str, Any] = {
    "tickers": [],
    "poll_minutes": 15,
    "reports_per_company": 12,
    "news_per_query": 8,
    "news_queries_per_company": 8,
    "max_contract_exhibits": 15,
    "user_agent": "Watchdog/2.0 your_email@example.com",
    "download_forms": ["10-K", "10-Q", "8-K", "20-F", "6-K", "DEF 14A"],
    "news_terms_per_bucket": 5,
    "network_retries": 4,
    "network_timeout_seconds": 60,
    "network_backoff_seconds": 2,
    "news_relevance_threshold": 5,
    "news_notification_limit": 5,
    "news_dedupe_window_days": 7,
    "snapshot_retention_days": 30,
    "report_retention_days": 60,
    "db_retention_days": 180,
    "max_log_mb": 5,
    "max_log_backups": 5,
    "task_start_delay_minutes": 1,
    "save_timestamped_snapshots": True,
    "summary_enabled": True,
}

PIPELINE_KEYWORDS = [
    "pipeline",
    "backlog",
    "program",
    "programs",
    "product candidate",
    "product candidates",
    "platform",
    "platforms",
    "offering",
    "offerings",
    "segment",
    "segments",
    "roadmap",
    "development",
    "projects",
    "project",
    "launch",
    "commercialization",
    "commercial",
    "manufacturing",
    "service line",
    "services",
    "solutions",
    "products",
]

INVESTMENT_KEYWORDS = [
    "investment",
    "investments",
    "invested",
    "equity stake",
    "minority stake",
    "joint venture",
    "joint ventures",
    "subsidiary",
    "subsidiaries",
    "acquisition",
    "acquired",
    "portfolio",
    "strategic investment",
    "equity method",
    "associate",
    "associates",
]

CONTRACT_KEYWORDS = [
    "agreement",
    "contract",
    "material contract",
    "license agreement",
    "supply agreement",
    "collaboration",
    "distribution agreement",
    "credit agreement",
    "lease",
    "amendment",
    "purchase agreement",
    "merger agreement",
    "service agreement",
    "government award",
]

EXHIBIT_HINTS = [
    "ex10",
    "agreement",
    "contract",
    "license",
    "collaboration",
    "supply",
    "lease",
    "credit",
    "facility",
    "purchase",
    "merger",
    "amend",
    "employment",
    "services",
]

SEVERITY_WORDS = {
    "critical": ["bankruptcy", "fraud", "probe", "investigation", "restatement", "delisting", "default"],
    "high": ["earnings", "guidance", "sec", "contract", "award", "approval", "lawsuit", "acquisition", "merger", "supply", "recall"],
    "medium": ["launch", "partnership", "collaboration", "backlog", "facility", "investment"],
}

STOPWORDS = {
    "company",
    "companies",
    "business",
    "products",
    "services",
    "segment",
    "segments",
    "program",
    "programs",
    "platform",
    "platforms",
    "pipeline",
    "project",
    "projects",
    "backlog",
    "commercial",
    "development",
    "agreement",
    "contract",
    "contracts",
    "annual",
    "quarterly",
    "report",
    "reports",
    "management",
    "operations",
    "market",
    "markets",
    "revenue",
    "customer",
    "customers",
    "solution",
    "solutions",
    "technology",
    "technologies",
    "product",
    "service",
    "inc",
    "corp",
    "corporation",
    "ltd",
    "llc",
}

SOURCE_NAMES = {"sec", "news", "usaspending", "general"}
_SOURCE_COOLDOWNS: dict[str, float] = {}
_PATHS_BOOT_LOGGED = False


@dataclass
class Filing:
    accession: str
    form: str
    filing_date: str
    primary_document: str
    primary_description: str
    base_url: str
    filing_url: str
    index_json_url: str


# ---------- paths + storage ----------

def ensure_dirs() -> None:
    global _PATHS_BOOT_LOGGED
    for path in [APP_DIR, CACHE_DIR, REPORTS_DIR, SNAPSHOT_DIR, SUMMARY_DIR]:
        path.mkdir(parents=True, exist_ok=True)
    init_db()
    if not _PATHS_BOOT_LOGGED:
        _PATHS_BOOT_LOGGED = True
        log(f"Storage layout: runtime_root={RUNTIME_ROOT} writable_root={APP_DIR}")


def runtime_paths_summary() -> dict[str, Any]:
    return {
        "runtime_root": str(RUNTIME_ROOT),
        "storage_root": str(APP_DIR),
        "config_path": str(CONFIG_PATH),
        "state_path": str(STATE_PATH),
        "health_path": str(HEALTH_PATH),
        "db_path": str(DB_PATH),
        "log_path": str(LOG_PATH),
        "cache_dir": str(CACHE_DIR),
        "reports_dir": str(REPORTS_DIR),
        "snapshots_dir": str(SNAPSHOT_DIR),
        "summaries_dir": str(SUMMARY_DIR),
        "frozen": bool(getattr(sys, "frozen", False)),
        "portable": False,
    }


def explorer_target_dir() -> Path:
    return APP_DIR


def company_dir(ticker: str) -> Path:
    path = SNAPSHOT_DIR / ticker
    path.mkdir(parents=True, exist_ok=True)
    return path


def reports_dir_for(ticker: str) -> Path:
    path = REPORTS_DIR / ticker
    path.mkdir(parents=True, exist_ok=True)
    return path


def summaries_dir_for(ticker: str) -> Path:
    path = SUMMARY_DIR / ticker
    path.mkdir(parents=True, exist_ok=True)
    return path


# ---------- JSON / text helpers ----------

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def utc_iso(dt: datetime | None = None) -> str:
    return (dt or now_utc()).isoformat()


def atomic_write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(text, encoding="utf-8")
    tmp.replace(path)


def load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return default


def save_json(path: Path, data: Any) -> None:
    atomic_write_text(path, json.dumps(data, indent=2, ensure_ascii=False))


# ---------- logging ----------

def rotate_logs_if_needed(max_mb: int = 5, backups: int = 5) -> None:
    if not LOG_PATH.exists():
        return
    max_bytes = max_mb * 1024 * 1024
    if LOG_PATH.stat().st_size <= max_bytes:
        return
    for idx in range(backups, 0, -1):
        older = LOG_PATH.with_name(f"{LOG_PATH.name}.{idx}")
        newer = LOG_PATH if idx == 1 else LOG_PATH.with_name(f"{LOG_PATH.name}.{idx - 1}")
        if older.exists():
            older.unlink(missing_ok=True)
        if newer.exists():
            newer.replace(older)


def log(message: str) -> None:
    ensure_dirs()
    stamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{stamp}] {message}"
    print(line)
    try:
        cfg = merged_config()
        rotate_logs_if_needed(int(cfg.get("max_log_mb", 5)), int(cfg.get("max_log_backups", 5)))
        with LOG_PATH.open("a", encoding="utf-8") as fh:
            fh.write(line + "\n")
    except Exception:
        pass


# ---------- config ----------

def normalize_ticker(ticker: str) -> str:
    return re.sub(r"[^A-Z0-9.\-]", "", ticker.strip().upper())


def merged_config(config: dict[str, Any] | None = None) -> dict[str, Any]:
    merged = DEFAULT_CONFIG.copy()
    merged.update(load_json(CONFIG_PATH, {}))
    if config:
        merged.update(config)
    merged["tickers"] = [normalize_ticker(t) for t in merged.get("tickers", []) if normalize_ticker(t)]
    return merged


def configure(force: bool = False) -> dict[str, Any]:
    ensure_dirs()
    config = merged_config()
    if config.get("tickers") and not force:
        return config

    print(f"\n{APP_NAME} first-run setup")
    tickers = input("Enter ticker(s), comma-separated: ").strip()
    user_agent = input(
        "SEC User-Agent (name/app + email, required by SEC fair-access guidance) "
        f"[{DEFAULT_CONFIG['user_agent']}]: "
    ).strip()
    poll = input(f"Poll interval in minutes [{DEFAULT_CONFIG['poll_minutes']}]: ").strip()

    parsed = DEFAULT_CONFIG.copy()
    parsed["tickers"] = [normalize_ticker(t) for t in tickers.split(",") if normalize_ticker(t)]
    if user_agent:
        parsed["user_agent"] = user_agent
    if poll.isdigit() and int(poll) > 0:
        parsed["poll_minutes"] = int(poll)

    if not parsed["tickers"]:
        raise SystemExit("No tickers supplied.")

    save_json(CONFIG_PATH, parsed)
    print(f"Saved config to {CONFIG_PATH}")
    return parsed


# ---------- health ----------

def default_health() -> dict[str, Any]:
    return {
        "app_name": APP_NAME,
        "status": "idle",
        "status_detail": "",
        "pid": None,
        "started_at_utc": "",
        "last_loop_started_utc": "",
        "last_loop_completed_utc": "",
        "last_success_utc": "",
        "last_error_utc": "",
        "last_error": "",
        "last_sleep_seconds": 0,
        "consecutive_failures": 0,
        "current_ticker": "",
        "tickers": [],
        "task": {},
        "sources": {
            source: {
                "state": "idle",
                "last_success_utc": "",
                "last_error_utc": "",
                "last_error": "",
                "cooldown_until_utc": "",
                "consecutive_failures": 0,
            }
            for source in sorted(SOURCE_NAMES)
        },
    }


def load_health() -> dict[str, Any]:
    return load_json(HEALTH_PATH, default_health())


def save_health(health: dict[str, Any]) -> None:
    save_json(HEALTH_PATH, health)


def update_health(**updates: Any) -> dict[str, Any]:
    health = load_health()
    health.update(updates)
    save_health(health)
    return health


def mark_source(source: str, ok: bool, error: str = "", cooldown_seconds: int = 0) -> None:
    if source not in SOURCE_NAMES:
        source = "general"
    health = load_health()
    entry = health.setdefault("sources", {}).setdefault(source, default_health()["sources"][source])
    now = utc_iso()
    if ok:
        entry["state"] = "ok"
        entry["last_success_utc"] = now
        entry["last_error"] = ""
        entry["cooldown_until_utc"] = ""
        entry["consecutive_failures"] = 0
        _SOURCE_COOLDOWNS.pop(source, None)
    else:
        entry["state"] = "cooldown" if cooldown_seconds else "error"
        entry["last_error_utc"] = now
        entry["last_error"] = error[:400]
        entry["consecutive_failures"] = int(entry.get("consecutive_failures", 0)) + 1
        if cooldown_seconds > 0:
            until = now_utc() + timedelta(seconds=cooldown_seconds)
            entry["cooldown_until_utc"] = until.isoformat()
            _SOURCE_COOLDOWNS[source] = until.timestamp()
    save_health(health)


# ---------- DB / events ----------

def init_db() -> None:
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_utc TEXT NOT NULL,
                kind TEXT NOT NULL,
                ticker TEXT NOT NULL,
                title TEXT NOT NULL,
                body TEXT NOT NULL,
                link TEXT NOT NULL,
                severity TEXT NOT NULL,
                score INTEGER NOT NULL DEFAULT 0,
                payload_json TEXT NOT NULL DEFAULT '{}'
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_events_kind_ticker_created ON events(kind, ticker, created_utc DESC)")
        conn.commit()
    finally:
        conn.close()


def append_event(
    kind: str,
    ticker: str,
    title: str,
    body: str,
    *,
    link: str = "",
    severity: str = "",
    score: int = 0,
    payload: dict[str, Any] | None = None,
) -> None:
    init_db()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(
            """
            INSERT INTO events (created_utc, kind, ticker, title, body, link, severity, score, payload_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                utc_iso(),
                kind,
                ticker,
                title,
                body,
                link,
                severity,
                int(score),
                json.dumps(payload or {}, ensure_ascii=False),
            ),
        )
        conn.commit()
    finally:
        conn.close()


def list_events(kind: str | None = None, ticker: str | None = None, limit: int = 200) -> list[dict[str, Any]]:
    init_db()
    sql = "SELECT created_utc, kind, ticker, title, body, link, severity, score, payload_json FROM events"
    clauses: list[str] = []
    params: list[Any] = []
    if kind:
        clauses.append("kind = ?")
        params.append(kind)
    if ticker:
        clauses.append("ticker = ?")
        params.append(ticker)
    if clauses:
        sql += " WHERE " + " AND ".join(clauses)
    sql += " ORDER BY created_utc DESC LIMIT ?"
    params.append(limit)

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute(sql, params).fetchall()
        out: list[dict[str, Any]] = []
        for row in rows:
            payload = {}
            try:
                payload = json.loads(row["payload_json"] or "{}")
            except Exception:
                payload = {}
            out.append(
                {
                    "created_utc": row["created_utc"],
                    "kind": row["kind"],
                    "ticker": row["ticker"],
                    "title": row["title"],
                    "body": row["body"],
                    "link": row["link"],
                    "severity": row["severity"],
                    "score": row["score"],
                    "payload": payload,
                }
            )
        return out
    finally:
        conn.close()


def prune_old_data(config: dict[str, Any]) -> None:
    cutoff = now_utc() - timedelta(days=int(config.get("db_retention_days", 180)))
    cutoff_text = cutoff.isoformat()
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute("DELETE FROM events WHERE created_utc < ?", (cutoff_text,))
        conn.commit()
    finally:
        conn.close()

    def prune_tree(root: Path, days: int) -> None:
        if not root.exists():
            return
        threshold = time.time() - days * 86400
        for path in root.rglob("*"):
            try:
                if path.is_file() and path.stat().st_mtime < threshold:
                    path.unlink(missing_ok=True)
            except Exception:
                pass

    prune_tree(REPORTS_DIR, int(config.get("report_retention_days", 60)))


# ---------- summary files ----------

def latest_summary_path(ticker: str) -> Path | None:
    target = summaries_dir_for(ticker)
    files = sorted(target.glob("summary_*.txt"), reverse=True)
    return files[0] if files else None


def latest_summary_text(ticker: str) -> str:
    path = latest_summary_path(ticker)
    if not path:
        return ""
    try:
        return path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return ""


def record_snapshot_summary(snapshot: dict[str, Any]) -> None:
    ticker = snapshot.get("ticker", "")
    if not ticker:
        return
    path = summaries_dir_for(ticker) / f"summary_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    lines = [
        f"Ticker: {snapshot.get('ticker', '')}",
        f"Company: {snapshot.get('company', '')}",
        f"Generated: {snapshot.get('generated_at_utc', '')}",
        "",
        "Pipeline terms:",
    ]
    for term in snapshot.get("pipeline", {}).get("terms", [])[:10]:
        lines.append(f" - {term}")
    lines.append("")
    lines.append("Investment entities:")
    for term in snapshot.get("investments", {}).get("entities", [])[:10]:
        lines.append(f" - {term}")
    lines.append("")
    lines.append("Contract counterparties:")
    for term in snapshot.get("contracts", {}).get("counterparties", [])[:10]:
        lines.append(f" - {term}")
    lines.append("")
    lines.append("Recent filing forms:")
    for filing in snapshot.get("recent_filings", [])[:12]:
        lines.append(f" - {filing.get('filing_date', '')} | {filing.get('form', '')} | {filing.get('description', '') or filing.get('primary_document', '')}")
    atomic_write_text(path, "\n".join(lines).strip() + "\n")


# ---------- subprocess helpers ----------

def subprocess_run_hidden(
    args: list[str],
    *,
    capture_output: bool = False,
    text: bool = False,
    check: bool = False,
) -> subprocess.CompletedProcess:
    kwargs: dict[str, Any] = {"capture_output": capture_output, "text": text, "check": check}
    if os.name == "nt":
        kwargs["creationflags"] = getattr(subprocess, "CREATE_NO_WINDOW", CREATE_NO_WINDOW)
    return subprocess.run(args, **kwargs)


def subprocess_popen_hidden(args: list[str], **kwargs: Any) -> subprocess.Popen:
    if os.name == "nt":
        kwargs.setdefault("creationflags", getattr(subprocess, "CREATE_NO_WINDOW", CREATE_NO_WINDOW))
    return subprocess.Popen(args, **kwargs)


# ---------- notifications + tasking ----------

def append_notification_history(
    kind: str,
    ticker: str,
    title: str,
    body: str,
    extra: dict[str, Any] | None = None,
) -> None:
    record = {
        "timestamp_utc": utc_iso(),
        "kind": kind,
        "ticker": ticker,
        "title": title,
        "body": body,
        "extra": extra or {},
    }
    with NOTIFICATION_LOG_PATH.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(record, ensure_ascii=False) + "\n")


def send_notification(title: str, body: str, link: str = "") -> str:
    body = (body or "")[:240]
    try:
        from win11toast import toast

        toast(title, body, duration="short")
        return "win11toast"
    except Exception:
        pass

    try:
        from win10toast import ToastNotifier

        ToastNotifier().show_toast(title, body, duration=8, threaded=True)
        return "win10toast"
    except Exception:
        pass

    if os.name == "nt":
        script = (
            f"$title = @'\n{title}\n'@\n"
            f"$body = @'\n{body}\n'@\n"
            "Add-Type -AssemblyName System.Windows.Forms; Add-Type -AssemblyName System.Drawing;\n"
            "$n = New-Object System.Windows.Forms.NotifyIcon;\n"
            "$n.Icon = [System.Drawing.SystemIcons]::Information;\n"
            "$n.BalloonTipTitle = $title;\n"
            "$n.BalloonTipText = $body;\n"
            "$n.Visible = $true;\n"
            "$n.ShowBalloonTip(8000);\n"
            "Start-Sleep -Seconds 10;\n"
            "$n.Dispose();"
        )
        try:
            subprocess_popen_hidden(
                [
                    "powershell",
                    "-NoProfile",
                    "-NonInteractive",
                    "-WindowStyle",
                    "Hidden",
                    "-ExecutionPolicy",
                    "Bypass",
                    "-Command",
                    script,
                ],
                stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
                close_fds=True,
            )
            return "powershell"
        except Exception:
            pass
    return "none"


def install_startup_task(command: str | None = None, delay_minutes: int | None = None) -> None:
    config = merged_config()
    delay_minutes = delay_minutes if delay_minutes is not None else int(config.get("task_start_delay_minutes", 1))
    if command is None:
        script = Path(__file__).resolve()
        python_exe = Path(sys.executable)
        pythonw = python_exe.with_name("pythonw.exe")
        runner = pythonw if pythonw.exists() else python_exe
        command = f'"{runner}" "{script}" --run'
    delay = f"{max(0, int(delay_minutes)):04d}:00"
    args = [
        "schtasks",
        "/Create",
        "/TN",
        APP_NAME,
        "/SC",
        "ONLOGON",
        "/TR",
        command,
        "/RL",
        "HIGHEST",
        "/F",
    ]
    if delay_minutes > 0:
        args.extend(["/DELAY", delay])
    subprocess_run_hidden(args, check=True)
    log(f"Installed scheduled task '{APP_NAME}'")


def query_startup_task() -> dict[str, Any]:
    try:
        proc = subprocess_run_hidden(
            ["schtasks", "/Query", "/TN", APP_NAME, "/FO", "LIST", "/V"],
            capture_output=True,
            text=True,
            check=True,
        )
        parsed: dict[str, str] = {}
        for line in proc.stdout.splitlines():
            if ":" not in line:
                continue
            key, value = line.split(":", 1)
            parsed[key.strip()] = value.strip()
        return {"installed": True, "raw": proc.stdout, "parsed": parsed}
    except Exception as exc:
        return {"installed": False, "error": str(exc), "raw": ""}


def is_process_running(pid: int | None) -> bool:
    if not pid:
        return False
    try:
        if os.name == "nt":
            proc = subprocess_run_hidden(["tasklist", "/FI", f"PID eq {pid}"], capture_output=True, text=True)
            return str(pid) in proc.stdout
        os.kill(int(pid), 0)
        return True
    except Exception:
        return False


def stop_running_watcher(pid: int | None = None) -> bool:
    pid = pid or load_health().get("pid")
    if not pid:
        return False
    try:
        if os.name == "nt":
            subprocess_run_hidden(["taskkill", "/PID", str(pid), "/T", "/F"], check=True, capture_output=True, text=True)
        else:
            os.kill(int(pid), 15)
        return True
    except Exception:
        return False


# ---------- network ----------

def source_from_url(url: str) -> str:
    lowered = url.lower()
    if "sec.gov" in lowered:
        return "sec"
    if "usaspending" in lowered:
        return "usaspending"
    if "news.google.com" in lowered:
        return "news"
    return "general"


def is_source_on_cooldown(source: str) -> bool:
    return _SOURCE_COOLDOWNS.get(source, 0) > time.time()


def get_session(user_agent: str, config: dict[str, Any] | None = None) -> requests.Session:
    config = merged_config(config)
    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": user_agent,
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json, text/html, application/xhtml+xml, application/xml;q=0.9, */*;q=0.8",
            "Connection": "keep-alive",
        }
    )
    setattr(session, "cw_config", config)
    return session


def _request(
    session: requests.Session,
    method: str,
    url: str,
    *,
    payload: dict[str, Any] | None = None,
    timeout: int | None = None,
    cache_path: Path | None = None,
    cache_ttl_seconds: int | None = None,
) -> requests.Response:
    config = getattr(session, "cw_config", DEFAULT_CONFIG)
    retries = int(config.get("network_retries", DEFAULT_CONFIG["network_retries"]))
    base_backoff = float(config.get("network_backoff_seconds", DEFAULT_CONFIG["network_backoff_seconds"]))
    timeout = timeout or int(config.get("network_timeout_seconds", DEFAULT_CONFIG["network_timeout_seconds"]))
    source = source_from_url(url)

    if is_source_on_cooldown(source) and cache_path and cache_path.exists():
        age = time.time() - cache_path.stat().st_mtime
        if cache_ttl_seconds is None or age <= cache_ttl_seconds:
            fake = requests.Response()
            fake.status_code = 200
            fake._content = cache_path.read_bytes()
            fake.url = url
            return fake

    last_exc: Exception | None = None
    for attempt in range(retries + 1):
        try:
            if method.upper() == "POST":
                resp = session.post(url, json=payload, timeout=timeout)
            else:
                resp = session.get(url, timeout=timeout)
            if resp.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"HTTP {resp.status_code}", response=resp)
            resp.raise_for_status()
            if cache_path is not None:
                cache_path.parent.mkdir(parents=True, exist_ok=True)
                cache_path.write_bytes(resp.content)
            mark_source(source, ok=True)
            return resp
        except Exception as exc:
            last_exc = exc
            if attempt >= retries:
                cooldown = int(base_backoff * 10)
                mark_source(source, ok=False, error=str(exc), cooldown_seconds=cooldown)
                break
            sleep_for = base_backoff * (2 ** attempt) + random.uniform(0, 0.75)
            time.sleep(sleep_for)

    if cache_path and cache_path.exists():
        age = time.time() - cache_path.stat().st_mtime
        if cache_ttl_seconds is None or age <= cache_ttl_seconds:
            fake = requests.Response()
            fake.status_code = 200
            fake._content = cache_path.read_bytes()
            fake.url = url
            return fake

    raise last_exc or RuntimeError(f"Request failed for {url}")


def fetch_json(
    session: requests.Session,
    url: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
    cache_path: Path | None = None,
    cache_ttl_seconds: int | None = None,
) -> Any:
    resp = _request(
        session,
        method,
        url,
        payload=payload,
        cache_path=cache_path,
        cache_ttl_seconds=cache_ttl_seconds,
    )
    return resp.json()


def fetch_text(
    session: requests.Session,
    url: str,
    *,
    cache_path: Path | None = None,
    cache_ttl_seconds: int | None = None,
) -> str:
    resp = _request(session, "GET", url, cache_path=cache_path, cache_ttl_seconds=cache_ttl_seconds)
    return resp.text


def fetch_bytes(
    session: requests.Session,
    url: str,
    *,
    cache_path: Path | None = None,
    cache_ttl_seconds: int | None = None,
) -> bytes:
    resp = _request(session, "GET", url, timeout=120, cache_path=cache_path, cache_ttl_seconds=cache_ttl_seconds)
    return resp.content


# ---------- data collection ----------

def get_ticker_map(session: requests.Session) -> dict[str, dict[str, str]]:
    cache_path = CACHE_DIR / "company_tickers.json"
    raw = fetch_json(session, SEC_TICKER_MAP_URL, cache_path=cache_path, cache_ttl_seconds=60 * 60 * 12)
    mapping: dict[str, dict[str, str]] = {}
    for row in raw.values() if isinstance(raw, dict) else raw:
        ticker = normalize_ticker(str(row.get("ticker", "")))
        if not ticker:
            continue
        mapping[ticker] = {
            "cik": str(row.get("cik_str", "")).zfill(10),
            "title": str(row.get("title", "")).strip(),
        }
    return mapping


def get_submissions(session: requests.Session, cik: str) -> dict[str, Any]:
    data = fetch_json(session, SEC_SUBMISSIONS_URL.format(cik=cik))
    extra = data.get("filings", {}).get("files", []) or []
    for item in extra:
        name = item.get("name")
        if not name:
            continue
        extra_url = f"https://data.sec.gov/submissions/{name}"
        try:
            more = fetch_json(session, extra_url)
            for key, values in more.items():
                data["filings"]["recent"].setdefault(key, [])
                data["filings"]["recent"][key].extend(values)
        except Exception as exc:
            log(f"Could not extend submissions with {name}: {exc}")
    return data


def filings_from_submissions(submissions: dict[str, Any]) -> list[Filing]:
    recent = submissions.get("filings", {}).get("recent", {})
    accessions = recent.get("accessionNumber", [])
    forms = recent.get("form", [])
    dates = recent.get("filingDate", [])
    docs = recent.get("primaryDocument", [])
    descriptions = recent.get("primaryDocDescription", [])
    cik = str(submissions.get("cik", "")).zfill(10)
    out: list[Filing] = []
    for accession, form, filing_date, doc, desc in zip(accessions, forms, dates, docs, descriptions):
        accession_nodash = str(accession).replace("-", "")
        base_url = f"https://www.sec.gov/Archives/edgar/data/{int(cik)}/{accession_nodash}"
        filing_url = f"{base_url}/{doc}" if doc else base_url
        out.append(
            Filing(
                accession=str(accession),
                form=str(form),
                filing_date=str(filing_date),
                primary_document=str(doc),
                primary_description=str(desc),
                base_url=base_url,
                filing_url=filing_url,
                index_json_url=f"{base_url}/index.json",
            )
        )
    return out


def html_to_text(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.extract()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text)


def sentence_candidates(text: str) -> list[str]:
    text = re.sub(r"\s+", " ", text)
    bits = re.split(r"(?<=[.!?])\s+", text)
    cleaned: list[str] = []
    for bit in bits:
        bit = bit.strip()
        if 50 <= len(bit) <= 450:
            cleaned.append(bit)
    return cleaned


def score_sentence(sentence: str, keywords: list[str]) -> int:
    lower = sentence.lower()
    score = 0
    for kw in keywords:
        if kw in lower:
            score += 3 if " " in kw else 1
    score += min(len(sentence) // 120, 3)
    return score


def top_sentences(text: str, keywords: list[str], limit: int = 8) -> list[str]:
    seen: set[str] = set()
    scored: list[tuple[int, str]] = []
    for sent in sentence_candidates(text):
        score = score_sentence(sent, keywords)
        if score <= 0:
            continue
        key = sent.lower()
        if key in seen:
            continue
        seen.add(key)
        scored.append((score, sent))
    scored.sort(key=lambda x: (-x[0], len(x[1])))
    return [s for _, s in scored[:limit]]


def extract_capitalized_terms(lines: list[str], limit: int = 10) -> list[str]:
    counts: Counter[str] = Counter()
    pattern = re.compile(r"\b(?:[A-Z][A-Za-z0-9&'/-]+(?:\s+[A-Z][A-Za-z0-9&'/-]+){0,3})\b")
    for line in lines:
        for match in pattern.findall(line):
            token = match.strip(" ,.;:()[]{}")
            lower = token.lower()
            if len(token) < 3 or lower in STOPWORDS:
                continue
            if token.isupper() and len(token) > 6:
                continue
            counts[token] += 1
    return [t for t, _ in counts.most_common(limit)]


def dedupe_keep_order(items: list[str]) -> list[str]:
    out: list[str] = []
    seen: set[str] = set()
    for item in items:
        key = item.strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(item.strip())
    return out


def scan_latest_reports(
    session: requests.Session,
    ticker: str,
    filings: list[Filing],
    config: dict[str, Any],
) -> tuple[list[dict[str, Any]], str]:
    downloaded: list[dict[str, Any]] = []
    report_texts: list[str] = []
    allowed_forms = set(config.get("download_forms", DEFAULT_CONFIG["download_forms"]))
    for filing in filings:
        if filing.form not in allowed_forms:
            continue
        target = reports_dir_for(ticker) / (
            f"{filing.filing_date}_{filing.form}_{filing.accession.replace('-', '')}_"
            f"{Path(filing.primary_document).name or 'filing.html'}"
        )
        if not target.exists():
            try:
                content = fetch_bytes(session, filing.filing_url)
                target.write_bytes(content)
            except Exception as exc:
                log(f"Failed to download {filing.filing_url}: {exc}")
                continue
        downloaded.append(
            {
                "form": filing.form,
                "filing_date": filing.filing_date,
                "accession": filing.accession,
                "url": filing.filing_url,
                "local_path": str(target),
            }
        )
        if len(downloaded) >= int(config.get("reports_per_company", 12)):
            break

    for report in downloaded[:3]:
        try:
            if Path(report["local_path"]).suffix.lower() in {".htm", ".html", ".txt", ""}:
                report_texts.append(
                    html_to_text(Path(report["local_path"]).read_text(encoding="utf-8", errors="ignore"))
                )
        except Exception:
            pass

    return downloaded, "\n\n".join(report_texts)


def get_index_items(session: requests.Session, filing: Filing) -> list[dict[str, Any]]:
    try:
        raw = fetch_json(session, filing.index_json_url)
        return raw.get("directory", {}).get("item", []) or []
    except Exception:
        return []


def collect_contract_exhibits(
    session: requests.Session,
    ticker: str,
    filings: list[Filing],
    config: dict[str, Any],
) -> list[dict[str, Any]]:
    exhibits: list[dict[str, Any]] = []
    max_items = int(config.get("max_contract_exhibits", 15))
    for filing in filings[:30]:
        if filing.form not in {"8-K", "10-K", "10-Q", "20-F", "6-K"}:
            continue
        for item in get_index_items(session, filing):
            name = str(item.get("name", ""))
            lowered = name.lower()
            if not any(hint in lowered for hint in EXHIBIT_HINTS):
                continue
            url = f"{filing.base_url}/{name}"
            exhibits.append(
                {
                    "ticker": ticker,
                    "filing_date": filing.filing_date,
                    "form": filing.form,
                    "title": name,
                    "name": name,
                    "url": url,
                    "accession": filing.accession,
                }
            )
            if len(exhibits) >= max_items:
                return exhibits
    return exhibits


def extract_public_investments(text: str) -> dict[str, Any]:
    snippets = top_sentences(text, INVESTMENT_KEYWORDS, limit=10)
    entities = extract_capitalized_terms(snippets, limit=12)
    return {"snippets": snippets, "entities": entities}


def extract_pipeline(text: str) -> dict[str, Any]:
    snippets = top_sentences(text, PIPELINE_KEYWORDS, limit=10)
    terms = extract_capitalized_terms(snippets, limit=12)
    return {"snippets": snippets, "terms": terms}


def extract_contract_signals(text: str, exhibits: list[dict[str, Any]]) -> dict[str, Any]:
    snippets = top_sentences(text, CONTRACT_KEYWORDS, limit=10)
    exhibit_titles = dedupe_keep_order([ex.get("title", "") for ex in exhibits])[:12]
    counterparties = extract_capitalized_terms(snippets + exhibit_titles, limit=12)
    return {"snippets": snippets, "exhibit_titles": exhibit_titles, "counterparties": counterparties}


def usaspending_recipient_lookup(session: requests.Session, company_name: str) -> list[dict[str, Any]]:
    url = f"{USASPENDING_BASE}/api/v2/autocomplete/recipient/"
    payload = {"search_text": company_name}
    try:
        raw = fetch_json(session, url, method="POST", payload=payload)
    except Exception:
        return []
    results = raw.get("results") if isinstance(raw, dict) else raw
    return results or []


def public_contracts_best_effort(session: requests.Session, company_name: str) -> list[dict[str, Any]]:
    recipients = usaspending_recipient_lookup(session, company_name)
    return recipients[:5]


def rss_items(session: requests.Session, query: str, limit: int) -> list[dict[str, str]]:
    url = NEWS_RSS.format(query=quote_plus(query))
    content = fetch_bytes(session, url)
    root = ET.fromstring(content)
    items: list[dict[str, str]] = []
    for item in root.findall(".//item")[:limit]:
        def text_of(tag: str) -> str:
            elem = item.find(tag)
            return elem.text.strip() if elem is not None and elem.text else ""

        items.append(
            {
                "title": text_of("title"),
                "link": text_of("link"),
                "pubDate": text_of("pubDate"),
                "description": html_to_text(text_of("description")),
                "source": text_of("source"),
                "query": query,
            }
        )
    return items


def news_queries(company: str, ticker: str, snapshot: dict[str, Any], config: dict[str, Any]) -> list[str]:
    n = int(config.get("news_terms_per_bucket", 5))
    queries = [f'"{company}" OR {ticker}']
    pipeline_terms = snapshot.get("pipeline", {}).get("terms", [])[:n]
    investment_terms = snapshot.get("investments", {}).get("entities", [])[:n]
    counterparties = snapshot.get("contracts", {}).get("counterparties", [])[:n]
    for term in pipeline_terms:
        queries.append(f'(\"{company}\" OR {ticker}) "{term}"')
    for term in investment_terms:
        queries.append(f'(\"{company}\" OR {ticker}) "{term}"')
    for term in counterparties:
        queries.append(f'(\"{company}\" OR {ticker}) "{term}"')
    return dedupe_keep_order(queries)[: int(config.get("news_queries_per_company", 8))]


def item_id(item: dict[str, Any]) -> str:
    src = f"{item.get('title', '')}|{item.get('link', '')}|{item.get('pubDate', '')}"
    return hashlib.sha256(src.encode("utf-8", errors="ignore")).hexdigest()


def normalize_story_title(title: str) -> str:
    title = title.split(" - ")[0]
    title = title.lower()
    title = re.sub(r"[^a-z0-9\s]", " ", title)
    title = re.sub(r"\s+", " ", title).strip()
    return title


def story_cluster_key(item: dict[str, Any]) -> str:
    normalized = normalize_story_title(item.get("title", ""))
    return hashlib.sha256(normalized.encode("utf-8", errors="ignore")).hexdigest()


def parse_date(value: str) -> str:
    if not value:
        return ""
    fmts = [
        "%a, %d %b %Y %H:%M:%S %Z",
        "%Y-%m-%d",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%S.%f%z",
    ]
    for fmt in fmts:
        try:
            parsed = datetime.strptime(value, fmt)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc).isoformat()
        except Exception:
            continue
    return value


def classify_severity(score: int, blob: str) -> str:
    if score >= 12 or any(word in blob for word in SEVERITY_WORDS["critical"]):
        return "critical"
    if score >= 8 or any(word in blob for word in SEVERITY_WORDS["high"]):
        return "high"
    if score >= 5 or any(word in blob for word in SEVERITY_WORDS["medium"]):
        return "medium"
    return "low"


def evaluate_news_item(item: dict[str, Any], snapshot: dict[str, Any], company: str, ticker: str) -> dict[str, Any]:
    blob = " ".join(
        [
            item.get("title", ""),
            item.get("description", ""),
            item.get("query", ""),
        ]
    ).lower()
    reasons: list[str] = []
    score = 0

    for token in [company.lower(), ticker.lower()]:
        if token and token in blob:
            score += 3
            reasons.append(f"match: {token}")

    for word in snapshot.get("pipeline", {}).get("terms", [])[:8]:
        if word.lower() in blob:
            score += 2
            reasons.append(f"pipeline: {word}")
    for word in snapshot.get("investments", {}).get("entities", [])[:8]:
        if word.lower() in blob:
            score += 2
            reasons.append(f"investment: {word}")
    for word in snapshot.get("contracts", {}).get("counterparties", [])[:8]:
        if word.lower() in blob:
            score += 2
            reasons.append(f"contract: {word}")

    for word in SEVERITY_WORDS["critical"] + SEVERITY_WORDS["high"] + SEVERITY_WORDS["medium"]:
        if word in blob:
            score += 1
            reasons.append(f"keyword: {word}")

    severity = classify_severity(score, blob)
    host = urlparse(item.get("link", "")).netloc
    return {
        **item,
        "id": item_id(item),
        "cluster_id": story_cluster_key(item),
        "score": score,
        "severity": severity,
        "reasons": dedupe_keep_order(reasons)[:10],
        "published_utc": parse_date(item.get("pubDate", "")),
        "host": host,
    }


def compact_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False)


# ---------- state ----------

def load_state() -> dict[str, Any]:
    return load_json(STATE_PATH, {"seen_news": {}, "seen_news_clusters": {}, "seen_filings": {}})


def save_state(state: dict[str, Any]) -> None:
    save_json(STATE_PATH, state)


def seen_cluster_recently(seen_clusters: dict[str, str], cluster_id: str, window_days: int) -> bool:
    raw = seen_clusters.get(cluster_id, "")
    if not raw:
        return False
    try:
        dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt >= now_utc() - timedelta(days=window_days)
    except Exception:
        return False


# ---------- core work ----------

def summarize_company(
    session: requests.Session,
    ticker: str,
    ticker_map: dict[str, dict[str, str]],
    config: dict[str, Any],
) -> dict[str, Any]:
    if ticker not in ticker_map:
        raise ValueError(f"Ticker not found in SEC map: {ticker}")

    meta = ticker_map[ticker]
    cik = meta["cik"]
    company = meta["title"]
    submissions = get_submissions(session, cik)
    filings = filings_from_submissions(submissions)

    reports, text_blob = scan_latest_reports(session, ticker, filings, config)
    contract_exhibits = collect_contract_exhibits(session, ticker, filings, config)
    pipeline = extract_pipeline(text_blob)
    investments = extract_public_investments(text_blob)
    contracts = extract_contract_signals(text_blob, contract_exhibits)
    public_awards = public_contracts_best_effort(session, company)

    snapshot = {
        "ticker": ticker,
        "company": company,
        "cik": cik,
        "generated_at_utc": utc_iso(),
        "pipeline": pipeline,
        "investments": investments,
        "contracts": contracts,
        "federal_recipient_hits": public_awards,
        "reports": reports,
        "recent_filings": [
            {
                "accession": f.accession,
                "form": f.form,
                "filing_date": f.filing_date,
                "url": f.filing_url,
                "primary_document": f.primary_document,
                "description": f.primary_description,
            }
            for f in filings[:25]
        ],
        "contract_exhibits": contract_exhibits,
    }
    ticker_path = company_dir(ticker)
    save_json(ticker_path / "latest_snapshot.json", snapshot)
    if config.get("save_timestamped_snapshots", True):
        stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        save_json(ticker_path / f"snapshot_{stamp}.json", snapshot)
    if config.get("summary_enabled", True):
        record_snapshot_summary(snapshot)
    return snapshot


def notify_new_filings(snapshot: dict[str, Any], state: dict[str, Any]) -> None:
    ticker = snapshot["ticker"]
    seen = set(state.setdefault("seen_filings", {}).setdefault(ticker, []))
    for filing in snapshot.get("recent_filings", [])[:10]:
        accession = filing.get("accession", "")
        if not accession or accession in seen:
            continue
        title = f"{ticker}: new {filing.get('form', '')}"
        body = filing.get("description", "") or filing.get("primary_document", "") or filing.get("url", "")
        link = filing.get("url", "")
        send_notification(title, body, link)
        append_notification_history(
            "filing",
            ticker,
            title,
            body,
            {"accession": accession, "form": filing.get("form", ""), "link": link, "filing_date": filing.get("filing_date", "")},
        )
        append_event(
            "filing",
            ticker,
            title,
            body,
            link=link,
            severity="high",
            score=10,
            payload=filing,
        )
        seen.add(accession)
    state["seen_filings"][ticker] = list(seen)[-1000:]


def notify_news_for_company(snapshot: dict[str, Any], config: dict[str, Any], state: dict[str, Any]) -> None:
    ticker = snapshot["ticker"]
    company = snapshot["company"]
    seen_items = set(state.setdefault("seen_news", {}).setdefault(ticker, []))
    seen_clusters = state.setdefault("seen_news_clusters", {}).setdefault(ticker, {})
    all_items: list[dict[str, Any]] = []
    for query in news_queries(company, ticker, snapshot, config):
        try:
            items = rss_items(get_session(config["user_agent"], config), query, limit=int(config.get("news_per_query", 8)))
            all_items.extend(items)
            time.sleep(1.5)
        except Exception as exc:
            log(f"News query failed for {ticker}: {query} | {exc}")

    scored: list[dict[str, Any]] = []
    for item in all_items:
        evaluated = evaluate_news_item(item, snapshot, company, ticker)
        if evaluated["id"] in seen_items:
            continue
        if seen_cluster_recently(seen_clusters, evaluated["cluster_id"], int(config.get("news_dedupe_window_days", 7))):
            continue
        if int(evaluated["score"]) >= int(config.get("news_relevance_threshold", 5)):
            scored.append(evaluated)

    scored.sort(key=lambda row: (-int(row.get("score", 0)), row.get("published_utc", "")), reverse=False)
    scored = sorted(scored, key=lambda row: (int(row.get("score", 0)), row.get("published_utc", "")), reverse=True)

    for row in scored[: int(config.get("news_notification_limit", 5))]:
        title = f"{ticker}: {row.get('title', '')[:72]}"
        body = row.get("description") or row.get("query", "")
        link = row.get("link", "")
        send_notification(title, body, link)
        append_notification_history(
            "news",
            ticker,
            title,
            body,
            {
                "score": row.get("score", 0),
                "severity": row.get("severity", ""),
                "company": company,
                "link": link,
                "pubDate": row.get("pubDate", ""),
                "source": row.get("source", ""),
                "query": row.get("query", ""),
                "raw_title": row.get("title", ""),
                "reasons": row.get("reasons", []),
            },
        )
        append_event(
            "news",
            ticker,
            title,
            body,
            link=link,
            severity=row.get("severity", ""),
            score=int(row.get("score", 0)),
            payload=row,
        )
        seen_items.add(row["id"])
        seen_clusters[row["cluster_id"]] = utc_iso()
        news_log = company_dir(ticker) / "news_log.jsonl"
        with news_log.open("a", encoding="utf-8") as fh:
            fh.write(json.dumps(row, ensure_ascii=False) + "\n")
        time.sleep(1)

    state["seen_news"][ticker] = list(seen_items)[-1000:]
    state["seen_news_clusters"][ticker] = seen_clusters


def run_once(config: dict[str, Any]) -> None:
    ensure_dirs()
    prune_old_data(config)
    state = load_state()
    session = get_session(config["user_agent"], config)
    ticker_map = get_ticker_map(session)
    update_health(
        status="running",
        status_detail="Running one scan",
        last_loop_started_utc=utc_iso(),
        tickers=config.get("tickers", []),
    )
    had_success = False
    for ticker in config["tickers"]:
        update_health(current_ticker=ticker)
        try:
            log(f"Scanning {ticker}...")
            snapshot = summarize_company(session, ticker, ticker_map, config)
            notify_new_filings(snapshot, state)
            notify_news_for_company(snapshot, config, state)
            had_success = True
        except Exception as exc:
            log(f"{ticker} failed: {exc}")
            update_health(
                status="error",
                status_detail=f"Ticker failed: {ticker}",
                last_error_utc=utc_iso(),
                last_error=str(exc),
                consecutive_failures=int(load_health().get("consecutive_failures", 0)) + 1,
            )
    save_state(state)
    update_health(
        status="idle" if had_success else load_health().get("status", "idle"),
        status_detail="Run completed" if had_success else load_health().get("status_detail", ""),
        last_loop_completed_utc=utc_iso(),
        last_success_utc=utc_iso() if had_success else load_health().get("last_success_utc", ""),
        consecutive_failures=0 if had_success else load_health().get("consecutive_failures", 0),
        current_ticker="",
    )


def single_instance_lock() -> socket.socket:
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        sock.bind(("127.0.0.1", LOCK_PORT))
    except OSError:
        raise SystemExit(f"{APP_NAME} already appears to be running.")
    return sock


def main_loop(config: dict[str, Any]) -> None:
    lock = single_instance_lock()
    _ = lock
    update_health(
        status="running",
        status_detail="Watcher started",
        pid=os.getpid(),
        started_at_utc=utc_iso(),
        tickers=config.get("tickers", []),
        consecutive_failures=0,
    )
    log(f"{APP_NAME} started for: {', '.join(config['tickers'])}")
    while True:
        started = time.time()
        update_health(status="running", status_detail="Scanning", last_loop_started_utc=utc_iso())
        try:
            run_once(config)
        except Exception as exc:
            log(f"Top-level loop error: {exc}")
            update_health(
                status="error",
                status_detail="Top-level loop error",
                last_error_utc=utc_iso(),
                last_error=str(exc),
                consecutive_failures=int(load_health().get("consecutive_failures", 0)) + 1,
            )
        elapsed = time.time() - started
        sleep_for = max(60, int(config["poll_minutes"] * 60 - elapsed))
        update_health(status="sleeping", status_detail="Waiting for next poll", last_sleep_seconds=sleep_for, pid=os.getpid())
        log(f"Sleeping {sleep_for} seconds")
        time.sleep(sleep_for)


def print_summary(config: dict[str, Any]) -> None:
    session = get_session(config["user_agent"], config)
    ticker_map = get_ticker_map(session)
    for ticker in config["tickers"]:
        snapshot_path = company_dir(ticker) / "latest_snapshot.json"
        if not snapshot_path.exists():
            snapshot = summarize_company(session, ticker, ticker_map, config)
        else:
            snapshot = load_json(snapshot_path, {})
        print(f"\n=== {ticker} | {snapshot.get('company', '')} ===")
        print("Pipeline:")
        for line in snapshot.get("pipeline", {}).get("snippets", [])[:5]:
            print(f" - {line}")
        print("Investments:")
        for line in snapshot.get("investments", {}).get("snippets", [])[:5]:
            print(f" - {line}")
        print("Contracts:")
        for line in snapshot.get("contracts", {}).get("exhibit_titles", [])[:5]:
            print(f" - {line}")
        summary = latest_summary_text(ticker)
        if summary:
            print("\nDaily summary:")
            print(summary)


# ---------- CLI ----------

def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=APP_NAME)
    p.add_argument("--configure", action="store_true", help="Prompt for tickers and settings.")
    p.add_argument("--install-task", action="store_true", help="Install a Windows scheduled task to run at logon.")
    p.add_argument("--once", action="store_true", help="Run one scan and exit.")
    p.add_argument("--summary", action="store_true", help="Print the latest local summary.")
    p.add_argument("--run", action="store_true", help="Run the continuous watcher.")
    p.add_argument("--health", action="store_true", help="Print current health JSON.")
    p.add_argument("--stop", action="store_true", help="Stop a running watcher process if one is known.")
    return p.parse_args()


def main() -> None:
    ensure_dirs()
    args = parse_args()
    if args.configure:
        configure(force=True)
        return
    if args.install_task:
        install_startup_task()
        return
    if args.health:
        print(json.dumps(load_health(), indent=2, ensure_ascii=False))
        return
    if args.stop:
        stopped = stop_running_watcher()
        print("Stopped" if stopped else "No running watcher found")
        return

    config = configure(force=False)

    if args.once:
        run_once(config)
        return
    if args.summary:
        print_summary(config)
        return

    main_loop(config)


if __name__ == "__main__":
    main()
