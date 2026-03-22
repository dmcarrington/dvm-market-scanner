#!/usr/bin/env python3
"""
DVM Market Scanner API — Serves aggregated stats from the scanner DB.
"""

import json
import sqlite3
import time
from datetime import datetime, timezone
from pathlib import Path
from http.server import HTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs

DB_PATH = Path(__file__).parent / "data" / "scanner.db"
PORT = 8765

# Load auth secret from secrets file
SECRET_FILE = Path.home() / ".openclaw" / "secrets" / "dvm-api-auth"
_RAW_AUTH = SECRET_FILE.read_text().strip() if SECRET_FILE.exists() else ""
_AUTH_VALUE = _RAW_AUTH.split(":")[-1] if _RAW_AUTH else ""

TASK_NAMES = {
    5000: "extract-text",
    5001: "summarize-text",
    5002: "translate-text",
    5050: "generate-text",
    5100: "generate-image",
    5200: "convert-video",
    5202: "generate-video",
    5250: "text-to-speech",
    5251: "generate-music",
    5300: "content-discovery",
}


def get_db():
    return sqlite3.connect(DB_PATH)


def get_overview(conn, since_hours=24):
    cutoff = int(time.time()) - (since_hours * 3600)

    total = conn.execute(
        "SELECT COUNT(*) FROM events WHERE created_at > ?", (cutoff,)
    ).fetchone()[0] or 0

    unique_pubkeys = conn.execute(
        "SELECT COUNT(DISTINCT pubkey) FROM events WHERE created_at > ?", (cutoff,)
    ).fetchone()[0] or 0

    job_count = conn.execute(
        "SELECT COUNT(*) FROM events WHERE created_at > ? AND kind >= 5000 AND kind < 6000", (cutoff,)
    ).fetchone()[0] or 0

    result_count = conn.execute(
        "SELECT COUNT(*) FROM events WHERE created_at > ? AND kind >= 6000 AND kind < 7000", (cutoff,)
    ).fetchone()[0] or 0

    by_kind = {}
    for kind, cnt in conn.execute(
        "SELECT kind, COUNT(*) AS c FROM events WHERE created_at > ? GROUP BY kind ORDER BY c DESC",
        (cutoff,)
    ).fetchall():
        kname = TASK_NAMES.get(kind, f"kind-{kind}")
        by_kind[kname] = cnt

    return {
        "period_hours": since_hours,
        "total_events": total,
        "job_events": job_count,
        "result_events": result_count,
        "unique_dvms": unique_pubkeys,
        "by_kind": by_kind,
        "generated_at": datetime.utcnow().isoformat() + "Z",
    }


def get_timeline(conn, since_hours=24, bucket_minutes=60):
    cutoff = int(time.time()) - (since_hours * 3600)
    bucket_sec = bucket_minutes * 60

    rows = conn.execute(
        f"""SELECT (created_at / {bucket_sec}) * {bucket_sec} AS bucket, kind, COUNT(*) AS c
            FROM events WHERE created_at > ?
            GROUP BY bucket, kind ORDER BY bucket""",
        (cutoff,)
    ).fetchall()

    timeline = {}
    for bucket, kind, cnt in rows:
        ts = datetime.fromtimestamp(bucket, tz=timezone.utc).isoformat()
        kname = TASK_NAMES.get(kind, f"kind-{kind}")
        if ts not in timeline:
            timeline[ts] = {"ts": ts}
        timeline[ts][kname] = cnt
        timeline[ts].setdefault("total", 0)
        timeline[ts]["total"] += cnt

    return list(timeline.values())


def get_top_dvms(conn, since_hours=24, limit=20):
    cutoff = int(time.time()) - (since_hours * 3600)
    rows = conn.execute(
        f"""SELECT pubkey,
               COUNT(*) AS total_events,
               SUM(CASE WHEN kind >= 5000 AND kind < 6000 THEN 1 ELSE 0 END) AS jobs,
               SUM(CASE WHEN kind >= 6000 AND kind < 7000 THEN 1 ELSE 0 END) AS results
            FROM events WHERE created_at > ?
            GROUP BY pubkey ORDER BY total_events DESC LIMIT ?""",
        (cutoff, limit)
    ).fetchall()

    return [
        {"pubkey": r[0], "events": r[1], "jobs": r[2], "results": r[3]}
        for r in rows
    ]


# ─── HTTP Server ────────────────────────────────────────────────────────────────

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass

    def send_json(self, data, status=200):
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization")
        self.end_headers()
        self.wfile.write(json.dumps(data).encode())

    def do_OPTIONS(self):
        self.send_response(204)
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Authorization")
        self.end_headers()

    def do_GET(self):
        # Shared secret auth
        if _AUTH_VALUE and self.headers.get("Authorization") != _AUTH_VALUE:
            self.send_response(401)
            self.send_header("Content-Type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps({"error": "unauthorized"}).encode())
            return

        parsed = urlparse(self.path)
        qs = parse_qs(parsed.query)
        since = int(qs.get("since", ["24"])[0])
        since = min(max(since, 1), 720)

        conn = get_db()
        try:
            if parsed.path == "/stats":
                self.send_json(get_overview(conn, since))
            elif parsed.path == "/timeline":
                self.send_json(get_timeline(conn, since))
            elif parsed.path == "/dvms":
                self.send_json(get_top_dvms(conn, since))
            elif parsed.path in ("/", "/dashboard"):
                self.send_response(200)
                self.send_header("Content-Type", "text/html")
                self.end_headers()
                with open(Path(__file__).parent / "dashboard" / "index.html", "rb") as f:
                    self.wfile.write(f.read())
            else:
                self.send_json({"error": "not found"}, 404)
        finally:
            conn.close()


def run():
    if not DB_PATH.exists():
        print(f"ERROR: {DB_PATH} not found. Run scanner.py first.")
        return
    print(f"API running on http://localhost:{PORT}")
    print(f"  GET /stats      — overview stats")
    print(f"  GET /timeline   — events over time")
    print(f"  GET /dvms       — top DVMs by activity")
    print(f"  GET /dashboard  — HTML dashboard")
    HTTPServer(("0.0.0.0", PORT), Handler).serve_forever()


if __name__ == "__main__":
    run()
