#!/usr/bin/env python3
"""
Sync DVM market stats to a GitHub Gist.
Creates the Gist on first run, updates it on subsequent runs.
Dashboard reads from the Gist's raw URL — no tunnel, no CORS.
"""

import json
import sqlite3
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path

DB_PATH = Path(__file__).parent / "data" / "scanner.db"
GIST_ID_FILE = Path.home() / ".openclaw" / "secrets" / "dvm-market-gist-id"
GIST_FILENAME = "stats.json"

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


def get_stats(conn, since_hours=24):
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
        "SELECT kind, COUNT(*) AS c FROM events WHERE created_at > ? GROUP BY kind ORDER BY c DESC", (cutoff,)
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
    }


def get_timeline(conn, since_hours=24, bucket_minutes=60):
    cutoff = int(time.time()) - (since_hours * 3600)
    bucket_sec = bucket_minutes * 60
    rows = conn.execute(
        f"""SELECT (created_at / {bucket_sec}) * {bucket_sec} AS bucket, kind, COUNT(*) AS c
            FROM events WHERE created_at > ? GROUP BY bucket, kind ORDER BY bucket""",
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
    return [{"pubkey": r[0], "events": r[1], "jobs": r[2], "results": r[3]} for r in rows]


def create_gist(content, filename):
    result = subprocess.run(
        ["gh", "gist", "create", "--public", "-d", "DVM Market Scanner live stats",
         "-f", filename],
        input=content, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"gist create failed: {result.stderr}")
    url = result.stdout.strip()
    return url.rsplit("/", 1)[-1]


def update_gist(gist_id, content, filename):
    result = subprocess.run(
        ["gh", "gist", "edit", gist_id, "-f", filename],
        input=content, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"gist edit failed: {result.stderr}")


def sync():
    if not DB_PATH.exists():
        print("ERROR: scanner.db not found. Run scanner.py first.")
        return

    conn = sqlite3.connect(DB_PATH)
    payload = {
        "stats": get_stats(conn),
        "timeline": get_timeline(conn),
        "dvms": get_top_dvms(conn),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }
    conn.close()

    content = json.dumps(payload, indent=2)
    gist_id = GIST_ID_FILE.read_text().strip() if GIST_ID_FILE.exists() else None

    if gist_id:
        print(f"Updating Gist {gist_id}...")
        update_gist(gist_id, content, GIST_FILENAME)
    else:
        print("Creating new Gist...")
        gist_id = create_gist(content, GIST_FILENAME)
        GIST_ID_FILE.write_text(gist_id)
        print(f"Gist created: https://gist.github.com/{gist_id}")

    print(f"Synced at {payload['synced_at']}")
    print(f"DVMs: {payload['stats']['unique_dvms']} | "
          f"Events: {payload['stats']['total_events']} | "
          f"Jobs: {payload['stats']['job_events']}")


if __name__ == "__main__":
    sync()
