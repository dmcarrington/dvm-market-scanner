#!/usr/bin/env python3
"""
Sync DVM market stats + poll analysis to GitHub Gist.
Reads from scanner.db, builds enriched payload, pushes to Gist.
"""

import json
import sqlite3
import subprocess
import time
from collections import Counter
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

CATEGORIES = {
    "Geopolitics":   ["trump","iran","qatar","china","uk","eu ","russia","sanctions","nato","war","opec","brent","oil","hormuz","lng","ras laffan","saudi","opec"],
    "Crypto/Bitcoin": ["bitcoin","btc","nostr","maxi","mining","foundry","lightning","satoshi","ordinal","taproot"],
    "AI/Tech":        ["openai","gpt","claude","model","release","update","primal","nip","github","oracle","software","chatgpt"],
    "Sports":         ["longhorns","march madness","elite","f1","gp","racing","tournament","bracket","nba","nfl","texas","russell"],
    "Markets":        ["gold","spike","market","barrel","crude","stock","etf","inflation","funds","bitcoin price","oil shock"],
    "Entertainment":  ["anime","movie","series","poll","fun","demon slayer","episode","nostalgia"],
}

def categorize(text):
    t = text.lower()
    return [c for c, kws in CATEGORIES.items() if any(kw in t for kw in kws)] or ["Other"]


# ─── DB queries ────────────────────────────────────────────────────────────────

def get_stats(conn, since_hours=24):
    cutoff = int(time.time()) - (since_hours * 3600)
    total = conn.execute("SELECT COUNT(*) FROM events WHERE created_at > ?", (cutoff,)).fetchone()[0] or 0
    unique = conn.execute("SELECT COUNT(DISTINCT pubkey) FROM events WHERE created_at > ?", (cutoff,)).fetchone()[0] or 0
    jobs = conn.execute("SELECT COUNT(*) FROM events WHERE created_at > ? AND kind >= 5000 AND kind < 6000", (cutoff,)).fetchone()[0] or 0
    results = conn.execute("SELECT COUNT(*) FROM events WHERE created_at > ? AND kind >= 6000 AND kind < 7000", (cutoff,)).fetchone()[0] or 0
    by_kind = {}
    for kind, cnt in conn.execute(
        "SELECT kind, COUNT(*) AS c FROM events WHERE created_at > ? GROUP BY kind ORDER BY c DESC", (cutoff,)
    ).fetchall():
        by_kind[TASK_NAMES.get(kind, f"kind-{kind}")] = cnt
    return {
        "period_hours": since_hours,
        "total_events": total,
        "job_events": jobs,
        "result_events": results,
        "unique_dvms": unique,
        "by_kind": by_kind,
    }


def get_all_stats(conn):
    """Return stats for all time ranges used by the dashboard buttons."""
    return {
        "1":  get_stats(conn, 1),
        "24": get_stats(conn, 24),
        "168": get_stats(conn, 168),
        "720": get_stats(conn, 720),
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
        f"""SELECT pubkey, COUNT(*) AS total_events,
               SUM(CASE WHEN kind >= 5000 AND kind < 6000 THEN 1 ELSE 0 END) AS jobs,
               SUM(CASE WHEN kind >= 6000 AND kind < 7000 THEN 1 ELSE 0 END) AS results
            FROM events WHERE created_at > ?
            GROUP BY pubkey ORDER BY total_events DESC LIMIT ?""",
        (cutoff, limit)
    ).fetchall()
    return [{"pubkey": r[0], "events": r[1], "jobs": r[2], "results": r[3]} for r in rows]


def get_question_report(conn):
    """Extract question/poll content from events and categorize them."""
    rows = conn.execute(
        "SELECT kind, content FROM events WHERE LENGTH(content) > 10"
    ).fetchall()

    poll_rows = [r[1] for r in rows if r[0] == 6969]
    poll_cat = Counter(cat for q in poll_rows for cat in categorize(q))

    return {
        "total_with_content": len(rows),
        "poll_questions": len(poll_rows),
        "poll_categories": dict(poll_cat.most_common()),
        "recent_polls": [q.strip()[:250] for q in poll_rows[:10]],
    }


# ─── Gist operations ───────────────────────────────────────────────────────────

def gh_gist_create(content, filename):
    result = subprocess.run(
        ["gh", "gist", "create", "--public", "-d", "DVM Market Scanner live stats", "-f", filename],
        input=content, capture_output=True, text=True
    )
    if result.returncode != 0:
        raise Exception(f"gist create failed: {result.stderr}")
    return result.stdout.strip().rsplit("/", 1)[-1]


def gh_gist_edit(gist_id, content, filename):
    import tempfile
    with tempfile.NamedTemporaryFile(mode="w", suffix=filename, delete=False) as f:
        f.write(content)
        f.flush()
        tmp = f.name
    result = subprocess.run(
        ["gh", "gist", "edit", gist_id, tmp],
        capture_output=True, text=True
    )
    import os
    os.unlink(tmp)
    if result.returncode != 0:
        raise Exception(f"gist edit failed: {result.stderr}")


# ─── Main sync ─────────────────────────────────────────────────────────────────

def sync():
    if not DB_PATH.exists():
        print("ERROR: scanner.db not found. Run scanner.py first.")
        return

    conn = sqlite3.connect(DB_PATH)

    # Build stats for each dashboard range
    range_hours = {"1": 1, "24": 24, "168": 168, "720": 720}
    all_stats = {}
    all_timelines = {}
    all_dvms = {}
    for key, hours in range_hours.items():
        all_stats[key] = get_stats(conn, hours)
        all_timelines[key] = get_timeline(conn, hours)
        all_dvms[key] = get_top_dvms(conn, hours)

    payload = {
        "stats": all_stats,
        "timelines": all_timelines,
        "dvms": all_dvms,
        "questions": get_question_report(conn),
        "synced_at": datetime.now(timezone.utc).isoformat(),
    }
    conn.close()

    content = json.dumps(payload, indent=2)
    gist_id = GIST_ID_FILE.read_text().strip() if GIST_ID_FILE.exists() else None

    if gist_id:
        print(f"Updating Gist {gist_id}...")
        gh_gist_edit(gist_id, content, GIST_FILENAME)
    else:
        print("Creating new Gist...")
        gist_id = gh_gist_create(content, GIST_FILENAME)
        GIST_ID_FILE.write_text(gist_id)
        print(f"Gist created: https://gist.github.com/{gist_id}")

    print(f"Synced at {payload['synced_at']}")
    s = all_stats["24"]
    print(f"Stats (24h): {s['total_events']} events | {s['job_events']} jobs | "
          f"{payload['questions']['poll_questions']} polls")


if __name__ == "__main__":
    sync()
