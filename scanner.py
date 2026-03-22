#!/usr/bin/env python3
"""
DVM Market Scanner — Pure Python WebSocket relay client for NIP-90 market data.
"""

import asyncio
import json
import os
import sqlite3
import time
import re
from datetime import datetime, timezone
from pathlib import Path
import websockets

DB_PATH = Path(__file__).parent / "data" / "scanner.db"
LOG_INTERVAL = 60

JOB_KINDS = list(range(5000, 6000))
RESULT_KINDS = list(range(6000, 7000))

RELAYS = [
    "wss://relay.damus.io",
    "wss://relay.nostr.net",
    "wss://nos.lol",
    "wss://relay.primal.net",
]

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

# ─── Database ──────────────────────────────────────────────────────────────────

def init_db():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS events (
            id TEXT PRIMARY KEY,
            pubkey TEXT NOT NULL,
            kind INTEGER NOT NULL,
            content TEXT,
            created_at INTEGER NOT NULL,
            relay TEXT,
            collected_at INTEGER NOT NULL
        )
    """)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS stats_log (
            ts INTEGER PRIMARY KEY,
            total_events INTEGER,
            job_events INTEGER,
            result_events INTEGER,
            unique_pubkeys INTEGER,
            by_kind TEXT
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_kind ON events(kind)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_pubkey ON events (pubkey)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_created ON events(created_at)")
    conn.commit()
    return conn


def insert_event(conn, event_id, pubkey, kind, content, created_at, relay):
    try:
        conn.execute(
            "INSERT OR IGNORE INTO events (id, pubkey, kind, content, created_at, relay, collected_at) VALUES (?, ?, ?, ?, ?, ?, ?)",
            (event_id, pubkey, kind, content, created_at, relay, int(time.time()))
        )
    except Exception:
        pass


def log_stats(conn):
    now = int(time.time())
    row = conn.execute("SELECT COUNT(*), COUNT(DISTINCT pubkey) FROM events").fetchone()
    total, unique = row[0] or 0, row[1] or 0

    by_kind = {}
    for row in conn.execute("SELECT kind, COUNT(*) FROM events GROUP BY kind ORDER BY COUNT(*) DESC"):
        kname = TASK_NAMES.get(row[0], f"kind-{row[0]}")
        by_kind[kname] = row[1]

    conn.execute(
        "INSERT INTO stats_log (ts, total_events, job_events, result_events, unique_pubkeys, by_kind) VALUES (?, ?, ?, ?, ?, ?)",
        (now, total,
         sum(v for k, v in by_kind.items() if any(str(k2) in k for k2 in range(5000, 6000))),
         sum(v for k, v in by_kind.items() if any(str(k2) in k for k2 in range(6000, 7000))),
         unique, json.dumps(by_kind))
    )
    conn.commit()
    print(f"[{datetime.now().strftime('%H:%M:%S')}] "
          f"Total: {total} | Jobs: {by_kind.get('kind-5',0)+by_kind.get('kind-5',0)} | "
          f"DVMs: {unique} | Top: {list(by_kind.items())[:3]}")


# ─── Relay Client ─────────────────────────────────────────────────────────────

async def collect_from_relay(conn, relay, stop_event, kinds):
    """Connect to one relay and collect NIP-90 events."""
    sub_id = "dvm-scan-1"
    seen = set()

    # Build NIP-90 REQ
    req = [
        "REQ", sub_id,
        {"kinds": kinds}
    ]

    async with websockets.connect(relay, ping_interval=None) as ws:
        await ws.send(json.dumps(req))
        print(f"  [{relay}] Connected, listening...")

        while not stop_event.is_set():
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=5.0)
                await handle_message(conn, json.loads(msg), relay, sub_id, seen)
            except asyncio.TimeoutError:
                continue
            except Exception as e:
                print(f"  [{relay}] Error: {e}")
                break

    print(f"  [{relay}] Disconnected")


async def handle_message(conn, msg, relay, sub_id, seen):
    if not isinstance(msg, list) or msg[0] != "EVENT":
        return

    event = msg[2]
    if not isinstance(event, dict):
        return

    event_id = event.get("id", "")
    pubkey = event.get("pubkey", "")
    kind = event.get("kind", 0)

    key = (event_id, pubkey, kind)
    if key in seen:
        return
    seen.add(key)

    insert_event(conn, event_id, pubkey, kind,
                 event.get("content", ""),
                 event.get("created_at", 0),
                 relay)


# ─── Main ─────────────────────────────────────────────────────────────────────

async def scan_relays():
    conn = init_db()
    stop_event = asyncio.Event()
    all_kinds = JOB_KINDS + RESULT_KINDS

    print("DVM Market Scanner")
    print(f"Relays: {RELAYS}")
    print(f"Kinds: {all_kinds[0]}–{all_kinds[-1]} (jobs + results)\n")

    # Start collector tasks
    tasks = [
        asyncio.create_task(collect_from_relay(conn, relay, stop_event, all_kinds))
        for relay in RELAYS
    ]

    # Stats logging
    last_stats = time.time()
    try:
        while True:
            await asyncio.sleep(5)
            conn.commit()
            if time.time() - last_stats >= LOG_INTERVAL:
                log_stats(conn)
                last_stats = time.time()
    except asyncio.CancelledError:
        pass
    finally:
        stop_event.set()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        conn.commit()
        log_stats(conn)
        conn.close()
        print("Scanner stopped.")


if __name__ == "__main__":
    try:
        asyncio.run(scan_relays())
    except KeyboardInterrupt:
        print("\nInterrupted.")
