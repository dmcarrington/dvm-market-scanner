#!/usr/bin/env python3
"""
DVM Market Scanner — Enhanced 5300 query fetcher.
Fetches full NIP-90 kind 5300 events from relays to extract question text.
Stores parsed questions in SQLite for category analysis.
"""

import asyncio
import json
import sqlite3
import time
from collections import Counter, defaultdict
from pathlib import Path
import websockets

DB_PATH = Path(__file__).parent / "data" / "scanner.db"
RELAYS = [
    "wss://relay.damus.io",
    "wss://relay.nostr.net",
    "wss://nos.lol",
    "wss://relay.primal.net",
]

# ─── Categories ────────────────────────────────────────────────────────────────

TOPIC_KEYWORDS = {
    "Geopolitics":      ["trump", "iran", "qatar", "china", "us ", "uk", "eu ", "russia", "sanctions", "nato", "war", "opec", "brent crude", "oil shock", "hormuz", "lng", "ras laffan"],
    "Crypto/Bitcoin":   ["bitcoin", "btc", "nostr", "maxi madness", "mining pool", "foundry", "satoshi", "ordinal", "lightning"],
    "AI/Tech":          ["openai", "gpt", "claude", "model", "release", "update", "primal", "nip", "github", "oracle"],
    "Sports":           ["longhorns", "march madness", "elite 8", "f1", "gp", "racing", "tournament", "bracket"],
    "Markets":          ["gold", "spike", "market", "barrel", "crude", "stock", "etf", "pound", "inflation"],
    "Entertainment":   ["demon slayer", "anime", "episode", "movie", "series", "poll"],
}

def categorize(text):
    text_lower = text.lower()
    matches = []
    for topic, keywords in TOPIC_KEYWORDS.items():
        if any(kw.lower() in text_lower for kw in keywords):
            matches.append(topic)
    return matches if matches else ["Other"]


# ─── Fetch events from relays ───────────────────────────────────────────────────

async def fetch_event_batch(event_ids, timeout_sec=8):
    """Fetch a batch of event IDs from relays concurrently."""

    async def fetch_from_relay(relay, event_ids):
        results = {}
        sub_id = "fetch-batch"
        try:
            async with websockets.connect(relay, open_timeout=timeout_sec, close_timeout=3) as ws:
                req = ["REQ", sub_id, {"ids": event_ids}]
                await ws.send(json.dumps(req))

                deadline = time.time() + timeout_sec - 1
                while time.time() < deadline:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=deadline - time.time())
                        msg = json.loads(msg)
                        if msg[0] == "EVENT" and msg[2]["id"] in event_ids:
                            results[msg[2]["id"]] = msg[2]
                    except asyncio.TimeoutError:
                        break
                # CLOSE
                await ws.send(json.dumps(["CLOSE", sub_id]))
                await ws.close()
        except Exception as e:
            pass  # Relay failed, try next
        return relay, results

    # Run all relays concurrently, first to respond wins
    tasks = [asyncio.create_task(fetch_from_relay(r, event_ids)) for r in RELAYS]
    done, pending = await asyncio.wait(tasks, timeout=timeout_sec)
    for t in pending:
        t.cancel()
    try:
        await asyncio.gather(*pending, return_exceptions=True)
    except Exception:
        pass

    # Merge results
    merged = {}
    for task in done:
        try:
            _, results = task.result()
            for eid, evt in results.items():
                if eid not in merged:
                    merged[eid] = evt
        except Exception:
            pass
    return merged


async def fetch_missing():
    """Fetch full content for 5300 events that don't have it stored."""
    conn = sqlite3.connect(DB_PATH)

    # Find 5300 events with no parsed question stored
    rows = conn.execute(
        "SELECT id FROM events WHERE kind=5300 AND id NOT IN (SELECT id FROM events WHERE content != '')"
    ).fetchall()

    # Also find already-stored events with empty content
    rows += conn.execute(
        "SELECT id FROM events WHERE kind=5300 AND content = ''"
    ).fetchall()

    event_ids = list({r[0] for r in rows})
    conn.close()

    if not event_ids:
        print(f"No 5300 events to fetch ({len(rows)} already processed)")
        return

    print(f"Fetching {len(event_ids)} 5300 events from relays...")
    fetched = await fetch_event_batch(event_ids)
    print(f"Got {len(fetched)} events back from relays")

    if not fetched:
        print("No events found on relays. They may have expired.")
        return

    conn = sqlite3.connect(DB_PATH)

    for eid, evt in fetched.items():
        content = evt.get("content", "") or ""
        tags = evt.get("tags", [])

        # Extract prompt from content or i-tag
        prompt = content
        if not prompt:
            # Try to get from ["i", "<text>", ...] tag
            for tag in tags:
                if len(tag) >= 2 and tag[0] == "i":
                    prompt = tag[1]
                    break

        # Also extract other params
        params = {}
        for tag in tags:
            if len(tag) >= 2 and tag[0] in ("model", "format", "size", "noise"):
                params[tag[0]] = tag[1]

        # Store back in DB: update content with the prompt
        conn.execute(
            "UPDATE events SET content = ? WHERE id = ?",
            (prompt, eid)
        )

    conn.commit()
    conn.close()
    print(f"Stored {len(fetched)} question texts in DB")


# ─── Analytics ────────────────────────────────────────────────────────────────

def analyze():
    """
    Analyze questions from kinds 6969 (polls) and any other kinds with actual content.
    Kind 5300 events have empty content on relays — the real questions live in 6969 poll responses.
    """
    conn = sqlite3.connect(DB_PATH)

    # Collect all event kinds that have actual content
    rows = conn.execute(
        """SELECT kind, content, pubkey, created_at FROM events
           WHERE LENGTH(content) > 10
           ORDER BY created_at DESC"""
    ).fetchall()

    if not rows:
        print("No events with content found.")
        conn.close()
        return

    print(f"\n{'='*60}")
    print(f"ANALYSIS: {len(rows)} events with content")
    print(f"{'='*60}\n")

    # Per-kind breakdown
    from collections import Counter
    kind_counts = Counter(r[0] for r in rows)
    print("BY EVENT KIND:")
    for kind, count in kind_counts.most_common():
        name = {
            6969: "polls (NIP-96B)",
            6300: "DVM results (NIP-90)",
            5100: "image gen (NIP-90)",
            6472: "anon polls (NIP-96B)",
        }.get(kind, f"kind-{kind}")
        print(f"  {kind} ({name}): {count}")

    print()

    # Analyze polls (kind 6969)
    poll_rows = [(r[1], r[3]) for r in rows if r[0] == 6969]
    if poll_rows:
        poll_questions = [r[0] for r in poll_rows]
        print(f"POLL QUESTIONS ({len(poll_questions)}):")
        cat_counts = Counter()
        for q in poll_questions:
            for cat in categorize(q):
                cat_counts[cat] += 1
        for cat, count in cat_counts.most_common():
            pct = count / len(poll_questions) * 100
            print(f"  {cat:20s} {count:4d} ({pct:5.1f}%)")
        print()
        print("SAMPLE POLL QUESTIONS:")
        for q in poll_questions[:8]:
            print(f"  • {q[:150].strip()}")
        print()

    # Other content types
    other_rows = [(r[0], r[1], r[2]) for r in rows if r[0] not in (6969,)]
    if other_rows:
        print("OTHER CONTENT:")
        shown = 0
        for kind, content, pubkey in other_rows:
            if shown >= 5:
                break
            print(f"  [{kind}] {content[:150].strip()}")
            shown += 1

    conn.close()


def full_report():
    """Generate enriched stats for Gist sync."""
    conn = sqlite3.connect(DB_PATH)
    rows = conn.execute(
        """SELECT kind, content FROM events
           WHERE LENGTH(content) > 10
           ORDER BY created_at DESC LIMIT 100"""
    ).fetchall()
    conn.close()

    poll_rows = [r[1] for r in rows if r[0] == 6969]
    poll_cat = Counter(cat for q in poll_rows for cat in categorize(q))

    return {
        "total_with_content": len(rows),
        "poll_questions": len(poll_rows),
        "poll_categories": dict(poll_cat.most_common()),
        "recent_polls": [q.strip()[:200] for q in poll_rows[:10]],
    }


if __name__ == "__main__":
    import sys
    if len(sys.argv) > 1 and sys.argv[1] == "--fetch":
        asyncio.run(fetch_missing())
    elif len(sys.argv) > 1 and sys.argv[1] == "--analyze":
        analyze()
    else:
        print("Usage:")
        print("  python3 fetch_questions.py --fetch   # Fetch missing 5300 event content")
        print("  python3 fetch_questions.py --analyze # Show category analysis")
