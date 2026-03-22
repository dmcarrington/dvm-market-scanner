# DVM Market Scanner

## Purpose
Scan Nostr relays for NIP-90 DVM activity, aggregate usage stats, and expose via a simple dashboard.

## Architecture

### Scanner (`scanner.py`)
- Python daemon using `nostr_sdk`
- Connects to major relays
- Subscribes to NIP-90 job events (kinds 5000-5999) and result events (kinds 6000-6999)
- Writes raw events to SQLite `data/scanner.db`
- Runs continuously, aggregates on demand

### API (`api.py`)
- Python HTTP server (no framework, stdlib)
- Exposes aggregated stats as JSON
- `/stats` — overview (total events, unique DVMs, by kind)
- `/timeline` — events per hour/day
- `/dvms` — ranked list of DVMs by activity

### Dashboard (`dashboard/`)
- Static HTML + CSS + Chart.js
- Fetches from API, renders charts and tables
- Time range selector (1h, 24h, 7d, 30d)

## Stats to track
- Events per kind (text gen, translate, summarize, image, audio, video, etc.)
- Events per DVM (by pubkey)
- Unique DVMs seen
- Zap amounts (from result events)
- Relay coverage

## Kind reference (NIP-90)
- 5000: Extract text
- 5001: Summarize text
- 5002: Translate text
- 5050: Generate text (LLM)
- 5100: Generate image
- 5200: Convert video
- 5202: Generate video
- 5250: Text to speech
- 5251: Generate music
- 5300: Content discovery
