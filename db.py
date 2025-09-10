# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import random
import sqlite3
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import aiosqlite

from config import CONFIG

log = logging.getLogger("token_tony.db")

# Serialize all SQLite writes to avoid 'database is locked' under concurrent tasks
DB_WRITE_LOCK = asyncio.Lock()

async def _execute_db(query: str, params: tuple = (), *, commit: bool = False, fetch: Optional[str] = None):
    """Execute a SQLite query with robust locking + retries.
    - Uses WAL + increased busy_timeout
    - Serializes writes behind a global async lock to prevent 'database is locked'
    """
    q_head = (query or "").lstrip().split(" ", 1)[0].upper()
    is_write = commit or q_head in {"INSERT", "UPDATE", "DELETE", "CREATE", "DROP", "VACUUM", "REINDEX", "ALTER", "REPLACE"}
    attempts = 5 if is_write else 3
    delay = 0.25

    async def _run_once():
        async with aiosqlite.connect(CONFIG["DB_FILE"], timeout=30.0) as con:
            # Pragmas tuned for concurrent single-writer load
            await con.execute("PRAGMA journal_mode=WAL;")
            await con.execute("PRAGMA synchronous=NORMAL;")
            await con.execute("PRAGMA busy_timeout=10000;")
            async with con.cursor() as cur:
                await cur.execute(query, params)
                if commit:
                    await con.commit()
                if fetch == 'one':
                    return await cur.fetchone()
                if fetch == 'all':
                    return await cur.fetchall()
                return None

    for attempt in range(1, attempts + 1):
        try:
            if is_write:
                async with DB_WRITE_LOCK:
                    return await _run_once()
            else:
                return await _run_once()
        except (aiosqlite.OperationalError, sqlite3.OperationalError) as e:
            msg = str(e).lower()
            if "database is locked" in msg or "database table is locked" in msg:
                if attempt < attempts:
                    backoff = min(2.0, delay * attempt) + random.uniform(0, 0.15)
                    log.warning(f"SQLite locked; retrying ({attempt}/{attempts}) in {backoff:.2f}s: {query[:48]}...")
                    await asyncio.sleep(backoff)
                    continue
            # Other operational errors fall through to log below
            log.error(f"Database error on query '{query[:50]}...': {e}")
            return None
        except aiosqlite.Error as e:
            log.error(f"Database error on query '{query[:50]}...': {e}")
            return None
        except Exception as e:
            log.error(f"Unexpected DB error on '{query[:50]}...': {e}")
            return None
    # If we exhausted retries
    log.error(f"Database permanently locked after {attempts} attempts for query '{query[:50]}...'")
    return None

async def setup_database():
    await _execute_db("""
        CREATE TABLE IF NOT EXISTS TokenLog (
            mint_address TEXT PRIMARY KEY,
            intel_json TEXT,
            discovered_at TEXT DEFAULT (CURRENT_TIMESTAMP),
            status TEXT DEFAULT 'discovered',
            last_analyzed_time TEXT,
            last_served_time TEXT,
            last_snapshot_time TEXT,
            sss_score INTEGER,
            mms_score INTEGER,
            final_score INTEGER,
            age_minutes REAL,
            is_hatching_candidate BOOLEAN DEFAULT FALSE,
            is_cooking_candidate BOOLEAN DEFAULT FALSE,
            is_fresh_candidate BOOLEAN DEFAULT FALSE,
            enhanced_bucket TEXT DEFAULT 'standby',
            served_count INTEGER DEFAULT 0
        )
    """, commit=True)
    await _execute_db("CREATE INDEX IF NOT EXISTS idx_status ON TokenLog (status)", commit=True)
    await _execute_db("CREATE INDEX IF NOT EXISTS idx_candidates ON TokenLog (is_hatching_candidate, is_cooking_candidate, is_fresh_candidate)", commit=True)
    
    await _execute_db("""
        CREATE TABLE IF NOT EXISTS TokenSnapshots (
            snapshot_id INTEGER PRIMARY KEY AUTOINCREMENT,
            mint_address TEXT NOT NULL,
            snapshot_time TEXT DEFAULT (CURRENT_TIMESTAMP),
            liquidity_usd REAL,
            volume_24h_usd REAL,
            market_cap_usd REAL,
            FOREIGN KEY(mint_address) REFERENCES TokenLog(mint_address)
        )
    """, commit=True)
    await _execute_db("CREATE INDEX IF NOT EXISTS idx_snapshot_mint_time ON TokenSnapshots (mint_address, snapshot_time)", commit=True)

    # Persist per-chat segment message IDs so we edit instead of spamming new ones
    await _execute_db(
        """
        CREATE TABLE IF NOT EXISTS PushMessages (
            chat_id INTEGER NOT NULL,
            segment TEXT NOT NULL,
            message_id INTEGER NOT NULL,
            last_updated TEXT DEFAULT (CURRENT_TIMESTAMP),
            PRIMARY KEY (chat_id, segment)
        )
        """,
        commit=True,
    )

    # Simple key-value store for misc persistent state
    await _execute_db(
        """
        CREATE TABLE IF NOT EXISTS KeyValueStore (
            key TEXT PRIMARY KEY,
            value TEXT
        )
        """, commit=True
    )

    # Add enhanced columns if they don't exist
    cursor = await _execute_db("PRAGMA table_info(TokenLog)", fetch='all')
    columns = [row[1] for row in cursor] if cursor else []
    if 'priority' not in columns:
        await _execute_db("ALTER TABLE TokenLog ADD COLUMN priority INTEGER DEFAULT 0", commit=True)
    if 'momentum' not in columns:
        await _execute_db("ALTER TABLE TokenLog ADD COLUMN momentum REAL DEFAULT 0.0", commit=True)
    if 'enhanced_bucket' not in columns:
        await _execute_db("ALTER TABLE TokenLog ADD COLUMN enhanced_bucket TEXT DEFAULT 'standby'", commit=True)
    if 'last_served_time' not in columns:
        await _execute_db("ALTER TABLE TokenLog ADD COLUMN last_served_time TEXT", commit=True)
    if 'served_count' not in columns:
        await _execute_db("ALTER TABLE TokenLog ADD COLUMN served_count INTEGER DEFAULT 0", commit=True)
    
    # Add performance indexes
    await _execute_db("CREATE INDEX IF NOT EXISTS idx_reanalyze ON TokenLog (enhanced_bucket, last_snapshot_time)", commit=True)
    await _execute_db("CREATE INDEX IF NOT EXISTS idx_score ON TokenLog (final_score DESC, last_analyzed_time DESC)", commit=True)
    await _execute_db("CREATE INDEX IF NOT EXISTS idx_age ON TokenLog (age_minutes)", commit=True)
    log.info("🗄️ Database ready.")

async def upsert_token_intel(mint: str, intel: Dict[str, Any]):
    """Inserts or updates a token's full analysis record."""
    intel_str = json.dumps(intel)
    query = """
        INSERT INTO TokenLog (mint_address, intel_json, status, last_analyzed_time, sss_score, mms_score, final_score, age_minutes)
        VALUES (?, ?, 'analyzed', CURRENT_TIMESTAMP, ?, ?, ?, ?)
        ON CONFLICT(mint_address) DO UPDATE SET
            intel_json = excluded.intel_json,
            status = excluded.status,
            last_analyzed_time = excluded.last_analyzed_time,
            sss_score = excluded.sss_score,
            mms_score = excluded.mms_score,
            final_score = excluded.final_score,
            age_minutes = excluded.age_minutes
    """
    params = (
        mint, intel_str, intel.get("sss_score"), intel.get("mms_score"),
        intel.get("score"), intel.get("age_minutes")
    )
    await _execute_db(query, params, commit=True)

async def save_snapshot(mint: str, data: Dict[str, Any]) -> None:
    """Persist a lightweight market snapshot and update last_snapshot_time."""
    try:
        liq = data.get("liquidity_usd")
        vol = data.get("volume_24h_usd")
        mc = data.get("market_cap_usd")
        await _execute_db(
            "INSERT INTO TokenSnapshots (mint_address, liquidity_usd, volume_24h_usd, market_cap_usd) VALUES (?, ?, ?, ?)",
            (mint, liq, vol, mc), commit=True
        )
        await _execute_db(
            "UPDATE TokenLog SET last_snapshot_time = CURRENT_TIMESTAMP WHERE mint_address = ?",
            (mint,), commit=True
        )
    except Exception:
        pass

async def load_latest_snapshot(mint: str) -> Optional[Dict[str, Any]]:
    """Load the most recent snapshot and return with age in seconds as 'snapshot_age_sec'."""
    row = await _execute_db(
        "SELECT liquidity_usd, volume_24h_usd, market_cap_usd, snapshot_time FROM TokenSnapshots WHERE mint_address = ? ORDER BY snapshot_time DESC LIMIT 1",
        (mint,), fetch='one'
    )
    if not row:
        return None
    liq, vol, mc, ts = row
    try:
        # SQLite CURRENT_TIMESTAMP uses 'YYYY-MM-DD HH:MM:SS'
        dt = datetime.fromisoformat(str(ts).replace(' ', 'T')).replace(tzinfo=timezone.utc)
    except Exception:
        try:
            dt = datetime.strptime(str(ts), "%Y-%m-%d %H:%M:%S").replace(tzinfo=timezone.utc)
        except Exception:
            return None
    age_sec = (datetime.now(timezone.utc) - dt).total_seconds()
    return {"liquidity_usd": liq, "volume_24h_usd": vol, "market_cap_usd": mc, "snapshot_time": str(ts), "snapshot_age_sec": age_sec}

async def get_push_message_id(chat_id: int, segment: str) -> Optional[int]:
    row = await _execute_db(
        "SELECT message_id FROM PushMessages WHERE chat_id = ? AND segment = ?",
        (int(chat_id), str(segment)),
        fetch='one',
    )
    return int(row[0]) if row and row[0] is not None else None

async def set_push_message_id(chat_id: int, segment: str, message_id: int) -> None:
    await _execute_db(
        """
        INSERT INTO PushMessages (chat_id, segment, message_id, last_updated)
        VALUES (?, ?, ?, CURRENT_TIMESTAMP)
        ON CONFLICT(chat_id, segment) DO UPDATE SET
            message_id = excluded.message_id,
            last_updated = excluded.last_updated
        """,
        (int(chat_id), str(segment), int(message_id)),
        commit=True,
    )

async def mark_as_served(mints: List[str]):
    if not mints:
        return
    placeholders = ','.join('?' for _ in mints)
    # Do not change status or clear tags; just set last_served_time and bump served_count
    query = f"""
        UPDATE TokenLog SET
            last_served_time = CURRENT_TIMESTAMP,
            served_count = COALESCE(served_count, 0) + 1
        WHERE mint_address IN ({placeholders})
    """
    await _execute_db(query, (*mints,), commit=True)

async def get_recently_served_mints(hours: int) -> set:
    rows = await _execute_db(
        "SELECT mint_address FROM TokenLog WHERE last_served_time > datetime('now', ?)",
        (f"-{hours} hours",), fetch='all'
    )
    return {r[0] for r in rows} if rows else set()