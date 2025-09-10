#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Token Tony - v23.0 "The Alpha Refactor"
# Modular, clean, and ready for the next evolution.

import asyncio
import logging
import sys
import json
import random
import websockets
import time
import statistics
import re
from datetime import datetime, timezone, time as dtime, timedelta
from typing import Any, Dict, List, Optional, Tuple
from pathlib import Path
import html as _html

import httpx
from telegram import Update, ReplyKeyboardRemove
from telegram.constants import ChatAction, ParseMode
from telegram.ext import (Application, CommandHandler, ContextTypes,
                          MessageHandler, filters)
from telegram.request import HTTPXRequest
from config import (ALCHEMY_RPC_URL, ALCHEMY_WS_URL, BIRDEYE_API_KEY, CONFIG,
                    HELIUS_API_KEY, HELIUS_RPC_URL, HELIUS_WS_URL, KNOWN_QUOTE_MINTS,
                    OWNER_ID, PUBLIC_CHAT_ID, SYNDICA_RPC_URL, SYNDICA_WS_URL,
                    TELEGRAM_TOKEN, VIP_CHAT_ID)
from tony_helpers.analysis import (POOL_BIRTH_CACHE, enrich_token_intel,
                                   _compute_mms, _compute_score, _compute_sss)
from tony_helpers.api import (API_PROVIDERS, LITE_MODE_UNTIL, _fetch,
                              extract_mint_from_check_text, fetch_birdeye,
                              fetch_dexscreener_by_mint,
                              fetch_dexscreener_chart, fetch_market_snapshot,
                              fetch_top10_via_rpc, fetch_jupiter_has_route)
from tony_helpers.db import (_execute_db, get_push_message_id,
                             get_recently_served_mints, load_latest_snapshot,
                             mark_as_served, save_snapshot, setup_database,
                             set_push_message_id, upsert_token_intel)
from tony_helpers.reports import (build_compact_report3, build_full_report2,
                                  load_advanced_quips)
from tony_helpers.utils import (_can_post_to_chat, _notify_owner,
                                _parse_typed_value, is_valid_solana_address,
                                OUTBOX, TokenBucket)

# --- Logging ---
LOG_FILE = "tony_log.log"
try:
    from logging.handlers import TimedRotatingFileHandler
    handlers = [TimedRotatingFileHandler(LOG_FILE, when='midnight', backupCount=7, encoding="utf-8"), logging.StreamHandler()]
except Exception:
    handlers = [logging.FileHandler(LOG_FILE, encoding="utf-8"), logging.StreamHandler()]
logging.basicConfig(level=logging.INFO, format="[%(asctime)s] %(levelname)s: %(message)s", handlers=handlers)
log = logging.getLogger("token_tony")
logging.getLogger("telegram").setLevel(logging.WARNING)
logging.getLogger("httpx").setLevel(logging.WARNING)
logging.getLogger("websockets").setLevel(logging.INFO)


def pick_header_label(command: str | None = None) -> str:
    """Selects a random, flavorful header for a command response."""
    headers = {
        "fresh": ["🪺 Fresh Hatch", "✨ Just Minted", "🔧 New Bolts"],
        "hatching": ["🐣 Nest Cracking", "🪺 New Brood", "🛰️ First Flight"],
        "cooking": ["🍳 Now Cooking", "🔥 Heat Rising", "🥓 Sizzle Check"],
        "top": ["🏆 Top Shelf", "⛰️ Peak View", "👑 Crowned Picks"],
        "check": ["🔎 Deep Scan", "🧪 Lab Read", "🧰 Toolbox Check"],
        "general": [
            "🛡️ Guard Duty", "🧭 Compass Check", "🧰 Toolbox Open",
            "🟢 Green Light", "⚡ Power Check",
        ],
    }
    cmd_to_bucket = {
        "/fresh": "fresh", "/hatching": "hatching", "/cooking": "cooking",
        "/top": "top", "/check": "check",
    }
    bucket = cmd_to_bucket.get(command or "", "general")
    pool = headers.get(bucket, headers["general"])
    return random.choice(pool)

# A mapping of DEX names to their program ID and the base58-encoded discriminator
# for their specific "create new pool" instruction. This is the most efficient way
# to subscribe to only the events we care about.
DEX_PROGRAMS_FOR_FIREHOSE = {
    # Slimmed to 3 Raydium families to reduce Helius usage/cost.
    # v4 (legacy AMM; most common new pools)
    "Raydium_v4": {
        "program_id": "675kPX9MHTjS2zt1qfr1NYHuzeLXfQM9H24wFSUt1Mp8",
        "discriminator": "3iVvT9Yd6oY"  # initialize2
    },
    # CLMM (concentrated)
    "Raydium_CLMM": {
        "program_id": "CAMMCzo5YL8w4VFF8KVHrK22GGUsp5VTaW7grrKgrWqK",
        "discriminator": "3iVvT9Yd6oY"  # initialize2
    },
    # CPMM (new constant-product router)
    "Raydium_CPMM": {
        "program_id": "CPMMoo8L3F4NbTegBCKVNunggL7H1ZpdTHKxQB5qKP1C",
        "discriminator": "3iVvT9Yd6oY"  # initialize2
    },
}

# --- Global State ---
FIREHOSE_STATUS: Dict[str, str] = {}
PUMPFUN_STATUS = "🔴 Disconnected"
# Adaptive processing state
from collections import deque
recent_processing_times = deque(maxlen=50) # Now local to this module
adaptive_batch_size = CONFIG["MIN_BATCH_SIZE"]
DB_MARKER_FILE = "tony_db.marker"
DISCOVERY_BUCKET = TokenBucket(capacity=8, refill_amount=8, interval_seconds=1.0)

# ======================================================================================
# Block 4: Unified Discovery Engine
# ======================================================================================

#

def _looks_like_solana_address(s: str) -> bool:
    return bool(re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", s or ""))

_seen_mints = deque(maxlen=2000)
CHANNEL_ALERT_SENT: Dict[str, bool] = {} # In-memory cache for this session

async def pumpportal_worker():
    """Single-socket PumpPortal subscriber with reconnect + resubscribe."""
    url = "wss://pumpportal.fun/api/data"
    backoff = 1.0
    while True:
        try:
            log.info("PumpPortal: Connecting...")
            global PUMPFUN_STATUS
            PUMPFUN_STATUS = "🟡 Connecting"
            async with websockets.connect(url, ping_interval=15, ping_timeout=10) as ws:
                backoff = 1.0
                # Subscribe to new tokens + migrations using a single socket
                await ws.send(json.dumps({"method": "subscribeNewToken"}))
                await ws.send(json.dumps({"method": "subscribeMigration"}))
                log.info("PumpPortal: Subscribed (new tokens + migration).")
                PUMPFUN_STATUS = "🟢 Connected"
                while True:
                    msg = await ws.recv()
                    try:
                        data = json.loads(msg)
                    except Exception:
                        continue
                    # Accept any payload containing a plausible mint
                    if isinstance(data, dict):
                        cand = data.get("mint") or data.get("token") or data.get("tokenMint")
                        if isinstance(cand, str) and _looks_like_solana_address(cand):
                            async def _queue():
                                await DISCOVERY_BUCKET.acquire(1)
                                await process_discovered_token(cand)
                            asyncio.create_task(_queue())
        except Exception as e:
            log.warning(f"PumpPortal: Disconnected: {e}. Reconnecting in {backoff:.1f}s...")
            PUMPFUN_STATUS = "🔴 Disconnected"
            await asyncio.sleep(backoff + random.uniform(0, 0.5))
            backoff = min(30.0, backoff * 2)


## Removed legacy Pump.fun client-api worker and Helius programSubscribe variant (unused).

async def _fetch_transaction(c: httpx.AsyncClient, rpc_url: str, signature: str) -> Optional[Dict[str, Any]]:
    payload = {
        "jsonrpc": "2.0", "id": 1, "method": "getTransaction",
        "params": [signature, {"encoding": "jsonParsed", "maxSupportedTransactionVersion": 0}]
    }
    res = await _fetch(c, rpc_url, method="POST", json=payload, timeout=10.0)
    return (res or {}).get("result") if res else None

def _extract_mints_from_tx_result(tx_result: Dict[str, Any]) -> List[str]:
    """Best-effort extraction of base/quote mints from a transaction result."""
    mints: set = set()
    meta = tx_result.get("meta") or {}
    for bal in meta.get("postTokenBalances", []) + meta.get("preTokenBalances", []):
        if mint := bal.get("mint"):
            mints.add(mint)
    # Also scan any parsed instruction infos that expose a 'mint'
    try:
        tx = tx_result.get("transaction", {})
        msg = tx.get("message", {})
        for ix in msg.get("instructions", []):
            if (parsed := ix.get("parsed")) and isinstance(parsed, dict):
                info = parsed.get("info", {})
                if mint := info.get("mint"):
                    mints.add(mint)
    except Exception:
        pass
    # Filter out known quote mints and limit number
    filtered = [m for m in mints if m not in KNOWN_QUOTE_MINTS]
    return filtered[:4]

POOL_BIRTH_KEYWORDS = {"createpool", "initializepool", "initialize_pool", "pool-init", "open_pool", "initialize2"}
GO_LIVE_KEYWORDS = {"addliquidity", "increase_liquidity"}
FLOW_KEYWORDS = {"swap"}

async def _logs_subscriber(provider_name: str, ws_url: str, rpc_url: str):
    key = f"Logs-{provider_name}"
    FIREHOSE_STATUS[key] = "🟡 Connecting"
    subscriptions = []
    while True:
        try:
            log.info(f"Logs Firehose ({provider_name}): Connecting {ws_url} ...")
            # Helius suggests pings approx every 60s; keep heartbeat under that
            async with websockets.connect(ws_url, ping_interval=55) as websocket:
                # Subscribe per DEX program using logsSubscribe mentions
                for name, d in DEX_PROGRAMS_FOR_FIREHOSE.items():
                    sub = {
                        "jsonrpc": "2.0", "id": random.randint(1000, 999999), "method": "logsSubscribe",
                        # Use mentions array per Solana WS API
                        "params": [{"mentions": [d["program_id"]]}, {"commitment": "processed"}]
                    }
                    await websocket.send(json.dumps(sub))
                    subscriptions.append(sub["id"])
                FIREHOSE_STATUS[key] = "🟢 Connected"
                log.info(f"✅ Logs Firehose ({provider_name}): Subscribed to {len(DEX_PROGRAMS_FOR_FIREHOSE)} programs.")
                async with httpx.AsyncClient(http2=True) as client:
                    while websocket.open:
                        try:
                            raw = await asyncio.wait_for(websocket.recv(), timeout=90.0)
                            msg = json.loads(raw)
                            if msg.get("method") != "logsNotification":
                                continue
                            result = msg.get("params", {}).get("result", {})
                            signature = result.get("value", {}).get("signature")
                            if not signature:
                                continue
                            # Check logs text for relevant signals before fetching tx
                            logs_list = (result.get("value", {}).get("logs") or [])
                            logs_text = "\n".join(logs_list).lower()
                            # Dial back: only react to pool birth to reduce Helius load
                            if not any(k in logs_text for k in POOL_BIRTH_KEYWORDS):
                                continue

                            # Rate-limit transaction lookups to reduce RPC spend
                            tx_res = await _fetch_transaction(client, rpc_url, signature)
                            if not tx_res:
                                continue
                            # Optional: ignore very old transactions to avoid backfill floods
                            try:
                                bt = tx_res.get("blockTime")
                                if bt and (time.time() - int(bt)) > 600:
                                    continue
                            except Exception:
                                pass
                            bt = tx_res.get("blockTime")
                            for mint in _extract_mints_from_tx_result(tx_res):
                                if bt:
                                    try:
                                        POOL_BIRTH_CACHE[mint] = int(bt)
                                    except Exception:
                                        pass
                                log.info(f"Logs Firehose ({provider_name}): discovered candidate mint {mint} from signature {signature}")
                                async def _queue():
                                    await DISCOVERY_BUCKET.acquire(1)
                                    await process_discovered_token(mint)
                                asyncio.create_task(_queue())
                        except asyncio.TimeoutError:
                            log.info(f"Logs Firehose ({provider_name}): idle, connection alive.")
                        except Exception as e:
                            log.error(f"Logs Firehose ({provider_name}): error {e}, reconnecting...")
                            break
        except Exception as e:
            FIREHOSE_STATUS[key] = f"🔴 Error: {e.__class__.__name__}"
            log.error(f"Logs Firehose ({provider_name}): connection failed: {e}. Retrying in 30s...")
            await asyncio.sleep(30)

async def logs_firehose_worker():
    """Start logsSubscribe firehose across configured providers (Helius/Syndica/Alchemy)."""
    providers = []
    if HELIUS_API_KEY:
        providers.append(("Helius", HELIUS_WS_URL, HELIUS_RPC_URL))
    if SYNDICA_WS_URL and SYNDICA_RPC_URL:
        providers.append(("Syndica", SYNDICA_WS_URL, SYNDICA_RPC_URL))
    if ALCHEMY_WS_URL and ALCHEMY_RPC_URL:
        providers.append(("Alchemy", ALCHEMY_WS_URL, ALCHEMY_RPC_URL))
    if not providers:
        log.warning("Logs Firehose disabled: no provider URLs configured (HELIUS/SYNDICA/ALCHEMY).")
        return
    log.info(f"Logs Firehose: launching {len(providers)} providers...")
    await asyncio.gather(*[_logs_subscriber(name, ws, http) for name, ws, http in providers])

async def discover_from_gecko_new_pools(client: httpx.AsyncClient) -> List[str]:
    """Discover recent Raydium pools on Solana via GeckoTerminal v2.
    Endpoint: /api/v2/networks/solana/new_pools?include=base_token,quote_token,dex,network
    """
    from tony_helpers.api import GECKO_API_URL
    mints: set = set()
    headers = {
        "Accept": "application/json;version=20230302",
        "User-Agent": "Mozilla/5.0"
    }
    url = f"{GECKO_API_URL}/networks/solana/new_pools?include=base_token,quote_token,dex,network"
    try:
        res = await _fetch(client, url, headers=headers)
        data = (res or {}).get("data") or []
        included = (res or {}).get("included") or []
        tok_addr = {item.get("id"): (item.get("attributes") or {}).get("address") for item in included if item.get("type") == "tokens"}
        dex_name = {item.get("id"): (item.get("attributes") or {}).get("name", "").lower() for item in included if item.get("type") == "dexes"}

        for pool in data:
            rel = pool.get("relationships", {})
            base_rel = (rel.get("base_token") or {}).get("data") or {}
            quote_rel = (rel.get("quote_token") or {}).get("data") or {}
            dex_rel = (rel.get("dex") or {}).get("data") or {}
            # Filter to Raydium where possible to reduce noise
            dex_id = dex_rel.get("id")
            if dex_id and dex_name.get(dex_id) and "raydium" not in dex_name.get(dex_id, ""):
                continue
            base = tok_addr.get(base_rel.get("id"))
            quote = tok_addr.get(quote_rel.get("id"))
            if base and base not in KNOWN_QUOTE_MINTS:
                mints.add(base)
            if quote and quote not in KNOWN_QUOTE_MINTS and quote != base:
                mints.add(quote)
    except Exception as e:
        log.warning(f"GeckoTerminal new_pools discovery failed: {e}")
    return list(mints)

async def _discover_from_gecko_search(client: httpx.AsyncClient, query: str) -> List[str]:
    """Search pools globally and filter to Solana/Raydium."""
    mints: set = set()
    from tony_helpers.api import GECKO_API_URL
    from tony_helpers.analysis import GECKO_SEARCH_CACHE

    headers = {
        "Accept": "application/json;version=20230302",
        "User-Agent": "Mozilla/5.0"
    }
    url = f"{GECKO_API_URL}/search/pools?query={query}&include=base_token,quote_token,dex,network"
    if (cached := GECKO_SEARCH_CACHE.get(url)):
        return cached
    try:
        res = await _fetch(client, url, headers=headers)
        data = (res or {}).get("data") or []
        included = (res or {}).get("included") or []
        tok_addr = {item.get("id"): (item.get("attributes") or {}).get("address") for item in included if item.get("type") == "tokens"}
        dex_name = {item.get("id"): (item.get("attributes") or {}).get("name", "").lower() for item in included if item.get("type") == "dexes"}
        networks = {item.get("id"): (item.get("attributes") or {}).get("identifier", "").lower() for item in included if item.get("type") == "networks"}

        for pool in data:
            rel = pool.get("relationships", {})
            base_rel = (rel.get("base_token") or {}).get("data") or {}
            quote_rel = (rel.get("quote_token") or {}).get("data") or {}
            dex_rel = (rel.get("dex") or {}).get("data") or {}
            net_rel = (rel.get("network") or {}).get("data") or {}
            if networks.get(net_rel.get("id")) and networks.get(net_rel.get("id")) != "solana":
                continue
            if dex_name.get(dex_rel.get("id")) and "raydium" not in dex_name.get(dex_rel.get("id"), ""):
                continue
            base = tok_addr.get(base_rel.get("id"))
            quote = tok_addr.get(quote_rel.get("id"))
            if base and base not in KNOWN_QUOTE_MINTS:
                mints.add(base)
            if quote and quote not in KNOWN_QUOTE_MINTS and quote != base:
                mints.add(quote)
    except Exception as e:
        log.warning(f"GeckoTerminal search discovery for query '{query}' failed: {e}")
    result = list(mints)
    GECKO_SEARCH_CACHE[url] = result
    return result

async def discover_from_gecko_search_pools(client: httpx.AsyncClient) -> List[str]:
    """Search pools globally and filter to Solana/Raydium."""
    return await _discover_from_gecko_search(client, "solana")

async def discover_from_gecko_search_tokens(client: httpx.AsyncClient) -> List[str]:
    """Use GeckoTerminal search pools API (alternate query) and filter to Solana/Raydium."""
    return await _discover_from_gecko_search(client, "bonk")

async def discover_from_dexscreener_new_pairs(client: httpx.AsyncClient) -> List[str]:
    """Discover recent pairs on Solana via DexScreener and resolve their mints.
    DexScreener occasionally returns a JSON with schemaVersion but null pairs due to edge caching.
    Mitigate with HTTP/1.1, no-cache headers, and a jittered query param to bust stale edges.
    """
    from tony_helpers.analysis import DS_NEW_CACHE
    mints = set()
    base_url = "https://api.dexscreener.com/latest/dex/pairs/solana/new"
    try:
        if (cached := DS_NEW_CACHE.get(base_url)):
            return cached
        ds_headers = {
            "Accept": "application/json",
            "Accept-Language": "en-US,en;q=0.9",
            "User-Agent": "Mozilla/5.0",
            "Cache-Control": "no-cache",
            "Pragma": "no-cache",
            "Referer": "https://dexscreener.com/solana",
            "Origin": "https://dexscreener.com"
        }
        # Use HTTP/1.1 for DS to reduce null responses
        async def _ds_get_json() -> Optional[Dict[str, Any]]:
            try:
                async with httpx.AsyncClient(http2=False, timeout=CONFIG["HTTP_TIMEOUT"]) as ds_c:
                    # Add a tiny jitter param to avoid stale CDN edges without breaking cache keying
                    req_url = f"{base_url}?t={int(time.time()) % 7}"
                    r = await ds_c.get(req_url, headers=ds_headers, follow_redirects=True)
                    r.raise_for_status()
                    return r.json()
            except Exception:
                return None

        res = await _ds_get_json()
        if not res or not (pairs := res.get("pairs")):
            # One retry with small jitter
            await asyncio.sleep(2.0 + random.uniform(0, 0.5))
            res = await _ds_get_json()
            pairs = (res or {}).get("pairs") if res else None
            if not res or not pairs:
                # Reduce noise: log at debug; this happens sporadically due to DS edge caching
                if res:
                    preview = str(res)[:180]
                    log.debug(f"DexScreener /new returned null pairs. Preview: {preview}")
                return []
        
        # The /new endpoint already contains the token addresses. No need for a second, redundant API call.
        for pair in pairs:
            if base_token := pair.get("baseToken", {}).get("address"):
                if base_token not in KNOWN_QUOTE_MINTS:
                    mints.add(base_token)
            if quote_token := pair.get("quoteToken", {}).get("address"):
                if quote_token not in KNOWN_QUOTE_MINTS and quote_token != base_token:
                    mints.add(quote_token)
        result = list(mints)
        DS_NEW_CACHE[base_url] = result
        return result
    except Exception as e:
        log.warning(f"DexScreener discovery failed: {e}")
        return []

# Removed broken DexScreener "recent" endpoint usage (/latest/dex/pairs/solana).
# Use /latest/dex/search and /latest/dex/pairs/solana/new instead.

async def discover_from_dexscreener_search_recent(client: httpx.AsyncClient) -> List[str]:
    """Fallback: use DexScreener search API and filter Solana pairs by recent creation time."""
    mints = set()
    url = "https://api.dexscreener.com/latest/dex/search?q=solana"
    try:
        ds_headers = {
            "Accept": "application/json",
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://dexscreener.com/solana"
        }
        # Force HTTP/1.1 for DS search as well
        async with httpx.AsyncClient(http2=False, timeout=CONFIG["HTTP_TIMEOUT"]) as ds_c:
            r = await ds_c.get(url, headers=ds_headers, follow_redirects=True)
            r.raise_for_status()
            res = r.json()
        pairs = (res or {}).get("pairs") or []
        if not pairs:
            if res:
                preview = str(res)[:180]
                log.info(f"DexScreener search: no pairs. Preview: {preview}")
            return []

        now_ms = int(time.time() * 1000)
        # Slightly wider window (10 minutes) to catch true new pairs reliably
        freshness_minutes = 10

        for p in pairs:
            if p.get("chainId") != "solana":
                continue
            created_ms = p.get("pairCreatedAt") or p.get("createdAt")
            try:
                if not created_ms:
                    continue
                age_min = (now_ms - int(created_ms)) / 60000.0
            except (ValueError, TypeError):
                continue
            if age_min > freshness_minutes:
                continue

            base_token = (p.get("baseToken") or {}).get("address")
            quote_token = (p.get("quoteToken") or {}).get("address")
            if base_token and base_token not in KNOWN_QUOTE_MINTS:
                mints.add(base_token)
            if quote_token and quote_token not in KNOWN_QUOTE_MINTS and quote_token != base_token:
                mints.add(quote_token)
    except Exception as e:
        log.warning(f"DexScreener search discovery failed: {e}")
    return list(mints)

async def aggregator_poll_worker():
    """Background worker to periodically poll aggregators for new tokens."""
    log.info("🦎 Aggregator Poller: Worker starting.")
    while True:
        try:
            async with httpx.AsyncClient(http2=True) as client:
                gecko_task = discover_from_gecko_new_pools(client)
                gecko_search_task = discover_from_gecko_search_pools(client)
                gecko_token_search_task = discover_from_gecko_search_tokens(client)
                dexscreener_new_task = discover_from_dexscreener_new_pairs(client)
                dexscreener_search_task = discover_from_dexscreener_search_recent(client)
                results = await asyncio.gather(
                    gecko_task,
                    gecko_search_task,
                    gecko_token_search_task,
                    dexscreener_new_task,
                    dexscreener_search_task,
                    return_exceptions=True
                )
                
                all_new_mints = set()
                for i, result in enumerate(results):
                    source_name = (
                        "GeckoTerminal new" if i == 0 else
                        "GeckoTerminal search pools" if i == 1 else
                        "GeckoTerminal search tokens" if i == 2 else
                        "DexScreener /new" if i == 3 else
                        "DexScreener search filtered"
                    )
                    if isinstance(result, Exception):
                        log.warning(f"🦎 Aggregator Poller: {source_name} task failed: {result}")
                    elif result:
                        log.info(f"🦎 Aggregator Poller: Found {len(result)} potential new tokens from {source_name}.")
                        all_new_mints.update(result)
                    else:
                        log.info(f"🦎 Aggregator Poller: {source_name} returned no new tokens this cycle.")
                if all_new_mints:
                    total = len(all_new_mints)
                    max_new = int(CONFIG.get("AGGREGATOR_MAX_NEW_PER_CYCLE", 0) or 0)
                    to_queue = list(all_new_mints)
                    if max_new > 0 and total > max_new:
                        to_queue = random.sample(to_queue, max_new)
                        log.info(f"🦎 Aggregator Poller: {total} found, capping to {max_new} this cycle.")
                    else:
                        log.info(f"🦎 Aggregator Poller: Total unique new mints this cycle: {total}.")
                    for mint in to_queue:
                        async def _queue(m=mint):
                            await DISCOVERY_BUCKET.acquire(1)
                            await process_discovered_token(m)
                        asyncio.create_task(_queue())
        except Exception as e:
            log.error(f"🦎 Aggregator Poller: Error during poll cycle: {e}")
        
        await asyncio.sleep(CONFIG["AGGREGATOR_POLL_INTERVAL_MINUTES"] * 60)

# ======================================================================================
# Block 5: Dynamic Pot System & Analysis Pipeline
# ======================================================================================

async def process_discovered_token(mint: str):
    """Single entry point for any newly discovered token from any source."""
    try:
        if not is_valid_solana_address(mint) or mint in _seen_mints:
            return
        
        if await _execute_db("SELECT 1 FROM TokenLog WHERE mint_address = ?", (mint,), fetch='one'):
            return

        _seen_mints.append(mint)
        log.info(f"DISCOVERED: {mint}. Queued for initial analysis.")
        
        # Just insert it with 'discovered' status. The initial_analyzer_worker will pick it up.
        await _execute_db("INSERT OR IGNORE INTO TokenLog (mint_address, status) VALUES (?, 'discovered')", (mint,), commit=True)
    except Exception as e:
        # This is a critical catch-all to ensure we see any errors during the very first step.
        log.error(f"CRITICAL ERROR in process_discovered_token for mint '{mint}': {e}", exc_info=True)

# Priority calculation helper (engine)
def calculate_priority(i: Dict[str, Any]) -> int:
    try:
        score = float(i.get("score", 0) or 0)
    except Exception:
        score = 0.0
    try:
        liq = float(i.get("liquidity_usd", 0) or 0)
    except Exception:
        liq = 0.0
    try:
        vol = float(i.get("volume_24h_usd", 0) or 0)
    except Exception:
        vol = 0.0
    try:
        age_m = float(i.get("age_minutes", 0) or 0)
    except Exception:
        age_m = 0.0

    def norm(x: float, k: float) -> float:
        return x / (x + k) if x >= 0 else 0.0

    pr = 0.0
    pr += 0.6 * score
    pr += 20.0 * norm(liq, 25_000)
    pr += 20.0 * norm(vol, 50_000)
    if age_m > 60:
        pr -= min(15.0, (age_m - 60) / 60.0 * 5.0)
    return int(max(0, min(100, pr)))

async def update_token_tags(mint: str, intel: Dict):
    """Updates the boolean candidate flags and enhanced bucket based on the latest intel."""
    # Compute multiple age signals (minutes)
    ages = []
    try:
        if (a := intel.get("age_minutes")) is not None:
            ages.append(float(a))
    except Exception:
        pass
    try:
        if (iso := intel.get("created_at_pool")):
            dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
            ages.append((datetime.now(timezone.utc) - dt).total_seconds() / 60)
    except Exception:
        pass
    try:
        row = await _execute_db("SELECT discovered_at FROM TokenLog WHERE mint_address=?", (mint,), fetch='one')
        if row and row[0]:
            ddt = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
            ages.append((datetime.now(timezone.utc) - ddt).total_seconds() / 60)
    except Exception:
        pass
    recent_age = min(ages) if ages else None

    # Hatching: newborn (≤ HATCHING_MAX_AGE_MINUTES) and has minimum liquidity.
    # If liquidity is unknown (None), allow into hatching; only exclude when explicit liq is below threshold.
    liq_val = intel.get("liquidity_usd", None)
    meets_liq = True if liq_val is None else (float(liq_val or 0) >= CONFIG["MIN_LIQUIDITY_FOR_HATCHING"])
    is_hatching = (
        (recent_age is not None and recent_age <= CONFIG["HATCHING_MAX_AGE_MINUTES"]) and
        meets_liq
    )

    # Cooking: momentum heuristic — combine price move and minimum volume
    try:
        price_change = float(intel.get("price_change_24h", 0) or 0)
    except Exception:
        price_change = 0.0
    try:
        vol24h = float(intel.get("volume_24h_usd", 0) or 0)
    except Exception:
        vol24h = 0.0
    vol_floor = float(CONFIG.get("COOKING_FALLBACK_VOLUME_MIN_USD", 200) or 200)
    is_cooking = (price_change >= 15.0 and vol24h >= max(500.0, vol_floor))

    # Fresh: young (≤24h)
    is_fresh = (intel.get("age_minutes", 99999) < 24 * 60)

    priority = calculate_priority(intel)
    score = intel.get("score", 0)

    bucket = "standby"
    if priority >= 80:
        bucket = "priority"
    elif is_hatching:
        bucket = "hatching"
    elif is_fresh:
        bucket = "fresh"
    elif is_cooking:
        bucket = "cooking"
    elif score >= 70:
        bucket = "top"

    log.info(f"Updating tags for {mint}: hatching={is_hatching}, cooking={is_cooking}, fresh={is_fresh}, priority={priority}, score={score}, bucket={bucket}")

    query = """
        UPDATE TokenLog SET
            is_hatching_candidate = ?,
            is_cooking_candidate = ?,
            is_fresh_candidate = ?,
            enhanced_bucket = ?,
            priority = ?
        WHERE mint_address = ?
    """
    await _execute_db(query, (is_hatching, is_cooking, is_fresh, bucket, priority, mint), commit=True)

async def re_analyzer_worker():
    """Periodically refreshes market snapshot + retags a subset of tokens based on staleness.
    Uses bucket-specific cadences and caps batch size to avoid network/DB overload.
    """
    log.info("🤖 Re-Analyzer Engine: Firing up the refresh cycle.")
    while True:
        try:
            hm = f"-{int(CONFIG['HATCHING_REANALYZE_MINUTES'])} minutes"
            fm = f"-{int(CONFIG['FRESH_REANALYZE_MINUTES'])} minutes"
            cm = f"-{int(CONFIG['COOKING_REANALYZE_MINUTES'])} minutes"
            om = f"-{int(CONFIG['OTHER_REANALYZE_MINUTES'])} minutes"
            pm = f"-{int(CONFIG['HATCHING_REANALYZE_MINUTES'])} minutes" # Priority tokens should be re-analyzed frequently, like hatching
            limit = int(CONFIG.get("RE_ANALYZER_BATCH_LIMIT", 60))
            # Upgraded query to use enhanced_bucket for smarter re-analysis
            query = f"""
                SELECT mint_address, intel_json
                FROM TokenLog
                WHERE status IN ('analyzed','served') AND (
                    (enhanced_bucket = 'hatching' AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                 OR (enhanced_bucket = 'cooking' AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                 OR (enhanced_bucket = 'fresh' AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                 OR (enhanced_bucket = 'priority' AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                 OR (enhanced_bucket NOT IN ('hatching', 'cooking', 'fresh', 'priority') AND (last_snapshot_time IS NULL OR last_snapshot_time <= datetime('now', ?)))
                )
                ORDER BY
                    CASE enhanced_bucket
                        WHEN 'priority' THEN 0
                        WHEN 'hatching' THEN 1
                        WHEN 'cooking' THEN 2
                        WHEN 'fresh' THEN 3
                        ELSE 4
                    END,
                    COALESCE(last_snapshot_time, '1970-01-01') ASC
                LIMIT ?
            """
            rows = await _execute_db(query, (hm, cm, fm, pm, om, limit), fetch='all')
            if not rows: 
                log.info("🤖 Re-Analyzer: No tokens need refresh this cycle.")
                continue
            log.info(f"🤖 Re-Analyzer: Starting cycle for {len(rows)} tokens...")

            async with httpx.AsyncClient(http2=True) as client:
                # Concurrently fetch market data with bounded concurrency to avoid saturating network
                limit = int(CONFIG.get("RE_ANALYZER_FETCH_CONCURRENCY", 6))
                sem = asyncio.Semaphore(max(1, limit))
                async def _task(mint: str):
                    async with sem:
                        return await fetch_market_snapshot(client, mint)
                tasks = [_task(row[0]) for row in rows]
                results = await asyncio.gather(*tasks, return_exceptions=True)

            # Process each token sequentially for DB updates to reduce write contention
            for i, result in enumerate(results):
                mint, old_intel_json = rows[i]
                if isinstance(result, Exception) or not result:
                    # Graceful fallback: try recent snapshot instead of dropping the token
                    try:
                        snap = await load_latest_snapshot(mint)
                        stale_sec = int(CONFIG.get("SNAPSHOT_STALENESS_SECONDS", 600) or 600)
                        if isinstance(snap, dict) and (snap.get("snapshot_age_sec") or 1e9) <= stale_sec:
                            intel = json.loads(old_intel_json)
                            for k in ("liquidity_usd", "volume_24h_usd", "market_cap_usd"):
                                if k in snap:
                                    intel[k] = snap[k]
                            # Recompute scores with cached values
                            intel["mms_score"] = _compute_mms(intel)
                            intel["score"] = _compute_score(intel)
                            await upsert_token_intel(mint, intel)
                            await update_token_tags(mint, intel)
                            # Do not save another snapshot (we just used an existing one)
                            log.info(f"🤖 Re-Analyzer: Used cached snapshot for {mint} (live refresh unavailable).")
                            continue
                    except Exception:
                        pass
                    log.warning(f"🤖 Re-Analyzer: Failed to refresh market data for {mint} (no live or cached snapshot).")
                    continue

                intel = json.loads(old_intel_json)

                # Recalculate age on every cycle, prefer pool creation time
                try:
                    creation_dt = None
                    if creation_time_str := intel.get("created_at_pool"):
                        creation_dt = datetime.fromisoformat(str(creation_time_str).replace("Z", "+00:00"))
                    elif creation_time_str := intel.get("created_at"):
                        creation_dt = datetime.fromisoformat(str(creation_time_str).replace("Z", "+00:00"))
                    else:
                        # fallback to DB discovery time
                        row = await _execute_db("SELECT discovered_at FROM TokenLog WHERE mint_address=?", (mint,), fetch='one')
                        if row and row[0]:
                            creation_dt = datetime.fromisoformat(row[0]).replace(tzinfo=timezone.utc)
                    if creation_dt:
                        intel["age_minutes"] = (datetime.now(timezone.utc) - creation_dt).total_seconds() / 60
                except Exception:
                    pass

                intel.update(result)
                # If live result has pair_created_ms or pool_created_at, normalize into created_at_pool for tagging
                try:
                    pool_dt = None
                    if (ms := result.get("pair_created_ms")):
                        pool_dt = datetime.fromtimestamp(int(ms) / 1000, tz=timezone.utc)
                    elif (iso := result.get("pool_created_at")):
                        pool_dt = datetime.fromisoformat(str(iso).replace("Z", "+00:00"))
                    if pool_dt:
                        intel["created_at_pool"] = pool_dt.isoformat()
                        intel["age_minutes"] = (datetime.now(timezone.utc) - pool_dt).total_seconds() / 60
                except Exception:
                    pass

                intel["mms_score"] = _compute_mms(intel)
                intel["score"] = _compute_score(intel)

                # Apply updates one token at a time to avoid DB lock storms
                await upsert_token_intel(mint, intel)
                await update_token_tags(mint, intel)
                # Save a lightweight snapshot for quick fallbacks
                await save_snapshot(mint, intel)
            log.info("🤖 Re-Analyzer: Cycle complete.")
        except Exception as e:
            log.error(f"🤖 Re-Analyzer: Error during cycle: {e}")

async def second_chance_worker():
    """Periodically re-evaluates a few 'rejected' tokens to see if they've improved."""
    """Periodically re-evaluates a few 'rejected' tokens to see if they've improved."""
    log.info("🧐 Second Chance Protocol: Engaging the scrap heap scanner.")
    await asyncio.sleep(120) # Initial delay
    while True:
        try:
            # Look at a small, random sample of rejected tokens
            rows = await _execute_db("SELECT mint_address FROM TokenLog WHERE status = 'rejected' ORDER BY RANDOM() LIMIT 5", fetch='all')
            if not rows:
                await asyncio.sleep(3600) # Nothing to do, wait an hour
                continue

            mints_to_recheck = [row[0] for row in rows]
            log.info(f"🧐 Second Chance: Re-evaluating {len(mints_to_recheck)} rejected tokens.")

            async with httpx.AsyncClient(http2=True) as client:
                for mint in mints_to_recheck:
                    # Just do a quick, cheap market data check
                    market_data = await fetch_birdeye(client, mint)
                    # Normalize BirdEye result into unified keys if present
                    if market_data and isinstance(market_data.get("data"), dict):
                        be = market_data["data"]
                        market_data = {
                            "liquidity_usd": float(be.get("liquidity", 0.0)),
                            "market_cap_usd": float(be.get("mc", 0.0)),
                            "volume_24h_usd": float(be.get("v24h", 0.0)),
                            "price_change_24h": float(be.get("priceChange24h", 0.0)),
                        }
                    if not market_data:
                        market_data = await fetch_dexscreener_by_mint(client, mint)
                    
                    if market_data and float(market_data.get("liquidity_usd", 0) or 0) >= CONFIG["MIN_LIQUIDITY_FOR_HATCHING"]:
                        log.info(f"🌟 REDEMPTION: {mint} now has enough liquidity! Moving back to discovery queue.")
                        await _execute_db("UPDATE TokenLog SET status = 'discovered' WHERE mint_address = ?", (mint,), commit=True)
                    await asyncio.sleep(5) # Rate limit ourselves
        except Exception as e:
            log.error(f"🧐 Second Chance Worker: Error during cycle: {e}")
        await asyncio.sleep(3600) # Run once per hour

async def _process_one_initial_token(mint: str):
    """Helper to analyze one discovered token, wait, and update its status."""
    log.info(f"🧐 Initial Analyzer: Processing {mint}. Waiting for APIs to index...")
    # Indexing delay is handled upstream by selecting only sufficiently old
    # discovered rows in the processing query. Avoid per-token sleeps here.

    try:
        async with httpx.AsyncClient(http2=True) as client:
            log.info(f"🧐 Initial Analyzer: Wait over. Starting enrichment for {mint}.")
            intel = await enrich_token_intel(client, mint, deep_dive=False)

            if not intel:
                await _execute_db("UPDATE TokenLog SET status = 'rejected' WHERE mint_address = ?", (mint,), commit=True)
                log.info(f"REJECTED: {mint} - Failed enrichment (no data).")
            else:
                await upsert_token_intel(mint, intel)
                await update_token_tags(mint, intel)
                if any(intel.get(k) is not None for k in ("liquidity_usd", "volume_24h_usd", "market_cap_usd")):
                    await save_snapshot(mint, intel)
                log.info(f"✅ ADDED TO POT: {intel.get('symbol', mint)} (Score: {intel.get('score')}, Liq: ${intel.get('liquidity_usd', 0):,.2f})")
    except Exception as e:
        log.error(f"🧐 Initial Analyzer: Error processing {mint}: {e}. Marking as rejected.")
        await _execute_db("UPDATE TokenLog SET status = 'rejected' WHERE mint_address = ?", (mint,), commit=True)

async def process_discovery_queue():
    """Enhanced processing worker with adaptive batching. Replaces initial_analyzer_worker."""
    global adaptive_batch_size
    log.info("🚀 Blueprint Engine: Adaptive intake worker is online.")
    await asyncio.sleep(15) # Initial delay

    while True:
        try:
            start_time = time.time()

            # Calculate adaptive batch size
            if CONFIG["ADAPTIVE_BATCH_SIZE"] and len(recent_processing_times) >= 5:
                avg_time = statistics.mean(recent_processing_times)
                target_time = CONFIG["TARGET_PROCESSING_TIME"]

                if avg_time < target_time * 0.7:
                    adaptive_batch_size = min(adaptive_batch_size + 2, CONFIG["MAX_BATCH_SIZE"])
                elif avg_time > target_time * 1.3:
                    adaptive_batch_size = max(adaptive_batch_size - 1, CONFIG["MIN_BATCH_SIZE"])

            # Gate selection on discovered_at to let indexers catch up, instead of sleeping per token
            idx_wait = int(CONFIG.get("INDEXING_WAIT_SECONDS", 60) or 60)
            rows = await _execute_db(
                "SELECT mint_address FROM TokenLog WHERE status = 'discovered' AND discovered_at <= datetime('now', ?) ORDER BY discovered_at ASC LIMIT ?",
                (f"-{idx_wait} seconds", adaptive_batch_size), fetch='all'
            )

            if not rows:
                await asyncio.sleep(30)
                continue

            mints_to_process = [row[0] for row in rows]
            log.info(f"Enhanced Processor: Found {len(mints_to_process)} new tokens (age≥{idx_wait}s). Processing batch...")

            # Cap concurrency to avoid bursts against APIs and DB
            conc = int(CONFIG.get("INITIAL_ANALYSIS_CONCURRENCY", 8) or 8)
            sem = asyncio.Semaphore(max(1, conc))
            async def _run(m: str):
                async with sem:
                    await _process_one_initial_token(m)
            await asyncio.gather(*[_run(m) for m in mints_to_process])
            
            processing_time = time.time() - start_time
            recent_processing_times.append(processing_time)
            log.info(f"📊 Processed {len(mints_to_process)} tokens in {processing_time:.2f}s (batch size: {adaptive_batch_size})")
        except Exception as e:
            log.error(f"Enhanced processing queue error: {e}", exc_info=True)
            await asyncio.sleep(60)

# ======================================================================================
# Block 7: Scoring, Quips & Reporting
# ======================================================================================

# === Database maintenance & owner commands ===

async def _db_prune(retain_snap_days: int, retain_rejected_days: int) -> int:
    removed = 0
    try:
        # Remove old snapshots
        await _execute_db(
            "DELETE FROM TokenSnapshots WHERE snapshot_time < datetime('now', ?)",
            (f"-{retain_snap_days} days",), commit=True
        )
        # Remove old rejected tokens
        await _execute_db(
            "DELETE FROM TokenLog WHERE status='rejected' AND (last_analyzed_time IS NULL OR last_analyzed_time < datetime('now', ?))",
            (f"-{retain_rejected_days} days",), commit=True
        )
        # VACUUM
        await _execute_db("VACUUM", commit=True)
        removed = 1
    except Exception as e:
        log.error(f"DB prune failed: {e}")
    return removed

async def _db_purge_all() -> None:
    try:
        await _execute_db("DELETE FROM TokenSnapshots", commit=True)
        await _execute_db("DELETE FROM TokenLog", commit=True)
        await _execute_db("VACUUM", commit=True)
        # reset marker
        try:
            Path(DB_MARKER_FILE).write_text(datetime.now(timezone.utc).isoformat(), encoding='utf-8')
        except Exception:
            pass
    except Exception as e:
        log.error(f"DB purge failed: {e}")

async def maintenance_worker():
    log.info("🧹 Maintenance Protocol: Tony's cleaning crew is on the clock.")
    # Initialize DB marker if missing
    while True:
        try:
            await _db_prune(CONFIG["SNAPSHOT_RETENTION_DAYS"], CONFIG["REJECTED_RETENTION_DAYS"])
            # Drop very old 'discovered' rows to prevent backlog bloat
            try:
                hrs = int(CONFIG.get("DISCOVERED_RETENTION_HOURS", 0) or 0)
                if hrs > 0:
                    await _execute_db(
                        "DELETE FROM TokenLog WHERE status='discovered' AND discovered_at < datetime('now', ?)",
                        (f"-{hrs} hours",), commit=True
                    )
            except Exception:
                pass
            # Optional full purge by age
            if (CONFIG.get("FULL_PURGE_INTERVAL_DAYS") or 0) > 0:
                try:
                    row = await _execute_db("SELECT value FROM KeyValueStore WHERE key = 'last_purge_time'", fetch='one')
                    if row and row[0]:
                        dt = datetime.fromisoformat(row[0])
                        age_days = (datetime.now(timezone.utc) - dt).days
                        if age_days >= int(CONFIG["FULL_PURGE_INTERVAL_DAYS"]):
                            log.warning("DB age exceeded FULL_PURGE_INTERVAL_DAYS. Purging all state.")
                            await _db_purge_all()
                            await _execute_db("INSERT OR REPLACE INTO KeyValueStore (key, value) VALUES (?, ?)", ('last_purge_time', datetime.now(timezone.utc).isoformat()), commit=True)
                except Exception:
                    pass
        except Exception as e:
            log.error(f"Maintenance cycle error: {e}")
        await asyncio.sleep(max(3600, int(CONFIG["MAINTENANCE_INTERVAL_HOURS"]) * 3600))

"""Deprecated report/grade helpers (replaced by *_label/*2 variants) removed for clarity."""

# --- Circuit breaker reset worker ---
async def circuit_breaker_reset_worker():
    """Periodically relax provider circuit breakers and decay failure counts.
    This allows temporarily failing providers to recover without hammering them.
    """
    while True:
        try:
            for name, stats in API_PROVIDERS.items():
                # Light decay of failure count; keep success as-is
                fail = stats.get('failure', 0)
                stats['failure'] = max(0, int(fail * 0.8))
                # If circuit is open, probe by closing it after cooldown window
                if stats.get('circuit_open'):
                    if stats['failure'] < 10:
                        stats['circuit_open'] = False
                        log.info(f"Circuit reset for provider {name}.")
        except Exception:
            pass
        await asyncio.sleep(120)

# Removed old _confidence_bar in favor of _confidence_bar2

# ======================================================================================
# Block 8: Telegram Handlers & Main Application
# ======================================================================================

async def _safe_is_group(u: Update) -> bool:
    try:
        t = (u.effective_chat.type or "").lower()
        return t in {"group", "supergroup"}
    except Exception:
        return False

async def _maybe_send_typing(u: Update) -> None:
    """Attempt to send 'typing' chat action where supported. No-ops for channels."""
    try:
        chat = u.effective_chat
        ctype = (chat.type or "").lower()
        if ctype in {"private", "group", "supergroup"}:
            await chat.send_action(action=ChatAction.TYPING)
    except Exception:
        # Silently ignore unsupported or transient errors
        pass

async def safe_reply_text(u: Update, text: str, **kwargs):
    bot = u.get_bot()
    chat_id = u.effective_chat.id
    chat_type = (getattr(u.effective_chat, 'type', '') or '').lower()
    # Map PTB's reply convenience to API fields
    if kwargs.get("quote"):
        kwargs["reply_to_message_id"] = getattr(getattr(u, "effective_message", None), "message_id", None)
        kwargs.pop("quote", None)
    # Channels don't support reply keyboards; drop reply_markup to avoid 400s
    if chat_type == 'channel' and 'reply_markup' in kwargs:
        kwargs.pop('reply_markup', None)
    return await OUTBOX.send_text(bot, chat_id, text, is_group=await _safe_is_group(u), **kwargs)

async def safe_reply_photo(u: Update, photo: bytes, **kwargs):
    bot = u.get_bot()
    chat_id = u.effective_chat.id
    if kwargs.get("quote"):
        kwargs["reply_to_message_id"] = getattr(getattr(u, "effective_message", None), "message_id", None)
        kwargs.pop("quote", None)
    return await OUTBOX.send_photo(bot, chat_id, photo, is_group=await _safe_is_group(u), **kwargs)

# create_links_keyboard removed; use action_row() instead

# ======================================================================================
# Block 9: Scheduled Pushes (Public/VIP) using cache-only reads
# ======================================================================================

SEGMENT_TO_TAG = {
    'fresh': 'is_fresh_candidate',
    'hatching': 'is_hatching_candidate',
    'cooking': 'is_cooking_candidate',
}

async def _select_items_for_segment(segment: str, cooldown: set) -> List[Dict[str, Any]]:
    seg = segment.lower().strip()
    if seg in SEGMENT_TO_TAG:
        tag = SEGMENT_TO_TAG[seg]
        # Use per-command floors if present
        if seg == 'fresh':
            min_score = CONFIG.get("FRESH_MIN_SCORE_TO_SHOW", CONFIG['MIN_SCORE_TO_SHOW'])
            limit = CONFIG.get("FRESH_COMMAND_LIMIT", 2)
        elif seg == 'hatching':
            min_score = CONFIG.get("HATCHING_MIN_SCORE_TO_SHOW", 0)
            limit = CONFIG.get("HATCHING_COMMAND_LIMIT", 2)
        else:
            min_score = CONFIG['MIN_SCORE_TO_SHOW']
            limit = CONFIG.get("COOKING_COMMAND_LIMIT", 2)
        items = await get_reports_by_tag(tag, int(limit), cooldown, min_score=int(min_score))
        # Fallback like /fresh and /hatching commands if tags are empty
        if not items and seg == 'fresh':
            exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
            query = f"""
                SELECT intel_json FROM TokenLog
                WHERE status IN ('analyzed','served')
                AND final_score >= {CONFIG.get('FRESH_MIN_SCORE_TO_SHOW', CONFIG['MIN_SCORE_TO_SHOW'])}
                AND (age_minutes IS NULL OR age_minutes < 1440)
                AND mint_address NOT IN ({exclude_placeholders})
                ORDER BY last_analyzed_time DESC, final_score DESC
                LIMIT ?
            """
            params = (*cooldown, int(limit))
            rows = await _execute_db(query, params, fetch='all')
            items = [json.loads(row[0]) for row in rows] if rows else []
        if not items and seg == 'cooking':
            # Fallback: pick high-volume tokens by joining the latest snapshot per mint
            exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
            min_vol = float(CONFIG.get('COOKING_FALLBACK_VOLUME_MIN_USD', 1000) or 1000)
            query = f"""
                WITH latest AS (
                    SELECT mint_address, MAX(snapshot_time) AS snapshot_time
                    FROM TokenSnapshots
                    GROUP BY mint_address
                )
                SELECT TL.intel_json
                FROM TokenLog TL
                JOIN latest L ON L.mint_address = TL.mint_address
                JOIN TokenSnapshots TS ON TS.mint_address = L.mint_address AND TS.snapshot_time = L.snapshot_time
                WHERE TL.status IN ('analyzed','served')
                  AND TL.mint_address NOT IN ({exclude_placeholders})
                  AND COALESCE(TS.volume_24h_usd, 0) >= ?
                ORDER BY TS.snapshot_time DESC, COALESCE(TS.volume_24h_usd, 0) DESC
                LIMIT ?
            """
            params = (*cooldown, float(min_vol), int(limit)) if cooldown else (float(min_vol), int(limit))
            rows = await _execute_db(query, params, fetch='all')
            items = [json.loads(row[0]) for row in rows] if rows else []
        if not items and seg == 'cooking':
            # Tertiary fallback: recent analyzed sorted by in-intel 24h price change
            exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
            query = f"""
                SELECT intel_json FROM TokenLog
                WHERE status IN ('analyzed','served')
                  AND mint_address NOT IN ({exclude_placeholders})
                ORDER BY last_analyzed_time DESC
                LIMIT 50
            """
            params = (*cooldown,) if cooldown else ()
            rows = await _execute_db(query, params, fetch='all')
            if rows:
                pool = [json.loads(r[0]) for r in rows]
                pool.sort(key=lambda x: float(x.get('price_change_24h') or 0), reverse=True)
                items = pool[:int(limit)]
        if not items and seg == 'hatching':
            exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
            age_limit = int(CONFIG.get('HATCHING_MAX_AGE_MINUTES', 30))
            query = f"""
                SELECT intel_json FROM TokenLog
                WHERE status IN ('analyzed','served')
                AND (age_minutes IS NULL OR age_minutes <= {age_limit})
                AND final_score >= {CONFIG.get('HATCHING_MIN_SCORE_TO_SHOW', 0)}
                AND mint_address NOT IN ({exclude_placeholders})
                ORDER BY last_analyzed_time DESC
                LIMIT ?
            """
            params = (*cooldown, int(limit))
            rows = await _execute_db(query, params, fetch='all')
            items = [json.loads(row[0]) for row in rows] if rows else []
        return items

    if seg == 'top':
        # Top by final_score, then recent first
        limit = int(CONFIG.get("TOP_COMMAND_LIMIT", 2))
        exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND mint_address NOT IN ({exclude_placeholders})
            AND final_score >= {CONFIG['MIN_SCORE_TO_SHOW']}
            ORDER BY final_score DESC, last_analyzed_time DESC
            LIMIT ?
        """
        params = (*cooldown, limit)
        rows = await _execute_db(query, params, fetch='all')
        return [json.loads(r[0]) for r in rows] if rows else []

    return []

async def _prepare_segment_text_from_cache(segment: str) -> Tuple[Optional[str], List[str]]:
    """Builds the segment text without triggering live HTTP calls.
    Returns (text, minted_ids_served). Adds 'Lite Mode' when cache is stale or circuit breaker is active.
    """
    cooldown_hours = int(CONFIG.get("PUSH_COOLDOWN_HOURS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours) 
    items = await _select_items_for_segment(segment, cooldown)
    if not items:
        # Provide a compact nothing-found message per segment
        empty_lines = {
            'fresh': "– Reservoir’s dry, Tony. No top-tier fresh signals right now. ⏱️",
            'hatching': "🦉 Token's nest is empty. No brand-new, structurally sound tokens right now.",
            'cooking': "🍳 Stove's cold. Nothing showing significant momentum right now.",
            'top': "– Nothin' but crickets. The pot's a bit thin right now, check back later. 🦗",
        }
        return empty_lines.get(segment, "Nothing to show right now."), []

    # Determine Lite Mode: if circuit breaker tripped OR snapshots stale
    lite_mode = False
    try:
        if LITE_MODE_UNTIL and LITE_MODE_UNTIL > time.time():
            lite_mode = True
        else:
            snaps = await asyncio.gather(*[load_latest_snapshot(i.get('mint')) for i in items], return_exceptions=True)
            staleness = int(CONFIG.get("SNAPSHOT_STALENESS_SECONDS", 600) or 600)
            for s in snaps:
                if isinstance(s, dict):
                    if (s.get('snapshot_age_sec') or 1e9) > staleness:
                        lite_mode = True
                        break
                else:
                    # No snapshot available => treat as lite
                    lite_mode = True
                    break
    except Exception:
        pass

    header = pick_header_label(f"/{segment}")
    if lite_mode:
        header = f"{header} — ⚡ Lite Mode"
    text_body = build_compact_report3(items[: int(CONFIG.get(f"{segment.upper()}_COMMAND_LIMIT", 2))], include_links=True)
    final = header + "\n\n" + text_body
    served = [i.get('mint') for i in items[: int(CONFIG.get(f"{segment.upper()}_COMMAND_LIMIT", 2))] if i.get('mint')]
    return final, served

async def push_segment_to_chat(app: Application, chat_id: int, segment: str) -> None:
    """Edit the existing segment message in a chat or send a new one if missing."""
    try:
        text, served = await _prepare_segment_text_from_cache(segment)
        if not text:
            return
        mid = await get_push_message_id(chat_id, segment)
        # Try to edit first
        if mid:
            try:
                # Apply basic gating to avoid pool bursts before editing
                try:
                    await OUTBOX.global_bucket.acquire(1)
                    if int(chat_id) < 0:
                        await (await OUTBOX._group_bucket(int(chat_id))).acquire(1)
                    await (await OUTBOX._chat_bucket(int(chat_id))).acquire(1)
                except Exception:
                    pass
                await app.bot.edit_message_text(chat_id=chat_id, message_id=mid, text=text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            except Exception as e:
                msg = str(e)
                if "message to edit not found" in msg.lower() or "message_id" in msg.lower():
                    mid = None  # fall through to send new
                elif "message is not modified" in msg.lower():
                    pass
                else:
                    # Unexpected edit error — try sending a fresh message
                    mid = None
        if not mid:
            # Use gated outbox to avoid saturating Telegram HTTP pool
            try:
                is_group = True if int(chat_id) < 0 else False
            except Exception:
                is_group = True
            sent = await OUTBOX.send_text(app.bot, int(chat_id), text, is_group=is_group, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
            try:
                mid_new = int(getattr(sent, 'message_id', 0)) if sent is not None else 0
            except Exception:
                mid_new = 0
            if mid_new:
                await set_push_message_id(chat_id, segment, mid_new)
                log.info(f"PUSH OK: segment={segment} chat={chat_id} mid={mid_new}")
            else:
                log.warning(f"PUSH SENT WITHOUT MESSAGE_ID: segment={segment} chat={chat_id}")
        if served:
            await mark_as_served(served)
    except Exception as e:
        msg = str(e)
        log.error(f"Scheduled push failed for segment={segment} chat={chat_id}: {e}")
        # Notify owner once per chat when permissions/membership issues are detected
        lower = msg.lower()
        if any(s in lower for s in [
            "need administrator rights",
            "chat not found",
            "bot was blocked",
            "not enough rights",
            "forbidden"
        ]):
            key = f"perm:{chat_id}"
            try:
                if not CHANNEL_ALERT_SENT.get(key):
                    CHANNEL_ALERT_SENT[key] = True
                    await _notify_owner(app.bot, (
                        f"<b>Auto-push failed</b> for chat <code>{chat_id}</code> (segment <code>{segment}</code>):\n"
                        f"{_html.escape(msg)}\n\n"
                        "Fix: Add the bot as <b>Admin</b> in that channel, then run /setpublic or /setvip in the target chat to reschedule."
                    ))
            except Exception:
                pass

async def scheduled_push_job(context: ContextTypes.DEFAULT_TYPE):
    data = context.job.data or {}
    seg = data.get('segment')
    chat_id = data.get('chat_id')
    if not seg or not chat_id:
        return
    await push_segment_to_chat(context.application, int(chat_id), str(seg))
async def get_reports_by_tag(tag_name: str, limit: int, exclude_mints: set, *, min_score: Optional[int] = None) -> List[Dict]:
    """Generic function to get reports based on a boolean candidate tag.
    Allows overriding the minimum score threshold per use-case.
    """
    if min_score is None:
        min_score = CONFIG['MIN_SCORE_TO_SHOW']
    exclude_placeholders = ','.join('?' for _ in exclude_mints) if exclude_mints else "''"
    extra_age_filter = " AND age_minutes < 1440" if tag_name == "is_fresh_candidate" else ""
    query = f"""
        SELECT intel_json FROM TokenLog
        WHERE {tag_name} = 1
        AND mint_address NOT IN ({exclude_placeholders})
        AND COALESCE(final_score, 0) >= {int(min_score)}{extra_age_filter}
        ORDER BY final_score DESC LIMIT ?
    """
    params = (*exclude_mints, limit)
    rows = await _execute_db(query, params, fetch='all')
    return [json.loads(row[0]) for row in rows] if rows else []

async def _refresh_reports_with_latest(items: List[Dict], *, allow_missing: bool = False) -> List[Dict]:
    """Fetch a quick market snapshot for each item and recompute scores right before sending.
    Keeps per-command results up-to-date and reduces stale extremes.
    """
    if not items:
        return items
    try:
        async with httpx.AsyncClient(http2=True) as client:
            tasks_snap = [fetch_market_snapshot(client, i.get("mint")) for i in items]
            # Only fetch top10 if currently missing, to keep it light
            need_top10 = [i.get("top10_holder_percentage") is None for i in items]
            tasks_top10 = [fetch_top10_via_rpc(client, i.get("mint")) if need else None for i, need in zip(items, need_top10)]
            # Jupiter route checks to prevent showing stale non-tradable pools as liquid
            tasks_jup = [fetch_jupiter_has_route(client, i.get("mint")) for i in items]
            # DB fallback snapshots in parallel
            tasks_last = [load_latest_snapshot(i.get("mint")) for i in items]
            snaps = await asyncio.gather(*tasks_snap, return_exceptions=True)
            jup_routes = await asyncio.gather(*tasks_jup, return_exceptions=True)
            lasts = await asyncio.gather(*tasks_last, return_exceptions=True)
            top10_results = [None] * len(items)
            for idx, t in enumerate(tasks_top10):
                if t is None:
                    continue
                try:
                    top10_results[idx] = await t
                except Exception:
                    top10_results[idx] = None
        out = []
        now = datetime.now(timezone.utc)
        snapshot_tasks: List[asyncio.Task] = []
        for idx, (i, snap) in enumerate(zip(items, snaps)):
            j = dict(i)
            live_ok = False
            if not isinstance(snap, Exception) and snap:
                j.update(snap)
                live_ok = True
                # Recompute age if we stored pool_created_at
                created_iso = j.get("pool_created_at") or j.get("created_at_pool") or j.get("created_at")
                try:
                    if created_iso:
                        dt = datetime.fromisoformat(str(created_iso).replace("Z", "+00:00"))
                        j["age_minutes"] = (now - dt).total_seconds() / 60
                except Exception:
                    pass
            else:
                # Fallback to recent snapshot if within staleness window
                try:
                    ls = lasts[idx]
                except Exception:
                    ls = None
                if isinstance(ls, dict) and (ls.get("snapshot_age_sec") or 1e9) <= CONFIG.get("SNAPSHOT_STALENESS_SECONDS", 600):
                    for k in ("liquidity_usd", "volume_24h_usd", "market_cap_usd"):
                        if k in ls:
                            j[k] = ls[k]
                else:
                    # No live data and snapshot too old; if allowed, keep item as-is (unknown market), else drop
                    if not allow_missing:
                        continue
            # If Jupiter reports no route, force liquidity/volume to zero to avoid stale displays
            try:
                jup_ok = jup_routes[idx]
                if jup_ok is False:
                    # For very young tokens, Jupiter may not have a route yet; avoid false negatives
                    young_grace_min = int(CONFIG.get("JUP_CLAMP_MIN_AGE_MINUTES", 60))
                    try:
                        age_m = float(j.get("age_minutes")) if j.get("age_minutes") is not None else None
                    except Exception:
                        age_m = None
                    # Clamp only if we have an established age beyond grace
                    if age_m is not None and age_m > young_grace_min:
                        liq_raw = j.get("liquidity_usd")
                        liq_now = None
                        if isinstance(liq_raw, (int, float)):
                            try:
                                liq_now = float(liq_raw)
                            except Exception:
                                liq_now = None
                        if liq_now is not None and liq_now < 50.0:
                            j["liquidity_usd"] = 0.0
                            j["volume_24h_usd"] = min(float(j.get("volume_24h_usd") or 0.0), 0.0)
            except Exception:
                pass
            # Fill top10 concentration if missing and we fetched it
            if j.get("top10_holder_percentage") is None:
                t = top10_results[idx]
                if isinstance(t, dict):
                    j.update(t)
            j["sss_score"] = _compute_sss(j)
            j["mms_score"] = _compute_mms(j)
            j["score"] = _compute_score(j)
            j["score_confidence"] = _score_confidence(j)
            # Persist snapshot only when we had live data
            if live_ok:
                try:
                    snapshot_tasks.append(asyncio.create_task(save_snapshot(j.get("mint"), j)))
                except Exception:
                    pass
            out.append(j)
        if snapshot_tasks:
            try:
                await asyncio.gather(*snapshot_tasks)
            except Exception:
                pass
        return out
    except Exception:
        return items

def _filter_items_for_command(items: List[Dict], cmd: str) -> List[Dict]:
    """Filter list items before sending to users.
    Rule: Exclude explicit zero/negative-liquidity for all commands. Allow unknown liquidity everywhere,
    with the special case that '/hatching' is the only place we may send items with unknown liq (explicit 0 still excluded).
    """
    out: List[Dict] = []
    for j in items or []:
        liq_val = j.get("liquidity_usd", None)
        try:
            if liq_val is None:
                # Unknown liquidity is allowed (policy focuses on avoiding explicit zero)
                out.append(j)
                continue
            liq = float(liq_val)
        except Exception:
            # Treat unparsable liquidity as unknown; allow
            out.append(j)
            continue
        if liq <= 0.0:
            # Allow explicit zero-liquidity for very young tokens in /fresh only (grace window)
            if cmd == '/fresh':
                try:
                    age_m = float(j.get('age_minutes')) if j.get('age_minutes') is not None else None
                except Exception:
                    age_m = None
                if age_m is not None and age_m < float(CONFIG.get('FRESH_ZERO_LIQ_AGE_MINUTES', 15) or 15):
                    out.append(j)
                    continue
            # Otherwise exclude
            continue
        out.append(j)
    return out

async def fetch_dexscreener_chart(pair_address: str) -> Optional[bytes]:
    if not pair_address: return None
    url = f"https://io.dexscreener.com/u/chart/pairs/solana/{pair_address}"
    try:
        browser_headers = {
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
            "Referer": "https://dexscreener.com/",
            "Origin": "https://dexscreener.com",
        }
        async with httpx.AsyncClient(http2=False) as img_client:
            r = await img_client.get(url, timeout=15.0, headers=browser_headers, follow_redirects=True)
            r.raise_for_status()
            return r.content
    except Exception as e:
        log.info(f"Chart image unavailable from {url}: {e}")
        return None

async def start(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await safe_reply_text(u, "👋 I’m Token Tony. Let’s get to work. /fresh, /hatching, /cooking, /top, /check <mint>, /diag.")

async def diag(u: Update, c: ContextTypes.DEFAULT_TYPE):
    # Enhanced diagnostics
    db_counts_rows = await _execute_db(
        "SELECT status, COUNT(*) FROM TokenLog GROUP BY status", fetch='all'
    )
    db_counts = {r[0]: r[1] for r in db_counts_rows} if db_counts_rows else {}

    bucket_rows = await _execute_db(
        "SELECT enhanced_bucket, COUNT(*) FROM TokenLog WHERE status IN ('analyzed','served') GROUP BY enhanced_bucket", fetch='all'
    )
    bucket_counts = {r[0]: r[1] for r in bucket_rows} if bucket_rows else {}

    firehose_statuses = [f" - {name}: {status}" for name, status in FIREHOSE_STATUS.items()]
    provider_status = []
    for provider, stats in API_PROVIDERS.items():
        total = stats['success'] + stats['failure']
        if total > 0:
            success_rate = stats['success'] / total
            status = "⚠️ Circuit Open" if stats['circuit_open'] else "✅ Operational"
            provider_status.append(f" - {provider.title()}: {status} ({success_rate:.1%} success)")
        else:
            provider_status.append(f" - {provider.title()}: Idle")

    # JobQueue status (what's scheduled)
    try:
        jobs = c.application.job_queue.jobs() if hasattr(c.application.job_queue, 'jobs') else []
        job_lines = [f" - {j.name}" for j in jobs] if jobs else [" - (no jobs scheduled)"]
    except Exception:
        job_lines = [" - (unable to enumerate jobs)"]

    diag_msgs = [
        "<b>🩺 Token Tony's System Status:</b>",
        "<b>Performance Metrics:</b>",
        f" - Adaptive Batch Size: {adaptive_batch_size}",
        f" - Avg. Proc. Time: {statistics.mean(recent_processing_times) if recent_processing_times else 0:.2f}s",
        "<b>Database State:</b>",
        f" - PumpPortal WS: {PUMPFUN_STATUS}",
        f" - DB (In Pot): {db_counts.get('analyzed', 0)}",
        f" - DB (In Queue): {db_counts.get('discovered', 0)}",
        f" - DB (Rejected): {db_counts.get('rejected', 0)}",
        "<b>Enhanced Bucket Distribution:</b>",
        *[f" - {k.title() if k else 'Unassigned'}: {v}" for k, v in sorted(bucket_counts.items())],
        "<b>Live Discovery Sources:</b>",
        "\n".join(firehose_statuses),
        "<b>API Service Status</b>",
        *provider_status,
        f" - Helius RPC (Specialist): {'✅ Configured' if HELIUS_API_KEY else '⚠️ Not Configured'}",
        f" - Birdeye API (Market Data): {'✅ Configured' if BIRDEYE_API_KEY else '❌ NOT CONFIGURED'}",
        "<b>Push Scheduling</b>",
        f" - PUBLIC_CHAT_ID: {PUBLIC_CHAT_ID if PUBLIC_CHAT_ID else 'not set'}",
        f" - VIP_CHAT_ID: {VIP_CHAT_ID if VIP_CHAT_ID else 'not set'}",
        f" - Fresh test interval: {CONFIG.get('FRESH_TEST_INTERVAL_SECONDS', 0)}s",
        "Jobs:",
        "\n".join(job_lines),
    ]
    await safe_reply_text(u, "\n".join(diag_msgs), parse_mode=ParseMode.HTML, disable_web_page_preview=True)

async def ping(u: Update, c: ContextTypes.DEFAULT_TYPE):
    # simple liveness check for owner testing
    await safe_reply_text(u, "PONG 🟢")

ALLOWED_RUNTIME_KEYS = {
    "MIN_LIQUIDITY_FOR_HATCHING",
    "HATCHING_MAX_AGE_MINUTES",
    "RE_ANALYZER_INTERVAL_MINUTES",
    "SNAPSHOT_STALENESS_SECONDS",
    "FRESH_COMMAND_LIMIT",
    "HATCHING_COMMAND_LIMIT",
    "COOKING_COMMAND_LIMIT",
    "TOP_COMMAND_LIMIT",
    "JUP_SLIPPAGE_BPS",
    "FRESH_TEST_INTERVAL_SECONDS",
}

async def set_config(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if not getattr(u, 'effective_user', None) or getattr(u.effective_user, 'id', None) != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    parts = text.split(maxsplit=2)
    if len(parts) < 3:
        return await safe_reply_text(u, "Usage: /set <KEY> <VALUE>")
    _, key, val = parts
    key = key.strip().upper()
    if key not in ALLOWED_RUNTIME_KEYS:
        return await safe_reply_text(u, f"Unsupported key. Allowed: {', '.join(sorted(ALLOWED_RUNTIME_KEYS))}")
    new_val = _parse_typed_value(val)
    CONFIG[key] = new_val
    await safe_reply_text(u, f"Set {key} = {new_val}")

async def _schedule_pushes(c: ContextTypes.DEFAULT_TYPE, chat_id: int, chat_type: str):
    """Schedules the push notifications for a given chat."""
    jq = c.application.job_queue
    # Cancel prior jobs
    try:
        for name in (f"{chat_type}_hatching", f"{chat_type}_cooking", f"{chat_type}_top", f"{chat_type}_fresh"):
            for j in jq.get_jobs_by_name(name):
                j.schedule_removal()
    except Exception:
        pass

    # Recreate schedules
    if chat_id:
        prefix = chat_type
        # Standardize both public and vip per your 60s spec
        jq.run_repeating(scheduled_push_job, interval=5 * 60, first=5.0, name=f"{prefix}_hatching", data={"chat_id": chat_id, "segment": "hatching"})
        jq.run_repeating(scheduled_push_job, interval=60, first=7.0, name=f"{prefix}_cooking", data={"chat_id": chat_id, "segment": "cooking"})
        jq.run_repeating(scheduled_push_job, interval=60 * 60, first=9.0, name=f"{prefix}_top", data={"chat_id": chat_id, "segment": "top"})
        jq.run_repeating(scheduled_push_job, interval=60, first=11.0, name=f"{prefix}_fresh", data={"chat_id": chat_id, "segment": "fresh"})

async def setpublic(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Set the current chat as PUBLIC_CHAT_ID and schedule auto-pushes."""
    is_channel = (getattr(getattr(u, 'effective_chat', None), 'type', '') or '').lower() == 'channel'
    if not is_channel and (not getattr(u, 'effective_user', None) or getattr(u.effective_user, 'id', None) != OWNER_ID):
        return await safe_reply_text(u, "Only the boss can do that.")
    global PUBLIC_CHAT_ID
    chat = u.effective_chat
    if not chat:
        return await safe_reply_text(u, "Can't detect chat.")
    PUBLIC_CHAT_ID = int(chat.id)
    await _schedule_pushes(c, PUBLIC_CHAT_ID, "public")
    return await safe_reply_text(u, f"Public auto-pushes scheduled for chat {PUBLIC_CHAT_ID}.")

async def setvip(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Set the current chat as VIP_CHAT_ID and schedule auto-pushes."""
    is_channel = (getattr(getattr(u, 'effective_chat', None), 'type', '') or '').lower() == 'channel'
    if not is_channel and (not getattr(u, 'effective_user', None) or getattr(u.effective_user, 'id', None) != OWNER_ID):
        return await safe_reply_text(u, "Only the boss can do that.")
    global VIP_CHAT_ID
    chat = u.effective_chat
    if not chat:
        return await safe_reply_text(u, "Can't detect chat.")
    VIP_CHAT_ID = int(chat.id)
    await _schedule_pushes(c, VIP_CHAT_ID, "vip")
    return await safe_reply_text(u, f"VIP auto-pushes scheduled for chat {VIP_CHAT_ID}.")

async def push(u: Update, c: ContextTypes.DEFAULT_TYPE):
    is_channel = (getattr(getattr(u, 'effective_chat', None), 'type', '') or '').lower() == 'channel'
    if not is_channel and (not getattr(u, 'effective_user', None) or getattr(u.effective_user, 'id', None) != OWNER_ID):
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    parts = text.split()
    # Expect: /push <segment> [public|vip]
    if len(parts) < 2:
        return await safe_reply_text(u, "Usage: /push <hatching|cooking|top|fresh> [public|vip]")
    segment = parts[1].lower()
    if segment not in {"hatching", "cooking", "top", "fresh"}:
        return await safe_reply_text(u, "Segment must be one of: hatching, cooking, top, fresh")
    dest = parts[2].lower() if len(parts) >= 3 else None
    if dest == "public":
        chat_id = PUBLIC_CHAT_ID
    elif dest == "vip":
        chat_id = VIP_CHAT_ID
    else:
        chat_id = u.effective_chat.id
    if not chat_id:
        return await safe_reply_text(u, "Missing target chat ID. Set PUBLIC_CHAT_ID / VIP_CHAT_ID in env, or run in target chat.")
    await push_segment_to_chat(c.application, int(chat_id), segment)
    await safe_reply_text(u, f"Pushed {segment} to {('public' if chat_id==PUBLIC_CHAT_ID else 'vip' if chat_id==VIP_CHAT_ID else chat_id)}")

async def testpush(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Send a small test message to public/vip/here and return deep link info."""
    bot = u.get_bot()
    text = (u.message.text if getattr(u, 'message', None) else getattr(getattr(u, 'effective_message', None), 'text', '')) or ''
    parts = text.split()
    target = parts[1].lower() if len(parts) > 1 else 'here'
    if target == 'public':
        chat_id = PUBLIC_CHAT_ID
    elif target == 'vip':
        chat_id = VIP_CHAT_ID
    else:
        chat_id = getattr(getattr(u, 'effective_chat', None), 'id', None)
    if not chat_id:
        return await safe_reply_text(u, "No target chat. Usage: /testpush [public|vip|here]")
    # Check rights and username
    ok, reason = await _can_post_to_chat(bot, int(chat_id))
    ch = None
    try:
        ch = await bot.get_chat(int(chat_id))
    except Exception:
        pass
    uname = getattr(ch, 'username', None)
    typ = getattr(ch, 'type', '')
    # Send a tiny test message
    sent = None
    if ok:
        try:
            sent = await OUTBOX.send_text(bot, int(chat_id), "Test push ✅", is_group=(int(chat_id) < 0), parse_mode=ParseMode.HTML, disable_web_page_preview=True)
        except Exception as e:
            await safe_reply_text(u, f"Send failed: {e}")
            return
    else:
        await safe_reply_text(u, f"Cannot post to {chat_id}: {reason}")
        return
    mid = int(getattr(sent, 'message_id', 0)) if sent else 0
    link = f"https://t.me/{uname}/{mid}" if uname and mid else "(no public link)"
    await safe_reply_text(u, f"PUSH OK to {chat_id} (type={typ}) mid={mid}\nLink: {link}")

async def fresh(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await get_reports_by_tag(
        "is_fresh_candidate",
        CONFIG["FRESH_COMMAND_LIMIT"],
        cooldown,
        min_score=CONFIG.get("FRESH_MIN_SCORE_TO_SHOW", CONFIG['MIN_SCORE_TO_SHOW'])
    )
    
    if not reports:
        log.warning("/fresh: Tag search found nothing. Activating Last Resort (ignoring tags).")
        exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND final_score >= {CONFIG.get('FRESH_MIN_SCORE_TO_SHOW', CONFIG['MIN_SCORE_TO_SHOW'])}
            AND (age_minutes IS NULL OR age_minutes < 1440)
            AND mint_address NOT IN ({exclude_placeholders})
            ORDER BY last_analyzed_time DESC, final_score DESC
            LIMIT ?
        """
        params = (*cooldown, CONFIG["FRESH_COMMAND_LIMIT"])
        rows = await _execute_db(query, params, fetch='all')
        if rows:
            reports = [json.loads(row[0]) for row in rows]

    if not reports:
        await safe_reply_text(u, "– Reservoir’s dry, Tony. No top-tier fresh signals right now. ⏱️")
        return

    # Fresh header quips (general scan/guard/tooling vibe)
    header_quips = [
        "🆕 Here’s a batch of fresh ones Tony approved",
        "🆕 These just passed the safety check",
        "🆕 Fresh off the truck — clean and ready",
        "🆕 Tony signed off on this stack",
        "🆕 Couple solid builds right here",
        "🆕 Passed inspection — no rust yet",
        "🆕 Tony’s fridge picks — crisp and clean",
        "🆕 Pulled a fresh set for you",
        "🆕 New kids on the block — safe enough to sniff",
        "🆕 Tony says: these are worth a look",
    ]
    # Refresh market snapshot and recompute scores just-in-time
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/fresh pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/fresh')
    header_line = f"{pick_header_label('/fresh')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No eligible fresh tokens at the moment.")
        return
    text = build_compact_report3(items, include_links=True)
    final_text = header_line + "\n\n" + text
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])

async def hatching(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await get_reports_by_tag(
        "is_hatching_candidate",
        CONFIG["HATCHING_COMMAND_LIMIT"],
        cooldown,
        min_score=CONFIG.get("HATCHING_MIN_SCORE_TO_SHOW", 0)
    )
    if not reports:
        # Last resort: query very young analyzed tokens directly (even if tags weren't set due to earlier failures)
        log.warning("/hatching: Tag search found nothing. Activating Last Resort (age-based scan).")
        exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
        age_limit = int(CONFIG.get('HATCHING_MAX_AGE_MINUTES', 30))
        query = f"""
            SELECT intel_json FROM TokenLog
            WHERE status IN ('analyzed','served')
            AND (age_minutes IS NULL OR age_minutes <= {age_limit})
            AND final_score >= {CONFIG.get('HATCHING_MIN_SCORE_TO_SHOW', 0)}
            AND mint_address NOT IN ({exclude_placeholders})
            ORDER BY last_analyzed_time DESC
            LIMIT ?
        """
        params = (*cooldown, CONFIG["HATCHING_COMMAND_LIMIT"])
        rows = await _execute_db(query, params, fetch='all')
        if rows:
            reports = [json.loads(row[0]) for row in rows]
        if not reports:
            await safe_reply_text(u, "🦉 Token's nest is empty. No brand-new, structurally sound tokens right now.")
            return
        
    # Hatching header quips (newborn/hatch theme)
    header_quips = [
        "🐣 Got a few newborns — just cracked open",
        "🐣 Fresh hatches straight from the nest",
        "🐣 Brand-new drops Tony just spotted",
        "🐣 Token and I pulled these off the line",
        "🐣 Hot from launch — here’s the hatch batch",
        "🐣 New coins in the wild — eyes on ‘em",
        "🐣 Nest is busy — fresh cracks today",
        "🐣 A handful of hatchlings for you",
        "🐣 Straight out the shell — fresh batch",
        "🐣 Don’t blink — Tony’s got hatchers",
    ]
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/hatching pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/hatching')
    header_line = f"{pick_header_label('/hatching')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No hatchlings with tradable liquidity yet.")
        return
    text = build_compact_report3(items, include_links=True)
    final_text = header_line + "\n\n" + text
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])

async def _get_cooking_reports_command(cooldown: set) -> List[Dict[str, Any]]:
    """Collect cooking candidates with graceful fallbacks for the /cooking command.
    Order of precedence:
    1) Tagged candidates (is_cooking_candidate)
    2) Latest snapshots with high 24h volume (CONFIG['COOKING_FALLBACK_VOLUME_MIN_USD'])
    3) Recent analyzed tokens sorted by in-intel price_change_24h
    """
    # Primary: tagged
    items = await get_reports_by_tag("is_cooking_candidate", CONFIG["COOKING_COMMAND_LIMIT"], cooldown)
    if items:
        return items
    # Secondary: snapshot volume
    exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
    min_vol = float(CONFIG.get('COOKING_FALLBACK_VOLUME_MIN_USD', 200) or 200)
    query = f"""
        WITH latest AS (
            SELECT mint_address, MAX(snapshot_time) AS snapshot_time
            FROM TokenSnapshots
            GROUP BY mint_address
        )
        SELECT TL.intel_json
        FROM TokenLog TL
        JOIN latest L ON L.mint_address = TL.mint_address
        JOIN TokenSnapshots TS ON TS.mint_address = L.mint_address AND TS.snapshot_time = L.snapshot_time
        WHERE TL.status IN ('analyzed','served')
          AND TL.mint_address NOT IN ({exclude_placeholders})
          AND COALESCE(TS.volume_24h_usd, 0) >= ?
        ORDER BY TS.snapshot_time DESC, COALESCE(TS.volume_24h_usd, 0) DESC
        LIMIT ?
    """
    params = (*cooldown, float(min_vol), CONFIG["COOKING_COMMAND_LIMIT"]) if cooldown else (float(min_vol), CONFIG["COOKING_COMMAND_LIMIT"])
    rows = await _execute_db(query, params, fetch='all')
    items = [json.loads(row[0]) for row in rows] if rows else []
    if items:
        return items
    # Tertiary: recent analyzed sorted by in-intel price change
    query2 = f"""
        SELECT intel_json FROM TokenLog
        WHERE status IN ('analyzed','served')
          AND mint_address NOT IN ({exclude_placeholders})
        ORDER BY last_analyzed_time DESC
        LIMIT 50
    """
    params2 = (*cooldown,) if cooldown else ()
    rows2 = await _execute_db(query2, params2, fetch='all')
    if not rows2:
        return []
    pool = [json.loads(r[0]) for r in rows2]
    pool.sort(key=lambda x: float(x.get('price_change_24h') or 0), reverse=True)
    return pool[:CONFIG["COOKING_COMMAND_LIMIT"]]

async def cooking(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    reports = await _get_cooking_reports_command(cooldown)
    if not reports:
        await safe_reply_text(u, "🍳 Stove's cold. Nothing showing significant momentum right now.")
        return
    
    # Cooking header quips (heat/cooking theme)
    header_quips = [
        "🍳 Got a few sizzling right now",
        "🍳 These ones are cooking hot",
        "🍳 Momentum’s rising across this batch",
        "🍳 Tony’s grill has a couple popping",
        "🍳 Here’s a pan full of movers",
        "🍳 These drops are smoking fast",
        "🍳 Couple hot picks — handle with mitts",
        "🍳 Tony says: fire under all of these",
        "🍳 The skillet’s crowded — crackling picks",
        "🍳 Burning quick — keep eyes sharp",
    ]
    refreshed = await _refresh_reports_with_latest(reports, allow_missing=True)
    log.info(f"/cooking pipeline: from_tags={len(reports)} after_refresh={len(refreshed)}")
    reports = _filter_items_for_command(refreshed, '/cooking')
    header_line = f"{pick_header_label('/cooking')} — {random.choice(header_quips)}"
    items = reports[:2]
    if not items:
        await safe_reply_text(u, "No eligible cooking tokens after filters.")
        return
    text = build_compact_report3(items, include_links=True)
    final_text = header_line + "\n\n" + text
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])

async def top(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await _maybe_send_typing(u)
    cooldown_hours = int(CONFIG.get("COMMAND_COOLDOWN_HOURS_COMMANDS", CONFIG.get("COMMAND_COOLDOWN_HOURS", 12)) or 12)
    cooldown = await get_recently_served_mints(cooldown_hours)
    
    exclude_placeholders = ','.join('?' for _ in cooldown) if cooldown else "''"
    query = f"""
        SELECT intel_json FROM TokenLog
        WHERE status IN ('analyzed','served')
        AND mint_address NOT IN ({exclude_placeholders})
        AND final_score >= {CONFIG['MIN_SCORE_TO_SHOW']}
        ORDER BY final_score DESC
        LIMIT ?
    """
    params = (*cooldown, CONFIG["TOP_COMMAND_LIMIT"])
    rows = await _execute_db(query, params, fetch='all')
    
    if not rows:
        await safe_reply_text(u, "– Nothin' but crickets. The pot's a bit thin right now, check back later. 🦗")
        return

    # Pull a bit more than we will display to allow post-refresh filtering/sorting
    more_params = (*cooldown, max(CONFIG["TOP_COMMAND_LIMIT"] * 5, CONFIG["TOP_COMMAND_LIMIT"]))
    rows_more = await _execute_db(query, more_params, fetch='all')
    reports = [json.loads(row[0]) for row in (rows_more or rows)]
    # Top header quips (leaderboard theme)
    top_quips = [
        "🏆 Tony’s proud picks — strongest of the bunch",
        "🏆 Here’s today’s winners’ circle",
        "🏆 Top shelf coins — only the best made it",
        "🏆 These few passed every test",
        "🏆 Tony’s shortlist — solid crew",
        "🏆 Couple standouts worth your time",
        "🏆 These are the cream of the crop",
        "🏆 Tony and Token hand-picked these",
        "🏆 Best of today — no slackers",
        "🏆 Tony says: these are built to last",
    ]
    header = f"{pick_header_label('/top')} — {random.choice(top_quips)}"
    refreshed = await _refresh_reports_with_latest(reports)
    log.info(f"/top pipeline: from_db={len(reports)} after_refresh={len(refreshed)}")
    reports = refreshed
    # Filter out obviously rugged/non-tradable and illiquid
    min_liq = float(CONFIG.get("MIN_LIQUIDITY_FOR_HATCHING", 100) or 100)
    filtered = []
    for j in reports:
        liq_raw = j.get("liquidity_usd", None)
        liq = None
        try:
            if liq_raw is not None:
                liq = float(liq_raw)
        except Exception:
            liq = None
        rug_txt = str(j.get("rugcheck_score") or "")
        # Enforce min liquidity only when we have a numeric value; unknown liquidity passes this check
        if liq is not None and liq < min_liq:
            continue
        if "High Risk" in rug_txt:
            continue
        filtered.append(j)
    # Apply global no-zero-liq rule for lists
    filtered = _filter_items_for_command(filtered, '/top')
    # Sort by freshly recomputed score, highest first
    filtered.sort(key=lambda x: int(x.get("score", 0) or 0), reverse=True)
    items = filtered[:CONFIG["TOP_COMMAND_LIMIT"]]
    if not items:
        await safe_reply_text(u, "No eligible top tokens after filters.")
        return
    text = build_compact_report3(items, include_links=True)
    final_text = header + "\n\n" + text
    await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True,
                          reply_markup=ReplyKeyboardRemove())
    await mark_as_served([i.get("mint") for i in items if i.get("mint")])

async def check(u: Update, c: ContextTypes.DEFAULT_TYPE):
    # Robustly extract text from any update type (DM, group, channel)
    try:
        text = (getattr(getattr(u, 'effective_message', None), 'text', '') or '').strip()
    except Exception:
        text = ''
    # Encourage DM-only deep checks to avoid exposing details in groups
    try:
        if await _safe_is_group(u) and u.effective_user.id != OWNER_ID:
            return await safe_reply_text(u, "For privacy, run /check in DM with me.")
    except Exception:
        pass
    # Ensure any old ReplyKeyboard is removed (Telegram persists it otherwise)
    await safe_reply_text(u, "Running a deep dive... one sec.", quote=True, reply_markup=ReplyKeyboardRemove())
    await _maybe_send_typing(u)
    try:
        async with httpx.AsyncClient(http2=True) as client:
            mint_address = await extract_mint_from_check_text(client, text)
            if not mint_address:
                return await safe_reply_text(u, "Give me a Solana token mint, pair link, or token URL, boss!")
            intel = await enrich_token_intel(client, mint_address, deep_dive=True)
        
        if not intel: return await safe_reply_text(u, "Couldn't find hide nor hair of that one. Bad address or no data.")
        
        # Header line like other commands
        check_quips = [
            "🔍 Tony put this one on the bench — full breakdown",
            "🔍 Here’s the inspection report",
            "🔍 Tony pulled it apart — no shortcuts",
            "🔍 Token double-checked the details",
            "🔍 Rugcheck complete — truth below",
            "🔍 Tony says: under the hood now",
            "🔍 Every gauge read — log below",
            "🔍 Inspection done — nothing hidden",
            "🔍 Tony left no gaps — all here",
            "🔍 Report delivered — raw and clear",
        ]
        header_line = f"{pick_header_label('/check')} — {random.choice(check_quips)}"
        report_text = build_full_report2(intel, include_links=True)
        final_text = header_line + "\n\n" + report_text
        photo_content = await fetch_dexscreener_chart(intel.get('pair_address'))
        if photo_content:
            await safe_reply_photo(u, photo=photo_content, caption=final_text, parse_mode=ParseMode.HTML)
        else:
            await safe_reply_text(u, final_text, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception as e:
        log.error(f"Error in /check for text '{text}': {e}", exc_info=True)
        await safe_reply_text(u, "💀 Tony’s tools are jammed. Can't get a read on that one right now.")

async def kill(u: Update, c: ContextTypes.DEFAULT_TYPE):
    await safe_reply_text(u, "Tony's punchin' out. Shutting down...")
    log.info(f"Shutdown command received from owner {u.effective_user.id}.")
    # Use a short delayed hard-exit for cross-platform reliability (Windows-safe)
    async def _delayed_exit():
        try:
            c.application.stop()
        except Exception:
            pass
    try:
        asyncio.create_task(_delayed_exit())
    except Exception:
        pass

async def pre_shutdown(app: Application) -> None:
    """Gracefully cancel all running background tasks before shutdown."""
    log.info("Initiating graceful shutdown. Canceling background tasks...")
    tasks = [t for t in asyncio.all_tasks() if t is not asyncio.current_task()]
    if not tasks:
        return
    log.info(f"Canceling {len(tasks)} background tasks...")
    for task in tasks:
        task.cancel()
    await asyncio.gather(*tasks, return_exceptions=True)
    log.info("All background tasks canceled. Shutdown complete.")

async def seed(u: Update, c: ContextTypes.DEFAULT_TYPE):
    """Owner-only: seed one or more mints into the discovery queue for testing."""
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    mints = text.split()[1:]
    for m in mints[:10]:
        asyncio.create_task(process_discovered_token(m))
    await safe_reply_text(u, f"Queued {len(mints[:10])} mint(s) for discovery.")

async def dbprune(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    days_snap = int(CONFIG.get("SNAPSHOT_RETENTION_DAYS", 14))
    days_rej = int(CONFIG.get("REJECTED_RETENTION_DAYS", 7))
    await safe_reply_text(u, f"Pruning snapshots >{days_snap}d and rejected >{days_rej}d...")
    ok = await _db_prune(days_snap, days_rej)
    await safe_reply_text(u, "DB prune complete." if ok else "DB prune encountered an error.")

async def dbpurge(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    text = (u.message.text or "").strip()
    if not text.lower().endswith("confirm"):
        return await safe_reply_text(u, "This erases all state. Run /dbpurge confirm to proceed.")
    await safe_reply_text(u, "Purging all state and vacuuming DB...")
    await _db_purge_all()
    await safe_reply_text(u, "All state wiped. Fresh start.")
    await _execute_db("INSERT OR REPLACE INTO KeyValueStore (key, value) VALUES (?, ?)", ('last_purge_time', datetime.now(timezone.utc).isoformat()), commit=True)

async def dbclean(u: Update, c: ContextTypes.DEFAULT_TYPE):
    if u.effective_user.id != OWNER_ID:
        return await safe_reply_text(u, "Only the boss can do that.")
    quips = [
        "🧹 Tony swept the floor — cleanup done",
        "🧹 Database clear — junk’s gone",
        "🧹 Garage tidy again",
        "🧹 Old scraps tossed",
        "🧹 Tony likes a clean shop",
        "🧹 Prune finished — DB fresh",
        "🧹 Nothing left but the good stuff",
        "🧹 Workshop spotless",
        "🧹 Clutter cleared",
        "🧹 Tony says: floor’s clean, back to work",
    ]
    await safe_reply_text(u, random.choice(quips))
    days_snap = int(CONFIG.get("SNAPSHOT_RETENTION_DAYS", 14))
    days_rej = int(CONFIG.get("REJECTED_RETENTION_DAYS", 7))
    ok = await _db_prune(days_snap, days_rej)
    await safe_reply_text(u, "DB cleaned." if ok else "DB clean encountered an error.")

async def post_init(app: Application) -> None:
    """Runs async setup and starts background workers after the bot is initialized."""
    await setup_database()
    load_advanced_quips()
    
    log.info("✅ Blueprint Engine: Firing up background workers...")
    # Using PumpPortal WS (single socket). Skip client-api.* pump.fun endpoints entirely.
    # Single-socket streams (keep counts low to avoid upstream limits)
    app.create_task(pumpportal_worker(), name="PumpPortalWS")
    # Use logsSubscribe-based firehose across providers (if configured)
    app.create_task(logs_firehose_worker(), name="LogsFirehoseWorker")
    app.create_task(aggregator_poll_worker(), name="AggregatorPollWorker")
    app.create_task(second_chance_worker(), name="SecondChanceWorker")
    app.create_task(process_discovery_queue(), name="EnhancedProcessingWorker")
    app.create_task(re_analyzer_worker(), name="ReAnalyzerWorker")
    app.create_task(maintenance_worker(), name="MaintenanceWorker")
    app.create_task(circuit_breaker_reset_worker(), name="CircuitBreakerResetWorker")

    if not all([BIRDEYE_API_KEY, HELIUS_API_KEY]):
        log.warning("One or more critical API keys (Helius, Birdeye) are missing. Analysis quality will be degraded.")
        FIREHOSE_STATUS.clear()
        FIREHOSE_STATUS["System"] = "🔴 Missing API Key(s)"

    # Schedule Public/VIP push cadences if chat IDs provided

    def _sched_repeating(name: str, secs: int, chat_id: int, segment: str, delay: float = 5.0):
        if chat_id:
            jq.run_repeating(
                scheduled_push_job,
                interval=secs,
                first=delay + random.uniform(0, 5.0),
                name=name,
                data={"chat_id": chat_id, "segment": segment},
            )
            log.info(f"Scheduled {name} every {secs}s for chat {chat_id}.")

    jq = app.job_queue

    # Public cadence - only if bot has rights to post
    if PUBLIC_CHAT_ID:
        ok, reason = await _can_post_to_chat(app.bot, PUBLIC_CHAT_ID)
        if ok:
            _sched_repeating("public_hatching", 5 * 60, PUBLIC_CHAT_ID, "hatching")
            _sched_repeating("public_cooking", 60, PUBLIC_CHAT_ID, "cooking") # User request: 60s
            _sched_repeating("public_top", 60 * 60, PUBLIC_CHAT_ID, "top")
            # Continuous fresh cadence every 60 seconds
            _sched_repeating("public_fresh", 60, PUBLIC_CHAT_ID, "fresh")
        else:
            log.error(f"PUBLIC_CHAT_ID={PUBLIC_CHAT_ID} is not writable: {reason}. Auto-pushes not scheduled.")
            await _notify_owner(app.bot, f"<b>Setup required:</b> Bot lacks post rights for PUBLIC chat <code>{PUBLIC_CHAT_ID}</code> ({reason}).\nAdd the bot as <b>Admin</b> in the channel and re-run /setpublic here or restart.")

    # VIP cadence - only if bot has rights to post
    if VIP_CHAT_ID:
        ok, reason = await _can_post_to_chat(app.bot, VIP_CHAT_ID)
        if ok:
            _sched_repeating("vip_hatching", 2 * 60, VIP_CHAT_ID, "hatching")
            _sched_repeating("vip_cooking", 60, VIP_CHAT_ID, "cooking") # User request: 60s
            _sched_repeating("vip_top", 20 * 60, VIP_CHAT_ID, "top")
            # Continuous fresh cadence every 60 seconds
            _sched_repeating("vip_fresh", 60, VIP_CHAT_ID, "fresh")
        else:
            log.error(f"VIP_CHAT_ID={VIP_CHAT_ID} is not writable: {reason}. Auto-pushes not scheduled.")
            await _notify_owner(app.bot, f"<b>Setup required:</b> Bot lacks post rights for VIP chat <code>{VIP_CHAT_ID}</code> ({reason}).\nAdd the bot as <b>Admin</b> in the channel and re-run /setvip here or restart.")
def main() -> None:
    """Configures and runs the Telegram bot."""
    if not TELEGRAM_TOKEN:
        log.critical("FATAL: TELEGRAM_TOKEN not set."); sys.exit(1)

    log.info(f"✅ Token_TonyV10 'The Final Blueprint' is starting up...")
    app = (
        Application.builder()
        .token(TELEGRAM_TOKEN)
        .request(
            HTTPXRequest(
                connection_pool_size=int(CONFIG.get("TELEGRAM_POOL_SIZE", 40) or 40),
                pool_timeout=float(CONFIG.get("TELEGRAM_POOL_TIMEOUT", 30.0) or 30.0),
                connect_timeout=float(CONFIG.get("TELEGRAM_CONNECT_TIMEOUT", 20.0) or 20.0),
                read_timeout=float(CONFIG.get("TELEGRAM_READ_TIMEOUT", 30.0) or 30.0),
                write_timeout=float(CONFIG.get("TELEGRAM_READ_TIMEOUT", 30.0) or 30.0),
            )
        )
        .build()
    )
    
    handlers = [CommandHandler(cmd, func) for cmd, func in [
        ("start", start),
        ("ping", ping),
        ("diag", diag),
        ("fresh", fresh),
        ("hatching", hatching),
        ("cooking", cooking),
        ("top", top),
        ("check", check),
        ("dbprune", dbprune),
        ("dbpurge", dbpurge),
        ("dbclean", dbclean),
    ]]
    handlers.append(CommandHandler("kill", kill, filters=filters.User(user_id=OWNER_ID)))
    handlers.append(CommandHandler("seed", seed, filters=filters.User(user_id=OWNER_ID)))
    handlers.append(CommandHandler("set", set_config, filters=filters.User(user_id=OWNER_ID)))
    handlers.append(CommandHandler("setpublic", setpublic, filters=filters.User(user_id=OWNER_ID)))
    handlers.append(CommandHandler("setvip", setvip, filters=filters.User(user_id=OWNER_ID)))
    handlers.append(CommandHandler("push", push, filters=filters.User(user_id=OWNER_ID)))
    handlers.append(CommandHandler("testpush", testpush, filters=filters.User(user_id=OWNER_ID)))
    app.add_handlers(handlers)

    # In channels, commands arrive as channel_post updates. Route them explicitly.
    async def _route_channel_commands(u: Update, c: ContextTypes.DEFAULT_TYPE):
        try:
            text = (getattr(getattr(u, 'effective_message', None), 'text', '') or '').strip()
            if not text.startswith('/'):
                return
            # Extract '/cmd' and strip optional '@BotUser'
            m = re.match(r"^/([A-Za-z0-9_]+)(?:@\w+)?(?:\s|$)", text)
            if not m:
                return
            cmd = m.group(1).lower()
            # Map to existing handlers
            mapping = {
                'start': start,
                'ping': ping,
                'diag': diag,
                'fresh': fresh,
                'hatching': hatching,
                'cooking': cooking,
                'top': top,
                'check': check,
                'setpublic': setpublic,
                'setvip': setvip,
                'push': push,
                'testpush': testpush,
            }
            func = mapping.get(cmd)
            if func:
                await func(u, c)
        except Exception as e:
            try:
                await safe_reply_text(u, f"Command error: {e}")
            except Exception:
                pass

    from telegram.ext import MessageHandler
    app.add_handler(MessageHandler(filters.ChatType.CHANNEL & filters.COMMAND, _route_channel_commands))

    app.post_init = post_init
    app.pre_shutdown = pre_shutdown
    # Longer long-poll timeout reduces connection churn and ReadError; small poll_interval keeps responsiveness
    app.run_polling(drop_pending_updates=True, timeout=30, poll_interval=0.5)

if __name__ == "__main__":
    main()
