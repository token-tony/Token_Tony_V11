# -*- coding: utf-8 -*-
import asyncio
import json
import logging
import random
import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import httpx
from cachetools import TTLCache

from config import (BIRDEYE_API_KEY, BITQUERY_API_KEY, CONFIG,
                    GECKO_API_URL, HELIUS_API_KEY, HELIUS_RPC_URL,
                    JUP_QUOTE_URL, KNOWN_QUOTE_MINTS, RUGCHECK_API_URL,
                    RUGCHECK_JWT, SOL_MINT, TOKEN_PROGRAM_ID, TOKEN2022_PROGRAM_ID, X_BEARER_TOKEN)
from tony_helpers.utils import HTTP_LIMITER, is_valid_solana_address

log = logging.getLogger("token_tony.api")

# --- Global State & Caches ---
JUP_ROUTE_CACHE: TTLCache = TTLCache(maxsize=500, ttl=180)
IPFS_GATEWAY_DOWN: TTLCache = TTLCache(maxsize=16, ttl=300)
_creator_cache: TTLCache = TTLCache(maxsize=200, ttl=3600)
_rugcheck_cache: TTLCache = TTLCache(maxsize=200, ttl=900)

# API Provider health tracking
API_PROVIDERS = {
    'helius': {'success': 0, 'failure': 0, 'circuit_open': False},
    'birdeye': {'success': 0, 'failure': 0, 'circuit_open': False},
    'dexscreener': {'success': 0, 'failure': 0, 'circuit_open': False},
    'geckoterminal': {'success': 0, 'failure': 0, 'circuit_open': False},
}

# Global Lite Mode circuit breaker when upstreams rate-limit. When > time.time(), UI marks as Lite.
LITE_MODE_UNTIL: float = 0.0

async def _fetch(c: httpx.AsyncClient, url: str, method: str = "GET", **kwargs) -> Optional[Dict[str, Any]]:
    # Internal control knobs (not forwarded to httpx)
    retries = int(kwargs.pop("_retries", CONFIG.get("HTTP_RETRIES", 2)))
    headers = kwargs.pop("headers", {})
    headers.setdefault("User-Agent", "TokenTony/22.0")
    headers.setdefault("Accept-Language", "en-US,en;q=0.9")
    timeout = kwargs.pop("timeout", CONFIG["HTTP_TIMEOUT"])
    provider_key = None # For circuit breaker

    # Determine provider for health tracking
    if "public-api.birdeye.so" in url: provider_key = "birdeye"
    elif "api.dexscreener.com" in url: provider_key = "dexscreener"
    elif "api.geckoterminal.com" in url: provider_key = "geckoterminal"
    elif "helius-rpc.com" in url: provider_key = "helius"

    if provider_key and API_PROVIDERS.get(provider_key, {}).get('circuit_open'):
        log.warning(f"Circuit breaker for {provider_key} is open. Skipping fetch for {url}")
        return None
    kwargs["headers"] = headers
    kwargs["timeout"] = timeout
    # Apply endpoint-aware throttling to avoid 429s
    try:
        rate_key = "default"
        if "public-api.birdeye.so" in url:
            if any(x in url for x in ["/price", "/defi/price", "/token_price"]):
                rate_key = "birdeye_price"
                await HTTP_LIMITER.ensure_bucket(rate_key, capacity=250, refill=250, interval=1.0)
            elif any(x in url for x in ["ohlcv", "history", "/candles"]):
                rate_key = "birdeye_history"
                await HTTP_LIMITER.ensure_bucket(rate_key, capacity=80, refill=80, interval=1.0)
            else:
                rate_key = "birdeye_default"
                await HTTP_LIMITER.ensure_bucket(rate_key, capacity=100, refill=100, interval=1.0)
        elif "api.dexscreener.com" in url:
            rate_key = "dexscreener"
            # Keep DS discovery modest to avoid null/blocked responses
            await HTTP_LIMITER.ensure_bucket(rate_key, capacity=2, refill=2, interval=1.0)
        elif "api.twitter.com" in url:
            rate_key = "twitter"
            await HTTP_LIMITER.ensure_bucket(rate_key, capacity=5, refill=5, interval=1.0)
        elif "api.geckoterminal.com" in url:
            if "/search/" in url:
                rate_key = "gecko_search"
                await HTTP_LIMITER.ensure_bucket(rate_key, capacity=1, refill=1, interval=1.2) # More conservative for search
            else:
                rate_key = "gecko_pools"
                await HTTP_LIMITER.ensure_bucket(rate_key, capacity=2, refill=2, interval=1.0)
        elif "rugcheck.xyz" in url:
            rate_key = "rugcheck"
            # Keep Rugcheck calls modest to avoid 5xx under burst
            await HTTP_LIMITER.ensure_bucket(rate_key, capacity=3, refill=3, interval=1.0)
        elif "helius-rpc.com" in url:
            rate_key = "helius_tx"
            await HTTP_LIMITER.ensure_bucket(rate_key, capacity=5, refill=5, interval=1.0)
        await HTTP_LIMITER.limit(rate_key)
    except Exception:
        # Never block on limiter misconfig
        pass

    for attempt in range(retries):
        try:
            r = await c.request(method, url, follow_redirects=True, **kwargs)
            r.raise_for_status()
            if provider_key: API_PROVIDERS[provider_key]['success'] += 1
            ctype = (r.headers.get("content-type") or "").lower()
            # Treat any content-type containing 'json' as JSON (handles vnd.api+json)
            if "json" in ctype:
                return r.json()
            return {"text": r.text}
        except httpx.HTTPStatusError as e:
            if provider_key:
                API_PROVIDERS[provider_key]['failure'] += 1
                # Check if we need to open the circuit
                stats = API_PROVIDERS[provider_key]
                if (stats['success'] + stats['failure']) > 20 and (stats['failure'] / (stats['success'] + stats['failure'])) > 0.5:
                    stats['circuit_open'] = True
                    log.error(f"CIRCUIT BREAKER OPEN for {provider_key} due to high failure rate.")
            # IPFS gateways occasionally return 5xx; treat as transient and let caller fallback to next gateway
            if 500 <= e.response.status_code < 600 and (
                "ipfs.io" in url or "cloudflare-ipfs" in url or "pinata.cloud" in url or "w3s.link" in url or "nftstorage.link" in url or "ipfscdn.io" in url
            ):
                log.info(f"IPFS gateway {e.response.status_code} for {url}. Will allow fallback.")
                return None
            if e.response.status_code == 429:
                # Honor Retry-After when present
                try:
                    ra = float(e.response.headers.get("Retry-After", "2"))
                except Exception:
                    ra = 2.0
                # Trip Lite Mode briefly to signal pushes to avoid live fetches
                try:
                    global LITE_MODE_UNTIL
                    LITE_MODE_UNTIL = max(LITE_MODE_UNTIL, time.time() + min(180.0, max(2.0, ra * 2)))
                except Exception:
                    pass
                await asyncio.sleep(min(10.0, ra) + random.uniform(0, 0.5))
                continue
            # Cloudflare edge SSL errors from some CDNs (e.g., 525/526) — don't hammer, just stop and let fallback handle
            if e.response.status_code in (520, 522, 523, 524, 525, 526):
                log.info(f"Transient upstream error {e.response.status_code} for {url}. Skipping.")
                return None
            # For IPFS gateways, treat 4xx (e.g., 404) as a signal to try the next gateway.
            if 400 <= e.response.status_code < 500 and e.response.status_code not in [429, 408]:
                if any(s in url for s in ("ipfs.io", "cloudflare-ipfs", "pinata.cloud", "w3s.link", "nftstorage.link", "ipfscdn.io")):
                    log.info(f"IPFS gateway {e.response.status_code} for {url}. Will try next gateway.")
                    return None
                if provider_key:
                    # A 4xx (non-rate-limit) is not a provider failure; reverse the earlier increment safely.
                    stats = API_PROVIDERS[provider_key]
                    stats['failure'] = max(0, stats['failure'] - 1)
                log.warning(f"Client error for {url}: {e.response.status_code}. Not retrying.")
                return None
            log.warning(f"HTTP error for {url}: {e}. Retrying ({attempt+1}/{CONFIG['HTTP_RETRIES']})...")
        except (httpx.RequestError, json.JSONDecodeError, asyncio.TimeoutError) as e:
            if provider_key: API_PROVIDERS[provider_key]['failure'] += 1
            # Downgrade IPFS gateway transient network failures to info to reduce noise
            will_retry = attempt < retries - 1
            msg = (
                f"Fetch failed for {url}: {e}. Retrying ({attempt+1}/{retries})..."
                if will_retry else
                f"Fetch failed for {url}: {e}. Attempt ({attempt+1}/{retries})."
            )
            if any(s in url for s in ("ipfs.io", "cloudflare-ipfs", "pinata.cloud", "w3s.link", "nftstorage.link", "ipfscdn.io")):
                log.info(msg)
            else:
                log.warning(msg)
        if attempt < retries - 1: await asyncio.sleep(1.2 * (attempt + 1))
    return None

def _is_ipfs_uri(uri: str) -> bool:
    u = (uri or "").strip().lower()
    # Accept any ipfs:// form or any URL containing '/ipfs/' regardless of host
    return u.startswith("ipfs://") or "/ipfs/" in u

def _ipfs_cid_and_path(uri: str) -> Optional[Tuple[str, str]]:
    try:
        s = uri.strip()
        if s.startswith("ipfs://"):
            rest = s[len("ipfs://"):]
            if rest.startswith("ipfs/"): rest = rest[len("ipfs/"):]
            parts = rest.split("/", 1)
            cid = parts[0]
            sub = parts[1] if len(parts) > 1 else ""
            return cid, sub
        # HTTP gateway forms
        m = re.search(r"/ipfs/([A-Za-z0-9]+)(?:/(.*))?", s)
        if m:
            cid = m.group(1)
            sub = m.group(2) or ""
            return cid, sub
    except Exception:
        return None
    return None

def _ipfs_urls(cid: str, subpath: str = "") -> List[str]:
    from config import get_ipfs_gateways
    suffix = f"{cid}/{subpath}" if subpath else cid
    gateways = get_ipfs_gateways()
    return [gw + suffix for gw in gateways]

IPFS_GATEWAY_METRICS: Dict[str, Dict[str, Any]] = {}

def _ipfs_metrics_record(host: str, ok: bool, latency_ms: float = 0.0, err: Optional[str] = None) -> None:
    try:
        m = IPFS_GATEWAY_METRICS.get(host)
        if not m:
            m = {"ok": 0, "err": 0, "avg_ms": 0.0, "samples": 0, "last_error": ""}
            IPFS_GATEWAY_METRICS[host] = m
        if ok:
            m["ok"] += 1
        else:
            m["err"] += 1
        # Running average latency (EWMA-lite)
        if latency_ms > 0:
            n = m["samples"] + 1
            m["avg_ms"] = (m["avg_ms"] * m["samples"] + latency_ms) / n
            m["samples"] = n
        if err:
            m["last_error"] = err[:200]
    except Exception:
        pass

async def fetch_ipfs_json(c: httpx.AsyncClient, uri: str) -> Optional[Dict[str, Any]]:
    """Fetch IPFS JSON with hedged fallback.
    - Primary gateway starts immediately.
    - After CONFIG["IPFS_HEDGE_MS"] delay, launch the next gateway.
    - First successful JSON wins; remaining task(s) are canceled.
    - Any additional gateways beyond the first two are tried sequentially if needed.
    """
    parsed = _ipfs_cid_and_path(uri)
    if not parsed:
        return None
    cid, sub = parsed
    urls = _ipfs_urls(cid, sub)

    # Skip gateways recently marked down
    try:
        down_cache = IPFS_GATEWAY_DOWN
    except NameError:
        down_cache = None
    active_urls = [u for u in urls if not down_cache or not down_cache.get(u.split('/')[2])]
    if not active_urls:
        active_urls = urls

    timeout_s = float(CONFIG.get("IPFS_FETCH_TIMEOUT_SECONDS", 5))

    async def _one(url: str):
        host = url.split('/')[2]
        try:
            # Short timeout and single attempt per gateway
            t0 = time.time()
            res = await _fetch(c, url, headers={"Accept": "application/json"}, timeout=timeout_s, _retries=1)
            dt = (time.time() - t0) * 1000.0
            if isinstance(res, dict):
                _ipfs_metrics_record(host, ok=True, latency_ms=dt)
            else:
                _ipfs_metrics_record(host, ok=False, latency_ms=dt, err="non-json response")
            return res
        except Exception as e:
            # Mark host as down for a few minutes on DNS issues
            if down_cache is not None and ("getaddrinfo failed" in str(e).lower() or isinstance(e, httpx.RequestError)):
                down_cache[host] = True
            _ipfs_metrics_record(host, ok=False, err=str(e))
            return None

    # If we have at least two gateways, do a hedged request: primary now, secondary after a delay.
    hedge_ms = int(CONFIG.get("IPFS_HEDGE_MS", 800) or 0)
    if len(active_urls) >= 2:
        primary = active_urls[0]
        secondary = active_urls[1]
        t_primary = asyncio.create_task(_one(primary))
        t_secondary: Optional[asyncio.Task] = None

        try:
            # Wait for either primary to finish within the hedge window, or time out and start secondary
            done, pending = await asyncio.wait({t_primary}, timeout=(hedge_ms / 1000.0) if hedge_ms > 0 else 0, return_when=asyncio.FIRST_COMPLETED)
            if not done and hedge_ms > 0:
                # Start secondary after hedge delay
                t_secondary = asyncio.create_task(_one(secondary))
                # Wait for first to complete between the two
                done, pending = await asyncio.wait({t_primary, t_secondary}, return_when=asyncio.FIRST_COMPLETED)

            # Check the first completed task
            for t in done:
                try:
                    res = await t
                except Exception:
                    res = None
                if res and isinstance(res, dict):
                    # Cancel the other task if running
                    for p in pending:
                        if not p.done():
                            p.cancel()
                    return res

            # If neither succeeded, await the remaining task (if any) and try additional gateways sequentially
            remaining_result = None
            for p in pending:
                try:
                    remaining_result = await p
                except Exception:
                    remaining_result = None
                if remaining_result and isinstance(remaining_result, dict):
                    return remaining_result
        finally:
            # Cleanup stray tasks
            for t in (t_primary, t_secondary) if 't_secondary' in locals() else (t_primary,):
                if t and not t.done():
                    try: t.cancel()
                    except Exception: pass

        # If both primary and secondary failed, try any additional gateways sequentially
        for url in active_urls[2:]:
            res = await _one(url)
            if res and isinstance(res, dict):
                return res
        return None

    # Only one active gateway -> just try it
    if active_urls:
        res = await _one(active_urls[0])
        if res and isinstance(res, dict):
            return res
    return None

async def fetch_helius_asset(c: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    if not HELIUS_API_KEY:
        log.warning("Helius API Key not configured. Cannot fetch asset data.")
        return None
    
    payload = {"jsonrpc": "2.0", "id": 1, "method": "getAsset", "params": {"id": mint}}
    res = await _fetch(c, HELIUS_RPC_URL, method="POST", json=payload, timeout=10.0)
    if res and "error" not in res:
        return res
    if res and "error" in res:
        log.warning(f"Helius RPC error: {res['error']}")
    return None

async def fetch_birdeye(c: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    if not BIRDEYE_API_KEY: return None
    headers = {"X-API-KEY": BIRDEYE_API_KEY}
    url = f"https://public-api.birdeye.so/v1/defi/token_overview?address={mint}"
    return await _fetch(c, url, method="GET", headers=headers)

async def fetch_market_snapshot(c: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    """Normalized market snapshot with intelligent, health-aware provider rotation."""
    # Build a list of available providers, sorted by success rate
    provider_fns = {
        'dexscreener': fetch_dexscreener_by_mint,
        'birdeye': fetch_birdeye,
        'geckoterminal': fetch_gecko_market_data,
    }

    available_providers = []
    for name, stats in API_PROVIDERS.items():
        if name in provider_fns and not stats.get('circuit_open'):
            success_rate = stats.get('success', 0) / max(1, stats.get('success', 0) + stats.get('failure', 0))
            available_providers.append((name, success_rate))

    # Sort providers by success rate, highest first
    available_providers.sort(key=lambda x: x[1], reverse=True)

    for provider_name, _ in available_providers:
        try:
            fetch_fn = provider_fns[provider_name]
            result = await fetch_fn(c, mint)
            if result:
                # The _fetch function handles success/failure counting
                return result
        except Exception as e:
            log.warning(f"Market snapshot fetch failed for provider {provider_name} on mint {mint}: {e}")
            # The _fetch function will have already marked the failure
            continue

    log.warning(f"All available market data providers failed for {mint}.")
    return None

async def fetch_top10_via_rpc(c: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    """Efficiently compute top10 holder concentration via RPC.
    Uses getTokenSupply and getTokenLargestAccounts. Returns {'top10_holder_percentage': float}.
    """
    try:
        sup_payload = {"jsonrpc": "2.0", "id": 1, "method": "getTokenSupply", "params": [mint]}
        sup = await _fetch(c, HELIUS_RPC_URL, method="POST", json=sup_payload, timeout=6.0)
        supply = int((((sup or {}).get("result") or {}).get("value") or {}).get("amount") or 0)
        if supply <= 0:
            return None
        lar_payload = {"jsonrpc": "2.0", "id": 1, "method": "getTokenLargestAccounts", "params": [mint, {"commitment": "processed"}]}
        lar = await _fetch(c, HELIUS_RPC_URL, method="POST", json=lar_payload, timeout=6.0)
        vals = (((lar or {}).get("result") or {}).get("value") or [])
        amounts = []
        for v in vals[:10]:
            try:
                amounts.append(int(v.get("amount") or 0))
            except Exception:
                continue
        if not amounts:
            return None
        pct = round((sum(amounts) / supply) * 100.0, 1)
        return {"top10_holder_percentage": pct}
    except Exception:
        return None

async def fetch_dexscreener_by_mint(c: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    """Fallback if Birdeye fails. Uses the modern /tokens/{address} endpoint."""
    url = f"https://api.dexscreener.com/latest/dex/tokens/{mint}"
    browser_headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36"
    }
    res = await _fetch(c, url, headers=browser_headers)
    if not res or not (pairs := res.get("pairs")):
        return None

    try:
        # Find the pair with the most liquidity, filtering out tiny/malformed ones
        best_pair = max(
            (p for p in pairs if p.get("liquidity") and p["liquidity"].get("usd")),
            key=lambda p: p["liquidity"]["usd"],
            default=None
        )
        if not best_pair: return None

        return {
            "liquidity_usd": best_pair["liquidity"]["usd"],
            "market_cap_usd": best_pair.get("fdv", 0.0),
            "volume_24h_usd": best_pair.get("volume", {}).get("h24", 0.0),
            "price_change_24h": best_pair.get("priceChange", {}).get("h24", 0.0),
            "pair_address": best_pair.get("pairAddress"),
            "pair_created_ms": best_pair.get("pairCreatedAt") or best_pair.get("createdAt")
        }
    except (ValueError, TypeError, KeyError) as e:
        log.warning(f"Could not parse DexScreener pairs for {mint}: {e}")
        return None

async def fetch_dexscreener_by_pair(c: httpx.AsyncClient, pair_address: str) -> Optional[Dict[str, Any]]:
    """Resolve a DexScreener pair address to a token mint and basic market data."""
    url = f"https://api.dexscreener.com/latest/dex/pairs/solana/{pair_address}"
    res = await _fetch(c, url)
    pairs = (res or {}).get("pairs") or []
    if not pairs:
        return None
    try:
        p = pairs[0]
        base = (p.get("baseToken") or {}).get("address")
        quote = (p.get("quoteToken") or {}).get("address")
        chosen = base if base and base not in KNOWN_QUOTE_MINTS else quote
        md = {
            "liquidity_usd": (p.get("liquidity") or {}).get("usd") or 0.0,
            "market_cap_usd": p.get("fdv", 0.0),
            "volume_24h_usd": (p.get("volume") or {}).get("h24", 0.0),
            "price_change_24h": (p.get("priceChange") or {}).get("h24", 0.0),
            "pair_address": p.get("pairAddress") or pair_address,
        }
        return {"mint": chosen, "market": md}
    except Exception:
        return None

async def extract_mint_from_check_text(c: httpx.AsyncClient, text: str) -> Optional[str]:
    """Accepts mint address directly, common URLs, or a DexScreener pair URL."""
    text = text or ""
    # DexScreener pair URL
    m = re.search(r"dexscreener\.com/.*/pairs/solana/([1-9A-HJ-NP-Za-km-z]{32,44})", text)
    if m:
        pair = m.group(1)
        resolved = await fetch_dexscreener_by_pair(c, pair)
        if resolved and resolved.get("mint"):
            return resolved["mint"]
    # Birdeye token URL
    m = re.search(r"birdeye\.so/token/([1-9A-HJ-NP-Za-km-z]{32,44})", text)
    if m:
        return m.group(1)
    # Solscan token URL
    m = re.search(r"solscan\.io/token/([1-9A-HJ-NP-Za-km-z]{32,44})", text)
    if m:
        return m.group(1)
    # Pump.fun coin URL
    m = re.search(r"pump\.fun/coin/([1-9A-HJ-NP-Za-km-z]{32,44})", text)
    if m:
        return m.group(1)
    # Fallback: any base58-like substring
    m = re.search(r"([1-9A-HJ-NP-Za-km-z]{32,44})", text)
    if m:
        return m.group(1)
    return None

async def fetch_jupiter_has_route(c: httpx.AsyncClient, out_mint: str) -> Optional[bool]:
    """Return True if Jupiter reports a route from SOL to out_mint for a tiny amount.
    If the API errors or times out, return None and do not override other sources.
    """
    try:
        # Basic pre-check to avoid obvious 400s on Jupiter
        if not out_mint or not is_valid_solana_address(out_mint):
            return None
        if out_mint in JUP_ROUTE_CACHE:
            return JUP_ROUTE_CACHE[out_mint]
        params = {
            "inputMint": SOL_MINT,
            "outputMint": out_mint,
            "amount": str(1_000_000),  # 0.001 SOL in lamports as string
            "slippageBps": int(CONFIG.get("JUP_SLIPPAGE_BPS", 300) or 300),
        }
        res = await _fetch(c, JUP_QUOTE_URL, params=params, timeout=6.0)
        routes = (res or {}).get("data") or []
        ok = bool(routes)
        # Cache only definitive answers
        JUP_ROUTE_CACHE[out_mint] = ok
        return ok
    except Exception:
        return None

async def fetch_holders_count_via_rpc(c: httpx.AsyncClient, mint: str, *, max_accounts: int = 5000) -> Optional[int]:
    """Approximate unique holder count via getProgramAccounts with jsonParsed.
    Filters accounts by mint and dataSize, counts owners with non-zero balance.
    Caps parsing to max_accounts to avoid huge payloads on large caps.
    """
    try:
        async def _owners_for_program(program_id: str) -> set:
            payload = {
                "jsonrpc": "2.0",
                "id": 1,
                "method": "getProgramAccounts",
                "params": [
                    program_id,
                    {
                        "encoding": "jsonParsed",
                        "commitment": "processed",
                        "filters": [
                            {"dataSize": 165},
                            {"memcmp": {"offset": 0, "bytes": mint}},
                        ],
                    },
                ],
            }
            res = await _fetch(c, HELIUS_RPC_URL, method="POST", json=payload, timeout=10.0)
            value = (res or {}).get("result") or []
            owners_local = set()
            count_local = 0
            for acc in value:
                try:
                    info = ((acc.get("account") or {}).get("data") or {}).get("parsed", {}).get("info", {})
                    owner = info.get("owner")
                    amount = ((info.get("tokenAmount") or {}).get("amount"))
                    if owner and int(amount or 0) > 0:
                        owners_local.add(owner)
                except Exception:
                    continue
                count_local += 1
                if count_local >= max_accounts:
                    break
            return owners_local

        # Query both classic SPL Token and Token-2022 program IDs and union owners
        owners_classic, owners_2022 = await asyncio.gather(
            _owners_for_program(TOKEN_PROGRAM_ID),
            _owners_for_program(TOKEN2022_PROGRAM_ID),
        )
        owners = owners_classic | owners_2022
        return len(owners)
    except Exception:
        return None

async def fetch_gecko_market_data(c: httpx.AsyncClient, mint: str) -> Optional[Dict[str, Any]]:
    """Fallback market data using GeckoTerminal v2 with resilient fallbacks.
    Tries token pools first; if 404/empty, search pools for the mint and fetch pool details.
    """
    from tony_helpers.analysis import GECKO_SEARCH_CACHE
    headers = {"Accept": "application/json;version=20230302", "User-Agent": "Mozilla/5.0"}

    # 1) Try token -> pools
    try:
        url = f"{GECKO_API_URL}/networks/solana/tokens/{mint}/pools?include=dex"
        res = await _fetch(c, url, headers=headers)
        data = (res or {}).get("data") or []
        if data:
            best_pool = max(data, key=lambda p: float((p.get("attributes") or {}).get("reserve_in_usd", 0.0)))
            attrs = best_pool.get("attributes", {})
            return {
                "liquidity_usd": float(attrs.get("reserve_in_usd", 0.0)),
                "market_cap_usd": float(attrs.get("fdv_usd", 0.0)),
                "volume_24h_usd": float((attrs.get("volume_usd") or {}).get("h24", 0.0)),
                "price_change_24h": float((attrs.get("price_change_percentage") or {}).get("h24", 0.0)),
                "pair_address": attrs.get("address") or best_pool.get("id"),
                "pool_created_at": attrs.get("created_at"),
            }
    except Exception:
        pass

    # 2) Fallback: search pools by mint on Solana; prefer Raydium, but accept any Solana DEX if Raydium not found
    try:
        search_url = f"{GECKO_API_URL}/search/pools?query={mint}&include=base_token,quote_token,dex,network"
        if (cached := GECKO_SEARCH_CACHE.get(search_url)):
            data, included = cached
        else:
            res = await _fetch(c, search_url, headers=headers)
            data = (res or {}).get("data") or []
            included = (res or {}).get("included") or []
            GECKO_SEARCH_CACHE[search_url] = (data, included)
        dex_name = {it.get("id"): (it.get("attributes") or {}).get("name", "").lower() for it in included if it.get("type") == "dexes"}
        networks = {it.get("id"): (it.get("attributes") or {}).get("identifier", "").lower() for it in included if it.get("type") == "networks"}
        pool_id = None
        # Pass 1: prefer Raydium on Solana
        for pool in data:
            rel = pool.get("relationships", {})
            dex_rel = (rel.get("dex") or {}).get("data") or {}
            net_rel = (rel.get("network") or {}).get("data") or {}
            if networks.get(net_rel.get("id")) != "solana":
                continue
            if "raydium" in dex_name.get(dex_rel.get("id"), ""):
                pool_id = pool.get("id") or (pool.get("attributes") or {}).get("address")
                if pool_id:
                    break
        # Pass 2: accept first Solana pool from any DEX if Raydium not found
        if not pool_id:
            for pool in data:
                rel = pool.get("relationships", {})
                net_rel = (rel.get("network") or {}).get("data") or {}
                if networks.get(net_rel.get("id")) != "solana":
                    continue
                pool_id = pool.get("id") or (pool.get("attributes") or {}).get("address")
                if pool_id:
                    break
        if not pool_id:
            return None

        # fetch pool details by id/address
        detail_url = f"{GECKO_API_URL}/networks/solana/pools/{pool_id}?include=dex"
        det = await _fetch(c, detail_url, headers=headers)
        det_data = (det or {}).get("data") or {}
        attrs = det_data.get("attributes") or {}
        if attrs:
            return {
                "liquidity_usd": float(attrs.get("reserve_in_usd", 0.0)),
                "market_cap_usd": float(attrs.get("fdv_usd", 0.0)),
                "volume_24h_usd": float((attrs.get("volume_usd") or {}).get("h24", 0.0)),
                "price_change_24h": float((attrs.get("price_change_percentage") or {}).get("h24", 0.0)),
                "pair_address": attrs.get("address") or det_data.get("id"),
                "pool_created_at": attrs.get("created_at"),
            }
    except Exception:
        pass

    return None

async def fetch_rugcheck_score(c: httpx.AsyncClient, mint: str) -> Optional[str]:
    """Fetch Rugcheck risk summary with resilient endpoint + optional JWT.
    Returns a short label like '✅ OK' or '❗️ N High Risk(s)'.
    """
    if mint in _rugcheck_cache:
        return _rugcheck_cache[mint]

    headers = {"Accept": "application/json"}
    if RUGCHECK_JWT:
        headers["Authorization"] = f"Bearer {RUGCHECK_JWT}"

    # Allow an override base via env, then fall back to default
    bases = []
    try:
        if RUGCHECK_API_URL:
            bases.append(RUGCHECK_API_URL.rstrip("/"))
    except Exception:
        pass
    if "https://api.rugcheck.xyz/v1" not in bases:
        bases.append("https://api.rugcheck.xyz/v1")
    # Optional alternate host
    if "https://prod-api.rugcheck.xyz/v1" not in bases:
        bases.append("https://prod-api.rugcheck.xyz/v1")

    score = "N/A"
    data = None
    for base in bases:
        try:
            url = f"{base}/tokens/{mint}/report"
            data = await _fetch(c, url, headers=headers, timeout=8.0)
            if data:
                break
        except Exception:
            continue

    if data and isinstance(risks := data.get("risks"), list):
        try:
            high_risks = sum(1 for r in risks if (r.get('level') or '').lower() == 'high')
            score = f"❗️ {high_risks} High Risk(s)" if high_risks else "✅ OK"
        except Exception:
            score = "N/A"

    _rugcheck_cache[mint] = score
    return score

async def fetch_twitter_stats(c: httpx.AsyncClient, twitter_url: str) -> Optional[Dict[str, Any]]:
    if not X_BEARER_TOKEN or not twitter_url: return None
    username_match = re.search(r"twitter\.com/(\w+)", twitter_url)
    if not username_match: return None
    username = username_match.group(1)

    headers = {"Authorization": f"Bearer {X_BEARER_TOKEN}"}
    params = {"user.fields": "created_at,public_metrics"}
    url = f"https://api.twitter.com/2/users/by/username/{username}"
    res = await _fetch(c, url, headers=headers, params=params, timeout=5.0)

    if res and (data := res.get("data")):
        try:
            created_at = datetime.strptime(data["created_at"], "%Y-%m-%dT%H:%M:%S.%fZ").replace(tzinfo=timezone.utc)
            age_days = (datetime.now(timezone.utc) - created_at).days
            return {"followers": data.get("public_metrics", {}).get("followers_count", 0), "age_days": age_days}
        except (KeyError, ValueError) as e:
            log.warning(f"Error parsing Twitter data for {username}: {e}")
    return None

async def fetch_creator_dossier_bitquery(c: httpx.AsyncClient, creator_address: str) -> Optional[int]:
    if creator_address in _creator_cache: return _creator_cache[creator_address]
    if not BITQUERY_API_KEY: return None
    query = "query ($creator: String!) { solana { transactions(options: {limit: 250}, txSigner: {is: $creator}, success: {is: true}) { instructions { programId ... on SolanaInstruction { instructionType mint: instructionSource } } } } }"
    headers = {"X-API-KEY": BITQUERY_API_KEY}
    res = await _fetch(c, "https://graphql.bitquery.io/", method="POST", json={'query': query, 'variables': {"creator": creator_address}}, headers=headers, timeout=8.0)
    count = 0
    if res and (data := res.get("data", {}).get("solana", {}).get("transactions")):
        mints = {ix["mint"] for tx in data for ix in tx.get('instructions', []) if ix.get("programId") == "spl-token" and ix.get("instructionType") == "mintTo" and ix.get("mint")}
        count = len(mints)
    _creator_cache[creator_address] = count
    return count

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
