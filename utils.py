# -*- coding: utf-8 -*-
import asyncio
import html as _html
import random
import re
import time
from typing import Any, Dict, List, Optional, Tuple

from telegram import Update
from telegram.constants import ParseMode

from config import OWNER_ID

# --------------------------------------------------------------------------------------
# Rate limiting primitives (token buckets) and Telegram outbox gating
# --------------------------------------------------------------------------------------

class TokenBucket:
    def __init__(self, capacity: int, refill_amount: int, interval_seconds: float) -> None:
        self.capacity = max(1, capacity)
        self.tokens = float(capacity)
        self.refill_amount = float(refill_amount)
        self.interval = float(interval_seconds)
        self._last = time.monotonic()
        self._lock = asyncio.Lock()

    async def acquire(self, amount: float = 1.0) -> None:
        amount = float(amount)
        while True:
            async with self._lock:
                now = time.monotonic()
                elapsed = max(0.0, now - self._last)
                if elapsed >= self.interval:
                    # Add whole-interval refills for stability under load
                    intervals = int(elapsed // self.interval)
                    self.tokens = min(self.capacity, self.tokens + intervals * self.refill_amount)
                    self._last = now if intervals > 0 else self._last
                if self.tokens >= amount:
                    self.tokens -= amount
                    return
                # Compute time until next token becomes available
                needed = amount - self.tokens
                rate_per_sec = (self.refill_amount / self.interval) if self.interval > 0 else self.refill_amount
                wait = max(0.01, needed / max(1e-6, rate_per_sec))
            # jitter to avoid thundering herd
            await asyncio.sleep(min(2.0, wait + random.uniform(0, 0.05)))


class HttpRateLimiter:
    """Endpoint/host aware limiters.
    Define buckets by string keys; call await limit('key') before HTTP calls.
    """

    def __init__(self) -> None:
        self._buckets: Dict[str, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def ensure_bucket(self, key: str, capacity: int, refill: int, interval: float) -> TokenBucket:
        async with self._lock:
            if key not in self._buckets:
                self._buckets[key] = TokenBucket(capacity, refill, interval)
            return self._buckets[key]

    async def limit(self, key: str) -> None:
        bucket = self._buckets.get(key)
        if bucket is None:
            # Default conservative bucket if unknown
            bucket = await self.ensure_bucket(key, capacity=10, refill=10, interval=1.0)
        await bucket.acquire(1.0)


class TelegramOutbox:
    """Global + per-chat + per-group token buckets for Telegram sends."""

    def __init__(self) -> None:
        # Global: ~30 msgs/sec
        self.global_bucket = TokenBucket(capacity=30, refill_amount=30, interval_seconds=1.0)
        self.per_chat: Dict[int, TokenBucket] = {}
        self.per_group: Dict[int, TokenBucket] = {}
        self._lock = asyncio.Lock()

    async def _chat_bucket(self, chat_id: int) -> TokenBucket:
        async with self._lock:
            if chat_id not in self.per_chat:
                # 1 msg/sec sustained per chat
                self.per_chat[chat_id] = TokenBucket(capacity=1, refill_amount=1, interval_seconds=1.0)
            return self.per_chat[chat_id]

    async def _group_bucket(self, chat_id: int) -> TokenBucket:
        async with self._lock:
            if chat_id not in self.per_group:
                # 20 msgs/min per group
                self.per_group[chat_id] = TokenBucket(capacity=20, refill_amount=20, interval_seconds=60.0)
            return self.per_group[chat_id]

    async def send_text(self, bot, chat_id: int, text: str, is_group: bool, **kwargs):
        await self.global_bucket.acquire(1)
        if is_group:
            await (await self._group_bucket(chat_id)).acquire(1)
        await (await self._chat_bucket(chat_id)).acquire(1)
        # Retry on 429 with jitter
        for attempt in range(5):
            try:
                # Map PTB convenience arg 'quote' to reply_to_message_id if present
                quote = bool(kwargs.pop("quote", False))
                if quote and hasattr(bot, "_Application"):  # not reliable; instead use context from kwargs if provided
                    pass
                reply_to_message_id = kwargs.pop("reply_to_message_id", None)
                if quote and reply_to_message_id is None:
                    # Try to infer from potential Update passed via 'update' kw (if any)
                    # If not present, just send without quoting (PTB's bot API doesn't support 'quote')
                    pass
                return await bot.send_message(chat_id=chat_id, text=text, reply_to_message_id=reply_to_message_id, **kwargs)
            except Exception as e:
                # Telegram RetryAfter or generic 429/420 errors
                msg = str(e)
                if ("Too Many Requests" in msg or "RetryAfter" in msg or "429" in msg) and attempt < 4:
                    await asyncio.sleep(1.5 + random.uniform(0, 0.6))
                    continue
                # Network flaps on underlying httpx/httpcore: retry lightly
                if any(s in msg for s in ("ReadError", "Timeout", "timed out", "Server disconnected", "reset by peer", "RemoteProtocolError", "ClientOSError")) and attempt < 4:
                    await asyncio.sleep(0.8 + 0.4 * attempt + random.uniform(0, 0.3))
                    continue
                raise

    async def send_photo(self, bot, chat_id: int, photo: bytes, is_group: bool, **kwargs):
        await self.global_bucket.acquire(1)
        if is_group:
            await (await self._group_bucket(chat_id)).acquire(1)
        await (await self._chat_bucket(chat_id)).acquire(1)
        for attempt in range(5):
            try:
                quote = bool(kwargs.pop("quote", False))
                reply_to_message_id = kwargs.pop("reply_to_message_id", None)
                if quote and reply_to_message_id is None:
                    pass
                return await bot.send_photo(chat_id=chat_id, photo=photo, reply_to_message_id=reply_to_message_id, **kwargs)
            except Exception as e:
                msg = str(e)
                if ("Too Many Requests" in msg or "RetryAfter" in msg or "429" in msg) and attempt < 4:
                    await asyncio.sleep(1.5 + random.uniform(0, 0.6))
                    continue
                if any(s in msg for s in ("ReadError", "Timeout", "timed out", "Server disconnected", "reset by peer", "RemoteProtocolError", "ClientOSError")) and attempt < 4:
                    await asyncio.sleep(0.8 + 0.4 * attempt + random.uniform(0, 0.3))
                    continue
                raise


OUTBOX = TelegramOutbox()
HTTP_LIMITER = HttpRateLimiter()

# --- Telegram helpers for channel access checks ---
async def _notify_owner(bot, text: str) -> None:
    try:
        if OWNER_ID:
            await OUTBOX.send_text(bot, OWNER_ID, text, is_group=False, parse_mode=ParseMode.HTML, disable_web_page_preview=True)
    except Exception:
        pass

async def _can_post_to_chat(bot, chat_id: int) -> tuple[bool, str]:
    """Check if the bot can post to the given chat (channel/group).
    Returns (ok, reason). ok=True when bot is admin (channels) or member with send rights (groups).
    """
    try:
        me = await bot.get_me()
        my_id = getattr(me, 'id', None)
        if not my_id:
            return False, "get_me returned no id"
    except Exception as e:
        return False, f"get_me failed: {e}"
    try:
        chat = await bot.get_chat(chat_id)
    except Exception as e:
        return False, f"get_chat failed: {e}"
    try:
        m = await bot.get_chat_member(chat_id, my_id)
        status = getattr(m, 'status', '')
        chat_type = getattr(chat, 'type', '') or ''
        is_channel = (chat_type == 'channel')
        if status in ("administrator", "creator"):
            # Admin of channel/group: check explicit permissions when available
            can_post = True
            # For channels, ensure can_post_messages if attribute exists
            if is_channel:
                can_post = bool(getattr(m, 'can_post_messages', True))
            # For groups, ensure can_send_messages if attribute exists (PTB uses ChatMemberAdministrator without this flag sometimes)
            if not is_channel:
                can_post = bool(getattr(m, 'can_send_messages', True))
            if can_post:
                return True, "ok"
            return False, f"admin but posting disabled (type={chat_type})"
        # Non-admin path: allow member in groups/supergroups if can_send_messages
        if not is_channel and status in ("member", "restricted"):
            can_send = getattr(m, 'can_send_messages', None)
            if can_send is None:
                # Assume allowed when flag missing
                return True, "ok"
            if bool(can_send):
                return True, "ok"
            return False, "member but cannot send messages"
        # For channels, non-admin cannot post
        return False, f"insufficient rights (type={chat_type}, status={status})"
    except Exception as e:
        return False, f"get_chat_member failed: {e}"

def is_valid_solana_address(address: str) -> bool:
    return bool(re.match(r"^[1-9A-HJ-NP-Za-km-z]{32,44}$", address or ""))

def _looks_like_solana_address(s: str) -> bool:
    return bool(re.fullmatch(r"[1-9A-HJ-NP-Za-km-z]{32,44}", s or ""))

def _esc(v: Any) -> str: return _html.escape(str(v), quote=True)

def _parse_typed_value(v: str) -> Any:
    s = v.strip()
    low = s.lower()
    if low in {"true", "yes", "on"}: return True
    if low in {"false", "no", "off"}: return False
    try:
        if "." in s: return float(s)
        return int(s)
    except ValueError:
        return s