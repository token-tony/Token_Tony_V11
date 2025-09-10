# -*- coding: utf-8 -*-
import configparser
import html as _html
import logging
import random
import re
from pathlib import Path
from typing import Any, Dict, List, Optional

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
try:
    from telegram.constants import CopyTextButton  # type: ignore
except Exception:
    CopyTextButton = None  # type: ignore

from config import CONFIG, PLAIN_TEXT_MODE

log = logging.getLogger("token_tony.reports")

ADVANCED_QUIPS: Dict[str, List[Dict[str, Any]]] = {}

def _parse_condition(condition_str: str) -> Optional[tuple[str, str, float]]:
    if not condition_str or condition_str.lower() == 'none': return None
    parts = condition_str.split();
    if len(parts) != 3: return None
    key, op, val_str = parts
    try: return (key, op, float(val_str)) if op in ['>', '<', '==', '!='] else None
    except ValueError: return None

def load_advanced_quips():
    global ADVANCED_QUIPS
    ADVANCED_QUIPS = {}
    path = Path(__file__).parent.parent.joinpath(CONFIG['QUIP_FILE'])
    if not path.exists(): return log.warning(f"Quip file not found: {CONFIG['QUIP_FILE']}.")
    config = configparser.ConfigParser()
    try:
        config.read(path, encoding='utf-8-sig')
        grade_map = {"MOONSHOT": "🚀 MOONSHOT", "PROMISING": "📈 PROMISING", "RISKY": "⚠️ RISKY", "RUG": "💀 DANGER"}
        for section in config.sections():
            if quip_text := config.get(section, 'QUIP', fallback=None):
                for grade in config.get(section, 'GRADE', fallback='').split(','):
                    if grade_key := grade_map.get(grade.strip().upper()):
                        ADVANCED_QUIPS.setdefault(grade_key, []).append({"quip": quip_text, "condition": _parse_condition(config.get(section, 'CONDITION', fallback='None'))})
        log.info(f"🗒️ Loaded {sum(len(q) for q in ADVANCED_QUIPS.values())} advanced quips.")
    except Exception as e:
        log.error(f"Error parsing quip file {CONFIG['QUIP_FILE']}: {e}")

def pick_advanced_quip(intel: Dict[str, Any]) -> str:
    score = intel.get("score", 0)
    grade = _grade_label(int(score or 0))
    eligible_quips = [q["quip"] for q in ADVANCED_QUIPS.get(grade, []) if q.get("condition") is None]
    for q_obj in ADVANCED_QUIPS.get(grade, []):
        if condition := q_obj.get("condition"):
            key, op, value = condition
            if (intel_val := intel.get(key)) is not None:
                try:
                    x = float(intel_val)
                    if (op == '>' and x > value) or (op == '<' and x < value) or \
                       (op == '==' and x == value) or (op == '!=' and x != value):
                        eligible_quips.append(q_obj["quip"])
                except (ValueError, TypeError): pass
    _DEFAULT = {"🚀 MOONSHOT": ["This dog will hunt."], "📈 PROMISING": ["Momentum’s building."], "⚠️ RISKY": ["Eyes open."], "💀 DANGER": ["You’re the exit liquidity."]}
    final_quip = random.choice(eligible_quips) if eligible_quips else random.choice(_DEFAULT.get(grade, ["Data > drama."]))
    try: return final_quip.format(liquidity=format_usd(intel.get('liquidity_usd')), mc=format_usd(intel.get('market_cap_usd')))
    except (KeyError, TypeError): return final_quip

def format_usd(x: Optional[float]) -> str:
    if x is None: return "—"
    try:
        if x >= 1_000_000: return f"${x/1_000_000:.2f}M"
        if x >= 1_000: return f"${x/1_000:.1f}k"
        return f"${x:,.0f}" if x > 0 else "$0"
    except (ValueError, TypeError): return str(x)

def _esc(v: Any) -> str: return _html.escape(str(v), quote=True)

def _token_link(mint: str, type: str) -> str:
    if type == "scanner": return f"https://rugcheck.xyz/tokens/{mint}"
    if type == "trade": return f"https://jup.ag/swap/SOL-{mint}"
    return f"https://birdeye.so/token/{mint}?chain=solana"

def _format_age(minutes: Optional[float]) -> str:
    if minutes is None: return "N/A"
    if minutes < 1: return "&lt;1m"
    if minutes < 60: return f"{int(minutes)}m"
    if minutes < 1440: return f"{int(minutes // 60)}h {int(minutes % 60)}m"
    return f"{int(minutes // 1440)}d {int((minutes % 1440) // 60)}h"

def _grade_label(score: int) -> str:
    if score >= 90:
        return "🚀 MOONSHOT"
    if score >= 70:
        return "📈 PROMISING"
    if score >= 40:
        return "⚠️ RISKY"
    return "💀 DANGER"

def _confidence_bar2(score: int) -> str:
    blocks = max(0, min(10, round(score / 10)))
    emoji = "🚀" if score >= 85 else "📈" if score >= 65 else "⚠️" if score >= 40 else "💀"
    return f"{emoji} [{'█' * blocks}{'░' * (10 - blocks)}] {score}%"

def _plain_divider() -> str:
    # Simple long divider line with no emoji
    return "\n" + ("─" * 34) + "\n"

def build_compact_report3(items: List[Dict[str, Any]], include_links: bool = True) -> str:
    """
    Tony's Scorecard: A richer compact report with mini-meters for at-a-glance risk assessment.
    - [NEW] Replaced text-based vitals with visual mini-meters for Mint and Top 10.
    - [NEW] Reorganized lines with themed emojis for better scannability.
    - [NEW] Dynamic grade emoji for a more expressive header.
    """
    blocks = []
    DIV = _plain_divider()

    def _norm_sym_name(i: Dict[str, Any], mint: str) -> tuple[str, str]:
        sym = i.get("symbol")
        name = i.get("name")
        if not sym or str(sym).upper() == "N/A": sym = mint[:4]
        if not name or str(name).strip().lower() in {"unnamed", "n/a", ""}: name = sym
        return str(sym), str(name)

    for i in items:
        mint = i.get("mint")
        if not mint: continue
        score = int(i.get("score", 0) or 0)
        grade = _grade_label(score)
        grade_emoji = grade.split(' ', 1)[0]
        sym_raw, name_raw = _norm_sym_name(i, mint)
        name = _esc(name_raw)
        sym = _esc(sym_raw)

        header = f"{grade_emoji} <b>${sym}</b> | <b>{name}</b> | {grade.split(' ', 1)[1]}"
        # Lines with emoji or plain text fallbacks (add Vol to market line)
        if PLAIN_TEXT_MODE:
            market_line = (
                f"MC: {format_usd(i.get('market_cap_usd'))} | "
                f"Liq: {format_usd(i.get('liquidity_usd'))} | "
                f"Vol: {format_usd(i.get('volume_24h_usd'))}"
            )
        else:
            market_line = (
                f"📈 MC: {format_usd(i.get('market_cap_usd'))} | "
                f"💧 Liq: {format_usd(i.get('liquidity_usd'))} | "
                f"🔄 Vol: {format_usd(i.get('volume_24h_usd'))}"
            )
        holders_val = (int(i.get('holders_count', 0)) if (i.get('holders_count') is not None) else 'N/A')
        if PLAIN_TEXT_MODE:
            community_line = f"Holders: {holders_val} | Age: {_format_age(i.get('age_minutes'))}"
        else:
            community_line = f"👥 Holders: {holders_val} | ⏱️ Age: {_format_age(i.get('age_minutes'))}"

        mint_auth = i.get('mint_authority')
        freeze_auth = i.get('freeze_authority')
        is_clean = (mint_auth is None and freeze_auth is None) or (not mint_auth and not freeze_auth)
        if PLAIN_TEXT_MODE:
            mint_line = f"Mint: {'Clean' if is_clean else 'Active'}"
        else:
            mint_meter = "■■■■■ Clean" if is_clean else "□□□□□ Active"
            mint_line = f"🔐 Mint: <code>{mint_meter}</code>"

        top10 = i.get('top10_holder_percentage')
        if top10 is None:
            top10_meter = "????? N/A"
        else:
            try:
                pct = float(top10)
                if pct <= 20: bar = "■■■■■"
                elif pct <= 40: bar = "■■■■□"
                elif pct <= 60: bar = "■■■□□"
                else: bar = "■□□□□"
                top10_meter = f"{bar} {pct:.1f}%"
            except Exception:
                top10_meter = f"????? {top10}%"
        
        if PLAIN_TEXT_MODE:
            if top10 is None:
                distro_line = "Top 10: N/A"
            else:
                try:
                    distro_line = f"Top 10: {float(top10):.1f}%"
                except Exception:
                    distro_line = f"Top 10: {top10}%"
        else:
            distro_line = f"🏆 Top 10: <code>{top10_meter}</code>"

        links_line = (
            f"<a href='{_esc(_token_link(mint, 'chart'))}'>Chart</a> | "
            f"<a href='{_esc(_token_link(mint, 'trade'))}'>Trade</a> | "
            f"<a href='{_esc(_token_link(mint, 'scanner'))}'>Tracker</a>"
            if PLAIN_TEXT_MODE
            else (
                f"🔗 <a href='{_esc(_token_link(mint, 'chart'))}'>Chart</a> | "
                f"⚒️ <a href='{_esc(_token_link(mint, 'trade'))}'>Trade</a> | "
                f"🐾 <a href='{_esc(_token_link(mint, 'scanner'))}'>Tracker</a>"
            )
        )
        mint_code_line = f"<code>{mint}</code>"

        parts = [
            header,
            market_line,
            community_line,
            mint_line,
            distro_line,
        ]
        if include_links:
            parts.extend(["", links_line, mint_code_line])
        blocks.append("\n".join(parts).strip())

    return f"{DIV}".join(blocks).strip()

def build_full_report2(i: Dict[str, Any], include_links: bool = True) -> str:
    """
    Full deep-dive report for the /check command.
    - [ENHANCED] Improved layout and emoji consistency for better scannability.
    - Added a blockquote style for "Tony's Quip" to make it stand out.
    """
    score = int(i.get("score", 0) or 0)
    grade = _grade_label(score)
    grade_emoji = grade.split(' ', 1)[0]
    mint = i.get("mint", "")
    # Normalize name/symbol to avoid N/A/Unnamed in output
    _sym = i.get("symbol")
    _name = i.get("name")
    if not _sym or str(_sym).upper() == "N/A":
        _sym = mint[:4]
    if not _name or str(_name).strip().lower() in {"unnamed", "n/a", ""}:
        _name = _sym
    name, sym = _esc(str(_name)), _esc(str(_sym))

    header = f"{grade_emoji} <a href='{_esc(_token_link(mint, 'chart'))}'><b>${sym} — {name}</b></a>"
    tonys_quip = f"<blockquote><i>\"{_esc(pick_advanced_quip(i))}\"</i></blockquote>"
    confidence_meter = _confidence_bar2(score)

    sss_score = i.get('sss_score', 'N/A')
    mms_score = i.get('mms_score', 'N/A')

    p24h = i.get('price_change_24h', 0) or 0
    p24h_str = f"📈 {p24h:.1f}%" if p24h >= 0 else f"📉 {p24h:.1f}%"
    
    market_pulse = [
        "<b>📡 Token Pulse</b>",
        f"  - Liquidity: {format_usd(i.get('liquidity_usd'))}",
        f"  - Market Cap: {format_usd(i.get('market_cap_usd'))}",
        f"  - Volume (24h): {format_usd(i.get('volume_24h_usd'))}",
        f"  - Price Change (24h): {p24h_str}",
        f"  - Age: {_format_age(i.get('age_minutes'))}",
    ]

    top10 = i.get('top10_holder_percentage')
    top10_str = f"{float(top10):.1f}%" if top10 is not None else "N/A"
    
    vitals = [
        "<b>🧰 Under the Hood</b>",
        f"  - Safety Score (SSS): <b>{sss_score}</b>/100",
        f"  - Maturity Score (MMS): <b>{mms_score}</b>/100",
        f"  - Mint/Freeze Auth: {'✅ Revoked' if not i.get('mint_authority') and not i.get('freeze_authority') else '⚠️ Active'}",
        f"  - Top 10 Holders: {top10_str}",
        f"  - Rugcheck.xyz: {i.get('rugcheck_score', 'N/A')}",
    ]

    socials = i.get("socials", {})
    social_lines = ["<b>🌐 Socials</b>"]
    if twt_link := socials.get('Twitter'):
        twt_stats = i.get('twitter_stats')
        if twt_stats:
            followers = twt_stats.get('followers', 0)
            age = twt_stats.get('age_days', 'N/A')
            social_lines.append(f"  - <a href='{_esc(twt_link)}'>Twitter</a>: ✅ Found ({followers:,} followers, {age}d old)")
        else:
            social_lines.append(f"  - <a href='{_esc(twt_link)}'>Twitter</a>: ✅ Found")
    else:
        social_lines.append(f"  - Twitter: ❌ Not Found")

    if tg_link := socials.get('Telegram'):
        social_lines.append(f"  - <a href='{_esc(tg_link)}'>Telegram</a>: ✅ Found")
    else:
        social_lines.append("  - Telegram: ❌ Not Found")

    if web_link := socials.get('Website'):
        social_lines.append(f"  - <a href='{_esc(web_link)}'>Website</a>: ✅ Found")
    else:
        social_lines.append("  - Website: ❌ Not Found")

    creator_lines = []
    if addr := i.get("creator_address"):
        count = i.get('creator_token_count')
        count_str = f"<b>{count}</b>" if count is not None else "N/A"
        creator_lines = [
            "<b>🧾 Creator Dossier</b>",
            f"  - Wallet: <a href='{_esc(f'https://solscan.io/account/{addr}')}'>{_esc(f'{addr[:4]}...{addr[-4:]}')}</a>",
            f"  - Prior Tokens Created: {count_str}",
        ]

    links_line = (
        f"<a href='{_esc(_token_link(mint, 'chart'))}'>Chart</a> | "
        f"<a href='{_esc(_token_link(mint, 'trade'))}'>Trade</a> | "
        f"<a href='{_esc(_token_link(mint, 'scanner'))}'>Tracker</a>"
        if PLAIN_TEXT_MODE
        else (
            f"🔗 <a href='{_esc(_token_link(mint, 'chart'))}'>Chart</a> | "
            f"⚒️ <a href='{_esc(_token_link(mint, 'trade'))}'>Trade</a> | "
            f"🐾 <a href='{_esc(_token_link(mint, 'scanner'))}'>Tracker</a>"
        )
    )

    report_parts = [
        header,
        tonys_quip,
        confidence_meter,
        "\n".join(vitals),
        "\n".join(market_pulse),
        "\n".join(social_lines),
    ]
    if creator_lines:
        report_parts.append("\n".join(creator_lines))
    
    if include_links:
        report_parts.extend(["", links_line, f"<code>{mint}</code>"])
        
    return "\n\n".join(report_parts)

def action_row(mint: str) -> InlineKeyboardMarkup:
    buttons = [
        InlineKeyboardButton("🔗 Chart", url=_token_link(mint, 'chart')),
        InlineKeyboardButton("⚒️ Trade", url=_token_link(mint, 'trade')),
        InlineKeyboardButton("🐾 Tracker", url=_token_link(mint, 'scanner')),
    ]
    # Try to add native copy button if supported; otherwise fall back to a link button
    try:
        if CopyTextButton is not None:
            buttons.append(InlineKeyboardButton("📋 Contract", copy_text=CopyTextButton(text=mint)))
        else:
            raise TypeError("CopyTextButton unavailable")
    except Exception:
        # Fallback when PTB < 21 or copy button unsupported
        buttons.append(InlineKeyboardButton("📋 Contract", url=_token_link(mint, 'chart')))
    return InlineKeyboardMarkup([buttons])
