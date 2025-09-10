import os
import sys
import argparse
from typing import Optional, Union

try:
    # Load .env if present when running this helper directly
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

import requests

# Read from environment. These must be present for sending to work.
BOT_TOKEN = (os.getenv("TELEGRAM_TOKEN", "") or "").strip()
VIP_CHANNEL_ID = (os.getenv("VIP_CHAT_ID", "") or "").strip()
PUBLIC_CHAT_ID = (os.getenv("PUBLIC_CHAT_ID", "") or "").strip()

def send_vip_alert(message_text: str, chat_id: Optional[Union[str, int]] = None, parse_mode: str = "HTML") -> bool:
    """
    Send a message to your VIP channel using the Telegram Bot API directly.
    - message_text: the text to send (HTML by default, since Tony formats with HTML)
    - chat_id: optional explicit channel/chat id; defaults to VIP_CHANNEL_ID
    - parse_mode: "HTML" or "Markdown" (Tony uses HTML)
    Returns True on success, False otherwise.
    """
    # Default to VIP channel; if not set, fall back to PUBLIC_CHAT_ID
    cid = chat_id if chat_id is not None else (VIP_CHANNEL_ID or PUBLIC_CHAT_ID)

    if not BOT_TOKEN or not cid:
        print("VIP alert skipped: missing TELEGRAM_TOKEN or VIP_CHAT_ID")
        return False

    # Coerce chat id to int if it looks numeric
    try:
        if isinstance(cid, str) and cid.lstrip("-").isdigit():
            cid = int(cid)
    except Exception:
        pass

    url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": cid,
        "text": message_text,
        "parse_mode": parse_mode,
        "disable_web_page_preview": True,
    }

    try:
        r = requests.post(url, json=payload, timeout=15)
        if r.status_code == 200:
            try:
                data = r.json()
            except Exception:
                data = {}
            if data.get("ok"):
                print("SUCCESS: Alert sent to VIP channel")
                return True
            print(f"ERROR: Telegram responded ok=false: {data}")
            return False
        print(f"ERROR: Telegram responded {r.status_code}: {r.text}")
        return False
    except Exception as e:
        print(f"VIP alert error: {e}")
        return False


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Send a VIP alert via Telegram Bot API.")
    parser.add_argument("message", nargs="?", default="Test alert from alert_sender.py", help="Message text (HTML allowed)")
    parser.add_argument("--chat", dest="chat", help="Override chat id (e.g., -1001234567890)")
    args = parser.parse_args()
    ok = send_vip_alert(args.message, chat_id=args.chat)
    sys.exit(0 if ok else 1)
