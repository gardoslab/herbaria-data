"""
Push notifications via a Slack incoming webhook.

The webhook URL is read from `.env` (SLACK_WEBHOOK_URL). Without it
configured, send_notification() is a silent no-op so the caller (e.g.
image_install_db.py) keeps working without notifications.
"""

import os
import requests
from dotenv import load_dotenv

load_dotenv()


def send_notification(title, message):
    url = os.getenv("SLACK_WEBHOOK_URL")
    if not url:
        print("SLACK_WEBHOOK_URL not set; skipping notification.")
        return
    try:
        requests.post(
            url,
            json={"text": f"*{title}*\n{message}"},
            timeout=10,
        )
    except requests.RequestException as e:
        # Never let a notification failure interrupt the caller.
        print(f"Slack notification failed: {e}")
