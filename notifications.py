
"""Push notifications to Slack.



Status updates are posted as threaded replies to the run's first message,

so a long download job produces one thread instead of flooding the channel.



Read from `.env`:

  SLACK_BOT_TOKEN    bot token (xoxb-...), enables threading

  SLACK_CHANNEL_ID   channel to post in, required with the bot token

  SLACK_WEBHOOK_URL  legacy fallback, used only if no bot token is set



With nothing configured, send_notification() is a silent no-op so callers

keep working without notifications.

"""



import os

import threading



import requests

from dotenv import load_dotenv



load_dotenv()



# Timestamp of this run's first message; later posts hang off it as replies.

_thread_ts = None

_thread_lock = threading.Lock()





def _post_via_bot(token, channel, title, message):

    global _thread_ts



    with _thread_lock:

        parent = _thread_ts



    payload = {"channel": channel, "text": f"*{title}*\n{message}"}

    if parent:

        payload["thread_ts"] = parent



    resp = requests.post(

        "https://slack.com/api/chat.postMessage",

        json=payload,

        headers={"Authorization": f"Bearer {token}"},

        timeout=10,

    )

    data = resp.json()

    if not data.get("ok"):

        print(f"Slack notification failed: {data.get('error')}")

        return



    if parent is None:

        with _thread_lock:

            if _thread_ts is None:

                _thread_ts = data.get("ts")





def send_notification(title, message):

    token = os.getenv("SLACK_BOT_TOKEN")

    channel = os.getenv("SLACK_CHANNEL_ID")

    url = os.getenv("SLACK_WEBHOOK_URL")



    try:

        if token and channel:

            _post_via_bot(token, channel, title, message)

        elif url:

            requests.post(url, json={"text": f"*{title}*\n{message}"}, timeout=10)

        else:

            print("No Slack credentials set; skipping notification.")

    except requests.RequestException as e:

        # Never let a notification failure interrupt the caller.

        print(f"Slack notification failed: {e}")

    except ValueError as e:

        print(f"Slack notification failed: bad response ({e})")

