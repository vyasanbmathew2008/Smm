import asyncio
from html import escape
import logging
import os
import queue
import random
import threading
import time

import requests
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, PersistentTimestampOutdatedError, RPCError, ServerError
from telethon.tl.types import MessageEmpty


# ---------------------------------------------------------------------------
# Configuration. Values from the supplied script are included as defaults for now.
# Environment variables still override them when present.
# ---------------------------------------------------------------------------
API_ID = 23877053
API_HASH = "989c360358b981dae46a910693ab2f4c"
PHONE_NUMBER = "+917511190745"
SESSION_NAME = "telegram_bot_session"
CHANNEL_USERNAMES = ["@darktunnel_data", "darktunnelfreeconfig", "@teamvrx", "@vrxtricks"]

SMM_API_KEY = "89b76130404c615e84d4a9b7edcd6c9f"
SMM_API_URL = "https://cheapestsmmpanels.com/api/v2"

# The attached script did not contain these two bot values. Fill them in here,
# or provide TELEGRAM_BOT_TOKEN and TELEGRAM_LOG_CHAT_ID as environment values.
TELEGRAM_BOT_TOKEN = "8813942198:AAHP56dlAFBMCBt91kp6Wv2Oq1AipctLwac"
TELEGRAM_LOG_CHAT_ID = 6424066420
TARGET_SERVICE_ID = 1160
MIN_BATCH_SIZE_PER_REQUEST = 100
MAX_BATCH_SIZE_PER_REQUEST = 125
MIN_DELAY_SECONDS_BETWEEN_REQUESTS = 640
MAX_DELAY_SECONDS_BETWEEN_REQUESTS = 3220
MIN_TOTAL_VIEWS_PER_POST = 700
MAX_TOTAL_VIEWS_PER_POST = 800
CATCH_UP_THRESHOLD_VIEWS = 400
CATCH_UP_CHECK_INTERVAL_SECONDS = 30

active_post_tasks = {}


class TelegramLogHandler(logging.Handler):
    """Send buffered multiline log records to a Telegram bot.

    Records are collected for a few seconds and sent together, so Telegram
    receives a readable block like the console log instead of one message
    per line. Sending happens in a worker thread and never blocks Telethon.
    """

    def __init__(self, token, chat_id):
        super().__init__()
        self.url = f"https://api.telegram.org/bot{token}/sendMessage"
        self.chat_id = chat_id
        self.log_queue = queue.Queue()
        self.max_content_length = 3400
        self.max_lines_per_page = 18
        self.batch_wait_seconds = 5
        self.worker = threading.Thread(
            target=self._worker,
            name="telegram-log-worker",
            daemon=True,
        )
        self.worker.start()

    def emit(self, record):
        try:
            self.log_queue.put_nowait(self.format(record))
        except Exception:
            # Never recurse into logging if Telegram itself is unavailable.
            pass

    def _worker(self):
        while True:
            try:
                first_line = self.log_queue.get()
                lines = [first_line]
                total_length = len(first_line)
                deadline = time.monotonic() + self.batch_wait_seconds

                while time.monotonic() < deadline:
                    try:
                        line = self.log_queue.get(timeout=max(0, deadline - time.monotonic()))
                    except queue.Empty:
                        break

                    lines.append(line)
                    total_length += len(line) + 1

                if lines:
                    self._send_pages(lines)
            except Exception:
                # Never recurse into logging if Telegram itself is unavailable.
                pass

    def _send_pages(self, lines):
        """Send compact, readable pages that fit comfortably on a phone."""
        pages = []
        current_page = []
        current_length = 0

        for line in lines:
            # Keep the timestamp in the title; make body lines compact for mobile.
            compact_line = self._compact_log_line(line)
            safe_line = escape(compact_line, quote=False)
            would_exceed_lines = len(current_page) >= self.max_lines_per_page
            would_exceed_chars = current_length + len(safe_line) + 1 > self.max_content_length

            if current_page and (would_exceed_lines or would_exceed_chars):
                pages.append(current_page)
                current_page = []
                current_length = 0

            current_page.append((line, safe_line))
            current_length += len(safe_line) + 1

        if current_page:
            pages.append(current_page)

        for page_lines in pages:
            # LogFormatter places the event date/time in the first 19 characters.
            # Use that event timestamp as the mobile message title.
            message_timestamp = escape(page_lines[0][0][:19], quote=False)
            header = f"<b>{message_timestamp}</b>\n\n"
            body = "\n\n".join(f"• <b>{item[1]}</b>" for item in page_lines)
            self._send(header + body)

    @staticmethod
    def _compact_log_line(line):
        """Convert 'date | level | logger | text' to just the message text."""
        parts = line.split(" | ", 2)
        if len(parts) == 3 and len(parts[0]) == 19:
            message_parts = parts[2].split(" | ", 1)
            return message_parts[1] if len(message_parts) == 2 else parts[2]
        return line

    def _send(self, text):
        try:
            requests.post(
                self.url,
                data={
                    "chat_id": self.chat_id,
                    "text": text,
                    "parse_mode": "HTML",
                    "disable_web_page_preview": "true",
                },
                timeout=10,
            ).raise_for_status()
        except Exception:
            # Never recurse into logging if Telegram itself is unavailable.
            pass


def configure_logging():
    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s",
        "%Y-%m-%d %H:%M:%S",
    )
    root = logging.getLogger()
    root.setLevel(os.getenv("LOG_LEVEL", "INFO").upper())

    console = logging.StreamHandler()
    console.setFormatter(formatter)
    root.addHandler(console)

    if TELEGRAM_BOT_TOKEN and TELEGRAM_LOG_CHAT_ID:
        telegram_handler = TelegramLogHandler(TELEGRAM_BOT_TOKEN, TELEGRAM_LOG_CHAT_ID)
        telegram_handler.setFormatter(formatter)
        root.addHandler(telegram_handler)
        root.info("Telegram log forwarding enabled")
    else:
        root.warning(
            "Telegram log forwarding is disabled: set TELEGRAM_BOT_TOKEN and "
            "TELEGRAM_LOG_CHAT_ID."
        )


async def _make_smm_request(payload):
    try:
        response = await asyncio.to_thread(
            requests.post,
            SMM_API_URL,
            data={"key": SMM_API_KEY, **payload},
            timeout=15,
        )
        if response.status_code == 200:
            return response.json()
        logging.error("SMM API request failed with status code %s: %s", response.status_code, response.text)
    except requests.exceptions.RequestException as exc:
        logging.error("SMM API request failed: %s", exc)
    return None


async def get_services():
    return await _make_smm_request({"action": "services"})


async def add_order(service_id, link, quantity):
    return await _make_smm_request(
        {"action": "add", "service": service_id, "link": link, "quantity": quantity}
    )


async def check_post_exists(client, channel_entity, post_id):
    try:
        message = await client.get_messages(channel_entity, ids=post_id)
        return message is not None and not isinstance(message, MessageEmpty) and message.id == post_id
    except Exception as exc:
        logging.warning("Error checking post %s existence: %s", post_id, exc)
        return False


async def process_post(client, channel_entity, message):
    post_id = message.id
    if post_id in active_post_tasks:
        return

    active_post_tasks[post_id] = True
    channel_username = getattr(channel_entity, "username", "unknown_channel")
    try:
        post_url = f"https://t.me/{channel_username}/{post_id}"
        logging.info("Processing post: %s", post_url)

        if not await check_post_exists(client, channel_entity, post_id):
            logging.warning("Post %s was deleted or is inaccessible; skipping", post_id)
            return

        services = await get_services()
        if not services:
            logging.warning("Could not retrieve SMM services; skipping post")
            return

        target_service = next(
            (service for service in services if service.get("service") == TARGET_SERVICE_ID), None
        )
        if not target_service:
            logging.warning("Target service ID %s was not found; skipping", TARGET_SERVICE_ID)
            return

        service_min = int(target_service.get("min", 1))
        service_max = int(target_service.get("max", 999999))
        target_views = random.randint(MIN_TOTAL_VIEWS_PER_POST, MAX_TOTAL_VIEWS_PER_POST)
        ordered_views = 0
        current_views = getattr(message, "views", 0) or 0

        if current_views >= target_views:
            logging.info("Post %s already has %s views (target %s); skipping", post_id, current_views, target_views)
            return

        remaining_views = target_views - current_views
        logging.info("Post %s: current=%s target=%s remaining=%s", post_id, current_views, target_views, remaining_views)

        while ordered_views < remaining_views:
            if not await check_post_exists(client, channel_entity, post_id):
                logging.warning("Post %s was deleted; stopping", post_id)
                return

            batch = min(
                random.randint(MIN_BATCH_SIZE_PER_REQUEST, MAX_BATCH_SIZE_PER_REQUEST),
                remaining_views - ordered_views,
                service_max,
            )
            if batch < service_min:
                if remaining_views - ordered_views >= service_min:
                    batch = service_min
                else:
                    break

            order = await add_order(TARGET_SERVICE_ID, post_url, batch)
            if order and "order" in order:
                ordered_views += batch
                logging.info("Ordered %s views for post %s (%s/%s)", batch, post_id, ordered_views, remaining_views)
            else:
                logging.error("Failed to place order for %s: %s", post_url, order)

            if ordered_views < remaining_views:
                delay = random.randint(MIN_DELAY_SECONDS_BETWEEN_REQUESTS, MAX_DELAY_SECONDS_BETWEEN_REQUESTS)
                logging.info("Waiting %sm %ss before next batch", delay // 60, delay % 60)
                start_wait = time.time()
                while time.time() - start_wait < delay:
                    await asyncio.sleep(30)
                    if not await check_post_exists(client, channel_entity, post_id):
                        logging.warning("Post %s was deleted during the wait; stopping", post_id)
                        return

        logging.info("Completed post: %s", post_url)
    except Exception:
        logging.exception("Error processing post %s in %s", post_id, channel_username)
    finally:
        active_post_tasks.pop(post_id, None)


async def catch_up_check(client):
    while True:
        try:
            if not active_post_tasks:
                logging.info("No active tasks; running catch-up check for the last 5 posts")

                async def check_single_channel(channel_username):
                    try:
                        channel_entity = await client.get_entity(channel_username)
                        async for message in client.iter_messages(channel_entity, limit=5):
                            if message.id in active_post_tasks:
                                continue
                            views = getattr(message, "views", 0) or 0
                            if views < CATCH_UP_THRESHOLD_VIEWS:
                                logging.info("Catch-up: post %s in %s has %s views; starting", message.id, channel_username, views)
                                asyncio.create_task(process_post(client, channel_entity, message))
                                await asyncio.sleep(2)
                    except ServerError as exc:
                        logging.warning("Telegram server error during catch-up for %s: %s", channel_username, exc)
                        await asyncio.sleep(random.randint(30, 60))
                    except Exception:
                        logging.exception("Error during catch-up check for %s", channel_username)

                await asyncio.gather(*(check_single_channel(username) for username in CHANNEL_USERNAMES))
            else:
                logging.info("Tasks are active; skipping catch-up check")
        except Exception:
            logging.exception("Unexpected error in catch_up_check")
        await asyncio.sleep(CATCH_UP_CHECK_INTERVAL_SECONDS)


async def new_message_handler(event, client):
    if event.message.fwd_from or event.message.via_bot_id:
        return
    channel_entity = await client.get_entity(event.message.peer_id)
    asyncio.create_task(process_post(client, channel_entity, event.message))


async def run_client():
    consecutive_persistent_errors = 0
    while True:
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        client.add_event_handler(
            lambda event: new_message_handler(event, client),
            events.NewMessage(chats=CHANNEL_USERNAMES),
        )
        catch_up_task = asyncio.create_task(catch_up_check(client))
        try:
            logging.info("Connecting to Telegram user client")
            await client.start(phone=PHONE_NUMBER)
            logging.info("Listening for new posts")
            consecutive_persistent_errors = 0
            await client.run_until_disconnected()
        except PersistentTimestampOutdatedError as exc:
            consecutive_persistent_errors += 1
            logging.warning("Persistent timestamp error (%s consecutive): %s", consecutive_persistent_errors, exc)
            if consecutive_persistent_errors >= 3:
                session_file = f"{SESSION_NAME}.session"
                if os.path.exists(session_file):
                    logging.error("Deleting stale session file: %s", session_file)
                    os.remove(session_file)
                consecutive_persistent_errors = 0
            await client.disconnect()
            await asyncio.sleep(random.randint(10, 30))
        except FloodWaitError as exc:
            logging.warning("Flood wait: sleeping %s seconds", exc.seconds)
            await client.disconnect()
            await asyncio.sleep(exc.seconds + 5)
        except ServerError as exc:
            logging.warning("Telegram server error: %s; retrying in 60 seconds", exc)
            await client.disconnect()
            await asyncio.sleep(60)
        except RPCError:
            logging.exception("Telegram RPC error; retrying")
            await client.disconnect()
            await asyncio.sleep(random.randint(10, 20))
        except Exception:
            logging.exception("Unexpected run_client error; retrying")
            await client.disconnect()
            await asyncio.sleep(random.randint(10, 20))
        finally:
            catch_up_task.cancel()


if __name__ == "__main__":
    configure_logging()
    asyncio.run(run_client())
