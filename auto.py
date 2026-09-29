import requests
import time
import random
import asyncio
import logging
import os
from telethon import TelegramClient, events
from telethon.tl.types import MessageEmpty
from telethon.errors import PersistentTimestampOutdatedError, FloodWaitError, RPCError, ServerError

# Configure logging
logging.basicConfig(format='[%(levelname) 5s/%(asctime)s] %(name)s: %(message)s', level=logging.INFO)

# --- Telegram Configuration ---
API_ID = 23877053
API_HASH = "989c360358b981dae46a910693ab2f4c"
PHONE_NUMBER = "+917511190745"
SESSION_NAME = "telegram_bot_session"
CHANNEL_USERNAMES = ["@darktunnel_data", "darktunnelfreeconfig", "@teamvrx", "@vrxtricks"]

# --- SMM Panel Configuration ---
TARGET_SERVICE_ID = 1160
MIN_BATCH_SIZE_PER_REQUEST = 100
MAX_BATCH_SIZE_PER_REQUEST = 125 # Slightly increased to handle the target within 4-5 hours

# To achieve ~1250 views (average of 1k-1.5k) in 4.5 hours (270 mins):
# Total batches needed: 1250 / 22.5 (avg batch) ≈ 55 batches
# Total delay needed: 270 mins / 55 batches ≈ 4.9 mins per batch
MIN_DELAY_SECONDS_BETWEEN_REQUESTS = 640 # 4 minutes
MAX_DELAY_SECONDS_BETWEEN_REQUESTS = 3220 # 7 minutes

MIN_TOTAL_VIEWS_PER_POST = 700
MAX_TOTAL_VIEWS_PER_POST = 800
CATCH_UP_THRESHOLD_VIEWS = 400
CATCH_UP_CHECK_INTERVAL_SECONDS = 30 # Check every 30 seconds when idle

# --- API Configuration ---
API_KEY = "89b76130404c615e84d4a9b7edcd6c9f"
API_URL = "https://cheapestsmmpanels.com/api/v2"

active_post_tasks = {}

async def _make_smm_request(payload):
    try:
        response = await asyncio.to_thread(requests.post, API_URL, data=payload, timeout=15)
        if response.status_code == 200:
            return response.json()
        else:
            logging.error(f"SMM API request failed with status code {response.status_code}: {response.text}")
    except requests.exceptions.RequestException as e:
        logging.error(f"SMM API request failed: {e}")
    return None

async def get_services():
    return await _make_smm_request({"key": API_KEY, "action": "services"})

async def add_order(service_id, link, quantity):
    return await _make_smm_request({"key": API_KEY, "action": "add", "service": service_id, "link": link, "quantity": quantity})

async def check_post_exists(client, channel_entity, post_id):
    try:
        message = await client.get_messages(channel_entity, ids=post_id)
        if message is None or isinstance(message, MessageEmpty):
            return False
        return message.id == post_id
    except Exception as e:
        logging.warning(f"Error checking post {post_id} existence: {e}")
        return False

async def process_post(client, channel_entity, message):
    post_id = message.id
    if post_id in active_post_tasks:
        return
    
    active_post_tasks[post_id] = True
    channel_username = 'unknown'
    try:
        channel_username = getattr(channel_entity, 'username', 'unknown_channel')
        post_url = f"https://t.me/{channel_username}/{post_id}"

        print(f"\n[+] Processing Post: {post_url}")
        
        if not await check_post_exists(client, channel_entity, post_id):
            print(f"    [!] Post {post_id} already deleted or inaccessible. Skipping.")
            return

        services = await get_services()
        if not services:
            logging.warning("Could not retrieve SMM services. Skipping post processing.")
            return

        target_service = next((s for s in services if s.get("service") == TARGET_SERVICE_ID), None)
        if not target_service:
            logging.warning(f"Target service ID {TARGET_SERVICE_ID} not found. Skipping post processing.")
            return

        service_min = int(target_service.get("min", 1))
        service_max = int(target_service.get("max", 999999))
        
        target_views = random.randint(MIN_TOTAL_VIEWS_PER_POST, MAX_TOTAL_VIEWS_PER_POST)
        ordered_views = 0

        # If it's a catch-up post, we might already have some views
        current_views = getattr(message, 'views', 0) or 0
        if current_views >= target_views:
            print(f"    [!] Post already has {current_views} views (Target: {target_views}). Skipping.")
            return
        
        # Adjust target if some views already exist
        remaining_views = target_views - current_views
        print(f"    -> Current Views: {current_views}, Target: {target_views}, Remaining: {remaining_views}")

        while ordered_views < remaining_views:
            if not await check_post_exists(client, channel_entity, post_id):
                print(f"    [!] Post {post_id} deleted. Stopping.")
                return

            batch = random.randint(MIN_BATCH_SIZE_PER_REQUEST, MAX_BATCH_SIZE_PER_REQUEST)
            batch = min(batch, remaining_views - ordered_views, service_max)
            
            if batch < service_min:
                if (remaining_views - ordered_views) >= service_min:
                    batch = service_min
                else:
                    break

            order = await add_order(TARGET_SERVICE_ID, post_url, batch)
            if order and "order" in order:
                ordered_views += batch
                print(f"    -> Ordered {batch} views (Total ordered: {ordered_views}/{remaining_views})")
            else:
                logging.error(f"Failed to place order for {batch} views on {post_url}. SMM response: {order}")
            
            if ordered_views < remaining_views:
                delay = random.randint(MIN_DELAY_SECONDS_BETWEEN_REQUESTS, MAX_DELAY_SECONDS_BETWEEN_REQUESTS)
                print(f"    -> Waiting {delay//60}m {delay%60}s before next batch...")
                
                start_wait = time.time()
                while time.time() - start_wait < delay:
                    await asyncio.sleep(30)
                    if not await check_post_exists(client, channel_entity, post_id):
                        print(f"    [!] Post {post_id} in {channel_username} was deleted. Stopping.")
                        return
        
        print(f"[#] Completed: {post_url}")

    except Exception as e:
        logging.exception(f"Error processing post {post_id} in {channel_username}")
    finally:
        if post_id in active_post_tasks:
            del active_post_tasks[post_id]

async def catch_up_check(client):
    while True:
        try:
            if not active_post_tasks:
                logging.info("[*] No active tasks. Running catch-up check for last 5 posts...")
                async def check_single_channel(channel_username):
                    try:
                        channel_entity = await client.get_entity(channel_username)
                        async for message in client.iter_messages(channel_entity, limit=5):
                            if message.id in active_post_tasks:
                                continue
                            
                            views = getattr(message, 'views', 0) or 0
                            if views < CATCH_UP_THRESHOLD_VIEWS:
                                logging.info(f"[*] Catch-up: Post {message.id} in {channel_username} has only {views} views. Starting order.")
                                asyncio.create_task(process_post(client, channel_entity, message))
                                # Small delay between starting tasks to avoid overwhelming
                                await asyncio.sleep(2)
                    except ServerError as e:
                        logging.warning(f"Telegram Server Error (500) during catch-up for {channel_username}. Backing off... {e}")
                        await asyncio.sleep(random.randint(30, 60))
                    except Exception as e:
                        logging.error(f"Error during catch-up check for {channel_username}: {e}")

                # Process all channels simultaneously
                await asyncio.gather(*(check_single_channel(username) for username in CHANNEL_USERNAMES))
            else:
                logging.info("[*] Tasks are currently active. Skipping catch-up check.")
        except Exception as e:
            logging.error(f"Unexpected error in catch_up_check: {e}")
        
        await asyncio.sleep(CATCH_UP_CHECK_INTERVAL_SECONDS)

async def new_message_handler(event, client):
    if event.message.fwd_from or event.message.via_bot_id: return
    channel_entity = await client.get_entity(event.message.peer_id)
    asyncio.create_task(process_post(client, channel_entity, event.message))

async def run_client():
    consecutive_persistent_errors = 0
    while True:
        client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
        client.add_event_handler(lambda e: new_message_handler(e, client), events.NewMessage(chats=CHANNEL_USERNAMES))
        
        # Start catch-up task
        catch_up_task = asyncio.create_task(catch_up_check(client))
        
        try:
            logging.info("[+] Connecting to Telegram...")
            await client.start(phone=PHONE_NUMBER)
            logging.info("[+] Listening for new posts...")
            consecutive_persistent_errors = 0
            await client.run_until_disconnected()
        except PersistentTimestampOutdatedError as e:
            consecutive_persistent_errors += 1
            logging.warning(f"PersistentTimestampOutdatedError caught: {e}. Consecutive errors: {consecutive_persistent_errors}")
            if consecutive_persistent_errors >= 3:
                session_file = f"{SESSION_NAME}.session"
                if os.path.exists(session_file):
                    logging.error(f"Too many consecutive PersistentTimestampOutdatedError. Deleting session file: {session_file}")
                    os.remove(session_file)
                consecutive_persistent_errors = 0
            await client.disconnect()
            await asyncio.sleep(random.randint(10, 30))
        except FloodWaitError as e:
            logging.warning(f"FloodWaitError caught: {e}. Waiting for {e.seconds} seconds.")
            await client.disconnect()
            await asyncio.sleep(e.seconds + 5)
        except ServerError as e:
            logging.warning(f"Telegram Server Error (500) caught: {e}. Waiting 60s for server recovery.")
            await client.disconnect()
            await asyncio.sleep(60)
        except RPCError as e:
            logging.error(f"Telethon RPC Error caught: {e}. Disconnecting and retrying.")
            await client.disconnect()
            await asyncio.sleep(random.randint(10, 20))
        except Exception as e:
            logging.exception(f"An unexpected error occurred in run_client: {e}. Disconnecting and retrying.")
            await client.disconnect()
            await asyncio.sleep(random.randint(10, 20))
        finally:
            catch_up_task.cancel()

if __name__ == "__main__":
    asyncio.run(run_client())
