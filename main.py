#!/usr/bin/env python3
import os
import re
import aiohttp
import asyncio
import threading
from pathlib import Path
from datetime import datetime, timedelta
from pyrogram import Client, filters
from pyrogram.types import Message, BotCommand, InlineKeyboardMarkup, InlineKeyboardButton
from PIL import Image
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
import subprocess
import traceback
from flask import Flask
import time
import math
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# env
API_ID = int(os.getenv("API_ID"))
API_HASH = os.getenv("API_HASH")
BOT_TOKEN = os.getenv("BOT_TOKEN")
PORT = int(os.getenv("PORT", "5000"))

TMP = Path("tmp")
TMP.mkdir(parents=True, exist_ok=True)

# state
USER_THUMBS = {}           # uid -> thumb path
LAST_FILE = {}             # uid -> last file metadata
TASKS = {}                 # uid -> list of asyncio.Event (cancel events)
SET_THUMB_REQUEST = set()  # uid set that recently ran /setthumb (next photo is explicit)
SUBSCRIBERS = set()        # chat ids who started the bot (for broadcast)

ADMIN_ID = int(os.getenv("ADMIN_ID", ""))  # change if env provided
MAX_SIZE = 2 * 1024 * 1024 * 1024  # 2GB

app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
flask_app = Flask(__name__)

# ---- utilities ----
def is_admin(uid: int) -> bool:
    return uid == ADMIN_ID

def is_drive_url(url: str) -> bool:
    return "drive.google.com" in url or "docs.google.com" in url

def extract_drive_id(url: str) -> str:
    # common patterns
    patterns = [
        r"/d/([a-zA-Z0-9_-]+)",
        r"id=([a-zA-Z0-9_-]+)",
        r"open\?id=([a-zA-Z0-9_-]+)",
        r"https://drive.google.com/file/d/([a-zA-Z0-9_-]+)/"
    ]
    for p in patterns:
        m = re.search(p, url)
        if m:
            return m.group(1)
    return None

def get_video_duration(file_path: Path) -> int:
    try:
        parser = createParser(str(file_path))
        if not parser:
            return 0
        with parser:
            metadata = extractMetadata(parser)
        if metadata and metadata.has("duration"):
            return int(metadata.get("duration").total_seconds())
    except Exception:
        return 0
    return 0

def progress_keyboard():
    return InlineKeyboardMarkup([[InlineKeyboardButton("Cancel ❌", callback_data="cancel_task")]])

# ---- progress callback helpers ----
async def progress_callback(current, total, message: Message, start_time, task="Progress"):
    try:
        now = datetime.now()
        diff = (now - start_time).total_seconds()
        if diff < 0.001:
            diff = 0.001
        percentage = (current * 100 / total) if total else 0
        speed = (current / diff / 1024 / 1024) if diff else 0  # MB/s
        elapsed = int(diff)
        eta = int((total - current) / (current / diff)) if current and diff and (current / diff) else 0

        done_blocks = int(percentage // 5)
        done_blocks = max(0, min(20, done_blocks))
        progress_bar = ("█" * done_blocks).ljust(20, "░")
        text = (
            f"{task}...\n"
            f"[{progress_bar}] {percentage:.2f}%\n"
            f"{current / 1024 / 1024:.2f}MB of { (total / 1024 / 1024) if total else 0:.2f}MB\n"
            f"Speed: {speed:.2f} MB/s\n"
            f"Elapsed: {elapsed}s | ETA: {eta}s\n\n"
            "আপলোড/ডাউনলোড বাতিল করতে নিচের বাটনে চাপুন।"
        )
        try:
            await message.edit_text(text, reply_markup=progress_keyboard())
        except Exception:
            pass
    except Exception:
        pass

def pyrogram_progress_wrapper(current, total, message_obj, start_time_obj, task_str="Progress"):
    try:
        loop = asyncio.get_event_loop()
        try:
            asyncio.run_coroutine_threadsafe(progress_callback(current, total, message_obj, start_time_obj, task=task_str), loop)
        except Exception:
            try:
                loop.create_task(progress_callback(current, total, message_obj, start_time_obj, task=task_str))
            except Exception:
                pass
    except RuntimeError:
        pass

# ---- robust download stream with retries ----
async def download_stream(resp, out_path: Path, message: Message = None, start_time=None, task="Downloading", cancel_event: asyncio.Event = None):
    total = 0
    try:
        size = int(resp.headers.get("Content-Length", 0))
    except:
        size = 0
    chunk_size = 1024 * 1024
    try:
        with out_path.open("wb") as f:
            async for chunk in resp.content.iter_chunked(chunk_size):
                if cancel_event and cancel_event.is_set():
                    return False, "অপারেশন ব্যবহারকারী দ্বারা বাতিল করা হয়েছে।"
                if not chunk:
                    break
                total += len(chunk)
                if total > MAX_SIZE:
                    return False, "ফাইলের সাইজ 2GB এর বেশি হতে পারে না।"
                f.write(chunk)
                if message and start_time:
                    await progress_callback(total, size, message, start_time, task=task)
    except Exception as e:
        return False, str(e)
    return True, None

async def fetch_with_retries(session, url, method="GET", max_tries=3, **kwargs):
    backoff = 1
    for attempt in range(1, max_tries + 1):
        try:
            resp = await session.request(method, url, **kwargs)
            return resp
        except Exception as e:
            if attempt == max_tries:
                raise
            await asyncio.sleep(backoff)
            backoff *= 2
    raise RuntimeError("unreachable")

async def download_url_generic(url: str, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    timeout = aiohttp.ClientTimeout(total=7200)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
        try:
            async with sess.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                return await download_stream(resp, out_path, message, datetime.now(), task="Downloading", cancel_event=cancel_event)
        except Exception as e:
            return False, str(e)

async def download_drive_file(file_id: str, out_path: Path, message: Message = None, cancel_event: asyncio.Event = None):
    base = f"https://drive.google.com/uc?export=download&id={file_id}"
    timeout = aiohttp.ClientTimeout(total=7200)
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64)"}
    connector = aiohttp.TCPConnector(limit=0, force_close=True)
    async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
        try:
            async with sess.get(base, allow_redirects=True) as resp:
                if resp.status == 200 and "content-disposition" in (k.lower() for k in resp.headers.keys()):
                    return await download_stream(resp, out_path, message, datetime.now(), task="Downloading", cancel_event=cancel_event)
                text = await resp.text(errors="ignore")
                m = re.search(r"confirm=([0-9A-Za-z-_]+)", text)
                if m:
                    token = m.group(1)
                    download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                    async with sess.get(download_url, allow_redirects=True) as resp2:
                        if resp2.status != 200:
                            return False, f"HTTP {resp2.status}"
                        return await download_stream(resp2, out_path, message, datetime.now(), task="Downloading", cancel_event=cancel_event)
                for k, v in resp.cookies.items():
                    if k.startswith("download_warning"):
                        token = v.value
                        download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                        async with sess.get(download_url, allow_redirects=True) as resp2:
                            if resp2.status != 200:
                                return False, f"HTTP {resp2.status}"
                            return await download_stream(resp2, out_path, message, datetime.now(), task="Downloading", cancel_event=cancel_event)
                return False, "ডাউনলোডের জন্য Google Drive থেকে অনুমতি প্রয়োজন বা লিংক পাবলিক নয়।"
        except Exception as e:
            return False, str(e)

async def set_bot_commands():
    cmds = [
        BotCommand("start", "বট চালু/হেল্প"),
        BotCommand("upload_url", "URL থেকে ফাইল ডাউনলোড ও আপলোড (admin only)"),
        BotCommand("setthumb", "কাস্টম থাম্বনেইল সেট করুন (admin only)"),
        BotCommand("view_thumb", "আপনার থাম্বনেইল দেখুন (admin only)"),
        BotCommand("del_thumb", "আপনার থাম্বনেইল মুছে ফেলুন (admin only)"),
        BotCommand("rename", "reply করা ভিডিও রিনেম করুন (admin only)"),
        BotCommand("broadcast", "ব্রডকাস্ট (কেবল অ্যাডমিন)"),
        BotCommand("help", "সহায়িকা")
    ]
    try:
        await app.set_bot_commands(cmds)
    except Exception as e:
        logger.warning("Set commands error: %s", e)

# ---- handlers ----
@app.on_message(filters.command("start") & filters.private)
async def start_handler(c, m: Message):
    await set_bot_commands()
    SUBSCRIBERS.add(m.chat.id)
    text = (
        "Hi! আমি URL uploader bot.\n\n"
        "নোট: বটের অনেক কমান্ড শুধু অ্যাডমিন (owner) চালাতে পারবে।\n\n"
        "Commands:\n"
        "/upload_url <url> - URL থেকে ডাউনলোড ও Telegram-এ আপলোড (admin only)\n"
        "/setthumb - একটি ছবি পাঠান, সেট হবে আপনার থাম্বনেইল (admin only)\n"
        "/view_thumb - আপনার থাম্বনেইল দেখুন (admin only)\n"
        "/del_thumb - আপনার থাম্বনেইল মুছে ফেলুন (admin only)\n"
        "/rename <newname.ext> - reply করা ভিডিও রিনেম করুন (admin only)\n"
        "/broadcast <text> - ব্রডকাস্ট (শুধুমাত্র অ্যাডমিন)\n"
        "/help - সাহায্য"
    )
    await m.reply_text(text)

@app.on_message(filters.command("help") & filters.private)
async def help_handler(c, m):
    await start_handler(c, m)

@app.on_message(filters.command("setthumb") & filters.private)
async def setthumb_prompt(c, m):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    SET_THUMB_REQUEST.add(m.from_user.id)
    await m.reply_text("একটি ছবি পাঠান (photo) — সেট হবে আপনার থাম্বনেইল।")

@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    if thumb_path and Path(thumb_path).exists():
        await c.send_photo(chat_id=m.chat.id, photo=thumb_path, caption="এটা আপনার সেভ করা থাম্বনেইল।")
    else:
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই। /setthumb দিয়ে সেট করুন।")

@app.on_message(filters.command("del_thumb") & filters.private)
async def del_thumb_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    if thumb_path and Path(thumb_path).exists():
        try:
            Path(thumb_path).unlink()
        except Exception:
            pass
        USER_THUMBS.pop(uid, None)
        await m.reply_text("আপনার থাম্বনেইল মুছে ফেলা হয়েছে।")
    else:
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই।")

@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    uid = m.from_user.id
    out = TMP / f"thumb_{uid}.jpg"
    try:
        await m.download(file_name=str(out))
        img = Image.open(out)
        img.thumbnail((320, 320))
        img = img.convert("RGB")
        img.save(out, "JPEG")
        USER_THUMBS[uid] = str(out)
        if uid in SET_THUMB_REQUEST:
            SET_THUMB_REQUEST.discard(uid)
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        else:
            await m.reply_text("অটো থাম্বনেইল সেভ হয়েছে।")
    except Exception as e:
        await m.reply_text(f"থাম্বনেইল সেভ করতে সমস্যা: {e}")

@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_cmd(c, m: Message):
    if not is_admin(m.from_user.id):
        await m.reply_text("আপনার অনুমতি নেই এই কমান্ড চালানোর।")
        return
    if not m.command or len(m.command) < 2:
        await m.reply_text("ব্যবহার: /upload_url <url>\nউদাহরণ: /upload_url https://example.com/file.mp4")
        return
    url = m.text.split(None, 1)[1].strip()
    asyncio.create_task(handle_url_download_and_upload(c, m, url))

@app.on_message(filters.text & filters.private)
async def auto_url_upload(c, m: Message):
    if not is_admin(m.from_user.id):
        return
    text = m.text.strip()
    if text.startswith("http://") or text.startswith("https://"):
        asyncio.create_task(handle_url_download_and_upload(c, m, text))

async def handle_url_download_and_upload(c: Client, m: Message, url: str):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)

    status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
    try:
        fname = url.split("/")[-1].split("?")[0] or f"download_{int(datetime.now().timestamp())}"
        safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)

        video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
        if not any(safe_name.lower().endswith(ext) for ext in video_exts):
            safe_name += ".mp4"

        tmp_in = TMP / f"dl_{uid}_{int(datetime.now().timestamp())}_{safe_name}"
        ok, err = False, None

        if is_drive_url(url):
            fid = extract_drive_id(url)
            if not fid:
                await status_msg.edit("Google Drive লিঙ্ক থেকে file id পাওয়া যায়নি। সঠিক লিংক দিন।", reply_markup=None)
                TASKS[uid].remove(cancel_event)
                return
            ok, err = await download_drive_file(fid, tmp_in, status_msg, cancel_event=cancel_event)
        else:
            ok, err = await download_url_generic(url, tmp_in, status_msg, cancel_event=cancel_event)

        if not ok:
            await status_msg.edit(f"ডাউনলোড ব্যর্থ: {err}", reply_markup=None)
            try:
                if tmp_in.exists():
                    tmp_in.unlink()
            except:
                pass
            TASKS[uid].remove(cancel_event)
            return

        await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_in, original_name=safe_name)
    except Exception as e:
        traceback.print_exc()
        await status_msg.edit(f"অপস! কিছু ভুল হয়েছে: {e}", reply_markup=None)
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

# --- CHANGED: FORWARDED DOCUMENT AND VIDEO HANDLER ---
@app.on_message((filters.video | filters.document) & filters.private & filters.forwarded)
async def forwarded_media_handler(c: Client, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        return
    
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)

    original_name = "forwarded_file.bin" # fallback
    if m.video and m.video.file_name:
        original_name = m.video.file_name
    elif m.document and m.document.file_name:
        original_name = m.document.file_name
    
    sanitized_name = re.sub(r'[\\/*?:"<>|]', "_", original_name)
    tmp_media_path = TMP / f"new_media_{uid}_{int(datetime.now().timestamp())}_{sanitized_name}"
    
    status_msg = await m.reply_text(f"ফরোয়ার্ড করা ফাইল (`{original_name}`) ডাউনলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
    try:
        start_time = datetime.now()
        await m.download(file_name=str(tmp_media_path),
                         progress=pyrogram_progress_wrapper,
                         progress_args=(status_msg, start_time, "Downloading"))
        await status_msg.edit("ডাউনলোড সম্পন্ন, এখন Telegram-এ আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_media_path, original_name=sanitized_name)
    except Exception as e:
        await m.reply_text(f"ফাইল প্রসেসিংয়ে সমস্যা: {e}")
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass


@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    if not m.reply_to_message or not (m.reply_to_message.video or m.reply_to_message.document):
        await m.reply_text("ভিডিও/ডকুমেন্ট ফাইলের reply দিয়ে এই কমান্ড দিন।\nUsage: /rename new_name.mp4")
        return
    if len(m.command) < 2:
        await m.reply_text("নতুন ফাইল নাম দিন। উদাহরণ: /rename new_video.mp4")
        return
    new_name = m.text.split(None, 1)[1].strip()
    new_name = re.sub(r"[\\/*?\"<>|:]", "_", new_name)
    await m.reply_text(f"ফাইল রিনেম করা হবে: {new_name}\n(রিনেম করতে reply করা ফাইলটি পুনরায় ডাউনলোড করে আপলোড করা হবে)")

    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    tmp_out = TMP / f"rename_{uid}_{int(datetime.now().timestamp())}_{new_name}"
    status_msg = await m.reply_text("রিনেমের জন্য ফাইল ডাউনলোড করা হচ্ছে...", reply_markup=progress_keyboard())
    try:
        start_time = datetime.now()
        await m.reply_to_message.download(file_name=str(tmp_out), progress=pyrogram_progress_wrapper, progress_args=(status_msg, start_time, "Downloading"))
        await status_msg.edit("ডাউনলোড সম্পন্ন, এখন নতুন নাম দিয়ে আপলোড হচ্ছে...", reply_markup=None)
        await process_file_and_upload(c, m, tmp_out, original_name=new_name)
    except Exception as e:
        await m.reply_text(f"রিনেম ত্রুটি: {e}")
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

@app.on_callback_query(filters.regex("cancel_task"))
async def cancel_task_cb(c, cb):
    uid = cb.from_user.id
    if uid in TASKS and TASKS[uid]:
        for ev in list(TASKS[uid]):
            try:
                ev.set()
            except:
                pass
        await cb.answer("অপারেশন বাতিল করা হয়েছে।", show_alert=True)
    else:
        await cb.answer("কোনো অপারেশন চলছে না।", show_alert=True)

# ---- main processing and upload ----
async def generate_video_thumbnail(video_path: Path, thumb_path: Path):
    try:
        duration = get_video_duration(video_path)
        timestamp = 1 if duration > 1 else 0
        cmd = [
            "ffmpeg", "-y", "-i", str(video_path), "-ss", str(timestamp),
            "-vframes", "1", "-vf", "scale=320:-1", str(thumb_path)
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return thumb_path.exists() and thumb_path.stat().st_size > 0
    except Exception as e:
        logger.warning("Thumbnail generate error: %s", e)
        return False

async def upload_progress_async(current, total, message: Message, start_time):
    await progress_callback(current, total, message, start_time, task="Uploading")

def pyrogram_upload_wrapper(current, total, message_obj, start_time_obj):
    try:
        loop = asyncio.get_event_loop()
        try:
            asyncio.run_coroutine_threadsafe(upload_progress_async(current, total, message_obj, start_time_obj), loop)
        except Exception:
            try:
                loop.create_task(upload_progress_async(current, total, message_obj, start_time_obj))
            except Exception:
                pass
    except RuntimeError:
        pass

async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None):
    uid = m.from_user.id
    cancel_event = asyncio.Event()
    TASKS.setdefault(uid, []).append(cancel_event)
    try:
        final_name = original_name or in_path.name
        thumb_path = USER_THUMBS.get(uid)
        if thumb_path and not Path(thumb_path).exists():
            thumb_path = None

        is_video = in_path.suffix.lower() in {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}

        if is_video and not thumb_path:
            thumb_path_tmp = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            ok = await generate_video_thumbnail(in_path, thumb_path_tmp)
            if ok:
                thumb_path = str(thumb_path_tmp)

        status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...", reply_markup=progress_keyboard())
        if cancel_event.is_set():
            await status_msg.edit("অপারেশন বাতিল করা হয়েছে, আপলোড শুরু করা হয়নি।", reply_markup=None)
            TASKS[uid].remove(cancel_event)
            return
        start_time = datetime.now()

        duration_sec = get_video_duration(in_path) if in_path.exists() else 0
        upload_attempts = 3
        last_exc = None
        for attempt in range(1, upload_attempts + 1):
            try:
                if is_video:
                    await c.send_video(
                        chat_id=m.chat.id, video=str(in_path), caption=final_name,
                        thumb=thumb_path, duration=duration_sec, progress=pyrogram_upload_wrapper,
                        progress_args=(status_msg, start_time), supports_streaming=True
                    )
                else:
                    await c.send_document(
                        chat_id=m.chat.id, document=str(in_path), file_name=final_name,
                        caption=final_name, progress=pyrogram_upload_wrapper,
                        progress_args=(status_msg, start_time)
                    )
                await status_msg.edit("আপলোড সম্পন্ন।", reply_markup=None)
                LAST_FILE[uid] = {"path": str(in_path), "name": final_name, "is_video": is_video, "thumb": thumb_path, "ts": datetime.now().isoformat()}
                last_exc = None
                break
            except Exception as e:
                last_exc = e
                logger.warning("Upload attempt %s failed: %s", attempt, e)
                await asyncio.sleep(2 * attempt)
                if cancel_event.is_set():
                    await status_msg.edit("অপারেশন বাতিল করা হয়েছে।", reply_markup=None)
                    break

        if last_exc:
            await status_msg.edit(f"আপলোড ব্যর্থ: {last_exc}", reply_markup=None)
    except Exception as e:
        await m.reply_text(f"আপলোডে ত্রুটি: {e}")
    finally:
        try:
            TASKS[uid].remove(cancel_event)
        except Exception:
            pass

# --- CHANGED: ENHANCED BROADCAST COMMAND ---
@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(c, m: Message):
    uid = m.from_user.id
    if not is_admin(uid):
        await m.reply_text("আপনার অনুমতি নেই।")
        return
    
    broadcast_msg_to_copy = m.reply_to_message
    broadcast_text = m.text.split(None, 1)[1] if len(m.command) > 1 else None

    if not broadcast_msg_to_copy and not broadcast_text:
        await m.reply_text("ব্যবহার:\n- একটি মেসেজে reply করে `/broadcast` লিখুন। (ছবি, ভিডিও, টেক্সট বা যেকোনো ফাইল ব্রডকাস্ট করতে)\n- অথবা, `/broadcast Your message here` এভাবে টেক্সট পাঠান।")
        return

    await m.reply_text(f"ব্রডকাস্ট শুরু হচ্ছে {len(SUBSCRIBERS)} সাবস্ক্রাইবার-এ...")
    failed = 0
    sent = 0
    for chat_id in list(SUBSCRIBERS):
        try:
            if broadcast_msg_to_copy:
                await broadcast_msg_to_copy.copy(chat_id)
            elif broadcast_text:
                await app.send_message(chat_id, broadcast_text)
            
            sent += 1
            await asyncio.sleep(0.08) # To avoid flood limits
        except Exception as e:
            failed += 1
            logger.warning("Broadcast to %s failed: %s", chat_id, e)
            # Optionally remove user on failure
            # SUBSCRIBERS.discard(chat_id)
    await m.reply_text(f"ব্রডকাস্ট শেষ।\n✅ সফলভাবে পাঠানো হয়েছে: {sent}\n❌ পাঠাতে ব্যর্থ: {failed}")


# Flask route to keep web service port open
@flask_app.route("/")
def home():
    return "Bot is running (Flask alive)."

def run_flask():
    flask_app.run(host="0.0.0.0", port=PORT)

async def periodic_cleanup():
    while True:
        try:
            now = datetime.now()
            for p in TMP.iterdir():
                try:
                    if p.is_file():
                        if now - datetime.fromtimestamp(p.stat().st_mtime) > timedelta(days=3):
                            p.unlink()
                except Exception:
                    pass
        except Exception:
            pass
        await asyncio.sleep(3600)

if __name__ == "__main__":
    print("Bot চালু হচ্ছে... Flask thread start করা হচ্ছে, তারপর Pyrogram চালু হবে।")
    t = threading.Thread(target=run_flask, daemon=True)
    t.start()
    try:
        loop = asyncio.get_event_loop()
        loop.create_task(periodic_cleanup())
    except RuntimeError:
        pass
    app.run()
