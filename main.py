#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import logging
import asyncio
import subprocess
from threading import Thread
from datetime import datetime
from pathlib import Path

import aiohttp
from aiohttp import TCPConnector
from flask import Flask
from pyrogram import Client, filters
from pyrogram.types import Message, BotCommand
from PIL import Image
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata

# -------------------------
# Config / Environment
# -------------------------
# Set these in your host (render/railway) environment variables
API_ID = int(os.getenv("API_ID", "0"))
API_HASH = os.getenv("API_HASH", "")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_ID = int(os.getenv("ADMIN_ID", "0"))  # your telegram id for admin commands

# Fallbacks or reminders (do not keep real tokens here)
if API_ID == 0 or not API_HASH or not BOT_TOKEN or ADMIN_ID == 0:
    logging.warning("One or more required environment variables are missing: API_ID, API_HASH, BOT_TOKEN, ADMIN_ID")

MAX_DOWNLOAD_BYTES = int(os.getenv("MAX_DOWNLOAD_BYTES", 2048 * 1024 * 1024))  # default 2GB

# -------------------------
# Logging tweaks
# -------------------------
logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(levelname)s | %(message)s")
# suppress hachoir noisy warnings
logging.getLogger("hachoir").setLevel(logging.ERROR)
# aiohttp noisy logs can be silenced if desired
logging.getLogger("aiohttp").setLevel(logging.WARNING)

# -------------------------
# Paths
# -------------------------
BASE_DIR = Path(__file__).parent.resolve()
TMP = BASE_DIR / "tmp"
TMP.mkdir(parents=True, exist_ok=True)
logging.info(f"TMP folder path: {TMP}")

# -------------------------
# Bot client
# -------------------------
app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

USER_THUMBS = {}  # uid -> thumb path (string)
LAST_FILE = {}    # uid -> last file info

# -------------------------
# Utilities
# -------------------------
def is_drive_url(url: str) -> bool:
    return "drive.google.com" in url.lower()

def extract_drive_id(url: str) -> str:
    patterns = [
        r"/d/([a-zA-Z0-9_-]+)",
        r"id=([a-zA-Z0-9_-]+)",
        r"file/d/([a-zA-Z0-9_-]+)"
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
            logging.debug("hachoir: cannot create parser for file")
            return 0
        with parser:
            metadata = extractMetadata(parser)
        if metadata and metadata.has("duration"):
            duration = metadata.get("duration").total_seconds()
            logging.debug(f"Video duration detected (hachoir): {duration} seconds")
            return int(duration)
    except Exception as e:
        logging.debug(f"Error getting duration: {e}")
    return 0

# -------------------------
# Progress helpers
# -------------------------
async def progress_callback(current, total, message: Message, start_time, task="Downloading"):
    now = datetime.now()
    diff = (now - start_time).total_seconds() or 1
    speed = current / diff  # bytes/sec
    speed_mb = speed / (1024 * 1024)
    percentage = (current * 100 / total) if total else 0
    eta = int((total - current) / speed) if speed > 0 else 0

    bars = int(percentage // 5)
    progress_str = "[" + "█" * bars + "░" * (20 - bars) + "]"
    text = (
        f"{task}...\n"
        f"{progress_str} {percentage:.2f}%\n"
        f"{current / (1024*1024):.2f}MB of {total / (1024*1024):.2f}MB\n"
        f"Speed: {speed_mb:.2f} MB/s\n"
        f"Elapsed: {int(diff)}s | ETA: {eta}s"
    )
    try:
        await message.edit_text(text)
    except Exception:
        # editing may fail if message deleted or rate limited
        pass

# -------------------------
# Download helpers
# -------------------------
async def download_stream(resp, out_path: Path, message: Message = None, start_time=None, task="Downloading"):
    total = 0
    size = int(resp.headers.get("Content-Length", 0))
    chunk_size = 256 * 1024  # 256 KB
    with out_path.open("wb") as f:
        async for chunk in resp.content.iter_chunked(chunk_size):
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_DOWNLOAD_BYTES:
                return False, "Exceeded max allowed size."
            f.write(chunk)
            if message and start_time:
                await progress_callback(total, size, message, start_time, task=task)
    return True, None

async def download_url_generic(url: str, out_path: Path, message: Message = None):
    try:
        timeout = aiohttp.ClientTimeout(total=3600)
        headers = {"User-Agent": "Mozilla/5.0 (compatible)"}
        connector = TCPConnector(limit=0, ttl_dns_cache=300)
        start_time = datetime.now()
        async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
            async with sess.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                return await download_stream(resp, out_path, message, start_time)
    except Exception as e:
        return False, str(e)

async def download_drive_file(file_id: str, out_path: Path, message: Message = None):
    base = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        timeout = aiohttp.ClientTimeout(total=3600)
        headers = {"User-Agent": "Mozilla/5.0 (compatible)"}
        connector = TCPConnector(limit=0, ttl_dns_cache=300)
        start_time = datetime.now()
        async with aiohttp.ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
            async with sess.get(base, allow_redirects=True) as resp:
                text = await resp.text(errors="ignore")
                # direct file
                if any(k.lower() == "content-disposition" for k in resp.headers.keys()):
                    async with sess.get(base) as r2:
                        return await download_stream(r2, out_path, message, start_time)
                # confirm token (large files)
                m = re.search(r"confirm=([0-9A-Za-z_-]+)", text)
                if m:
                    token = m.group(1)
                    download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                    async with sess.get(download_url, allow_redirects=True) as resp2:
                        if resp2.status != 200:
                            return False, f"HTTP {resp2.status}"
                        return await download_stream(resp2, out_path, message, start_time)
                # href pattern
                m2 = re.search(r'href="(/uc\?export=download[^"]+)"', text)
                if m2:
                    href = m2.group(1)
                    full = "https://drive.google.com" + href
                    async with sess.get(full, allow_redirects=True) as resp3:
                        if resp3.status != 200:
                            return False, f"HTTP {resp3.status}"
                        return await download_stream(resp3, out_path, message, start_time)
                # fallback
                async with sess.get(base, allow_redirects=True) as resp4:
                    if resp4.status != 200:
                        return False, f"HTTP {resp4.status}"
                    return await download_stream(resp4, out_path, message, start_time)
    except Exception as e:
        return False, str(e)

# -------------------------
# Thumbnail generation
# -------------------------
async def generate_video_thumbnail(video_path: Path, thumb_path: Path):
    try:
        duration = get_video_duration(video_path)
        timestamp = 1 if duration > 1 else 0
        cmd = [
            "ffmpeg",
            "-y",
            "-i", str(video_path),
            "-ss", str(timestamp),
            "-vframes", "1",
            "-vf", "scale=320:-1",
            str(thumb_path)
        ]
        # run blocking subprocess (ffmpeg must be installed)
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        if thumb_path.exists() and thumb_path.stat().st_size > 0:
            return True
    except Exception as e:
        logging.debug(f"Thumbnail generate error: {e}")
    return False

# -------------------------
# Upload helpers
# -------------------------
async def upload_progress(current, total, message: Message, start_time):
    await progress_callback(current, total, message, start_time, task="Uploading")

# -------------------------
# Bot commands & handlers
# -------------------------
async def set_bot_commands():
    cmds = [
        BotCommand("start", "বট চালু/হেল্প"),
        BotCommand("upload_url", "URL থেকে ফাইল ডাউনলোড ও আপলোড"),
        BotCommand("setthumb", "কাস্টম থাম্বনেইল সেট করুন"),
        BotCommand("rename", "reply করা ভিডিও রিনেম করুন"),
        BotCommand("view_thumb", "আপনার থাম্বনেইল দেখুন"),
        BotCommand("del_thumb", "আপনার থাম্বনেইল মুছে ফেলুন"),
        BotCommand("broadcast", "ব্রডকাস্ট (কেবলমাত্র অ্যাডমিন)"),
        BotCommand("help", "সহায়িকা")
    ]
    try:
        await app.set_bot_commands(cmds)
    except Exception as e:
        logging.debug(f"Set commands error: {e}")

@app.on_message(filters.command("start") & filters.private)
async def start_handler(c, m: Message):
    await set_bot_commands()
    text = (
        "Hi! আমি URL uploader bot.\n\n"
        "Commands:\n"
        "/upload_url <url> - URL থেকে ডাউনলোড ও Telegram-এ আপলোড\n"
        "/setthumb - একটি ছবি পাঠান, সেট হবে আপনার থাম্বনেইল\n"
        "/view_thumb - আপনার থাম্বনেইল দেখুন\n"
        "/del_thumb - আপনার থাম্বনেইল মুছে ফেলুন\n"
        "/rename <newname.ext> - reply করা ভিডিও রিনেম করুন\n"
        "/broadcast <text> - ব্রডকাস্ট (শুধুমাত্র অ্যাডমিন)\n"
        "/help - সাহায্য"
    )
    await m.reply_text(text)

@app.on_message(filters.command("help") & filters.private)
async def help_handler(c, m):
    await start_handler(c, m)

@app.on_message(filters.command("setthumb") & filters.private)
async def setthumb_prompt(c, m):
    await m.reply_text("একটি ছবি পাঠান (photo) — সেট হবে আপনার থাম্বনেইল।")

@app.on_message(filters.photo & filters.private)
async def photo_handler(c, m: Message):
    uid = m.from_user.id
    out = TMP / f"thumb_{uid}.jpg"
    TMP.mkdir(parents=True, exist_ok=True)
    try:
        await m.download(file_name=str(out))
        logging.info(f"Downloaded photo to: {out}")
        img = Image.open(out)
        img.thumbnail((320, 320))
        img = img.convert("RGB")
        img.save(out, "JPEG")
        if out.exists() and out.stat().st_size > 0:
            USER_THUMBS[uid] = str(out)
            await m.reply_text("আপনার থাম্বনেইল সেভ হয়েছে।")
        else:
            await m.reply_text("থাম্বনেইল সেভ হয়নি, আবার চেষ্টা করুন।")
    except Exception as e:
        await m.reply_text(f"থাম্বনেইল সেট করতে সমস্যা হয়েছে: {e}")

@app.on_message(filters.command("view_thumb") & filters.private)
async def view_thumb_cmd(c, m: Message):
    uid = m.from_user.id
    thumb_path = USER_THUMBS.get(uid)
    if thumb_path and Path(thumb_path).exists():
        await c.send_photo(chat_id=m.chat.id, photo=thumb_path, caption="এটা আপনার সেভ করা থাম্বনেইল।")
    else:
        await m.reply_text("আপনার কোনো থাম্বনেইল সেভ করা নেই। /setthumb দিয়ে সেট করুন।")

@app.on_message(filters.command("del_thumb") & filters.private)
async def del_thumb_cmd(c, m: Message):
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

@app.on_message(filters.command("upload_url") & filters.private)
async def upload_url_cmd(c, m: Message):
    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /upload_url <url>\nউদাহরণ: /upload_url https://example.com/file.mp4")
        return
    url = m.text.split(None, 1)[1].strip()
    status_msg = await m.reply_text("ডাউনলোড শুরু হচ্ছে...")

    fname = url.split("/")[-1].split("?")[0] or f"download_{int(datetime.now().timestamp())}"
    safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)

    video_exts = {".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"}
    if not any(safe_name.lower().endswith(ext) for ext in video_exts):
        safe_name += ".mp4"

    tmp_in = TMP / f"dl_{m.from_user.id}_{int(datetime.now().timestamp())}_{safe_name}"
    ok, err = False, None
    if is_drive_url(url):
        fid = extract_drive_id(url)
        if not fid:
            await status_msg.edit("Google Drive লিঙ্ক থেকে file id পাওয়া যায়নি। সঠিক লিংক দিন।")
            return
        ok, err = await download_drive_file(fid, tmp_in, status_msg)
    else:
        ok, err = await download_url_generic(url, tmp_in, status_msg)

    if not ok:
        await status_msg.edit(f"ডাউনলোড ব্যর্থ: {err}")
        try:
            tmp_in.unlink(missing_ok=True)
        except Exception:
            pass
        return

    await status_msg.edit("ডাউনলোড সম্পন্ন, Telegram-এ আপলোড হচ্ছে...")
    await process_file_and_upload(c, m, tmp_in, original_name=safe_name)

@app.on_message((filters.video | filters.document) & filters.private)
async def incoming_file_handler(c, m: Message):
    if m.forward_from or m.forward_from_chat:
        await m.reply_text("ফরোয়ার্ড করা ফাইল আপলোড সাপোর্ট করা হয় না।")
        return

    uid = m.from_user.id
    status_msg = await m.reply_text("ফাইল ডাউনলোড হচ্ছে...")
    fname = None
    if m.video:
        fname = m.video.file_name or f"video_{uid}"
    elif m.document:
        fname = m.document.file_name or f"file_{uid}"
    else:
        fname = f"file_{uid}"
    safe_name = re.sub(r"[\\/*?\"<>|:]", "_", fname)
    tmp_in = TMP / f"recv_{uid}_{int(datetime.now().timestamp())}_{safe_name}"
    await m.download(file_name=str(tmp_in))
    await status_msg.edit("ডাউনলোড সম্পন্ন — Telegram-এ আপলোড হচ্ছে...")
    await process_file_and_upload(c, m, tmp_in, original_name=safe_name)

@app.on_message(filters.command("rename") & filters.private)
async def rename_cmd(c, m: Message):
    uid = m.from_user.id
    if not m.reply_to_message:
        await m.reply_text("ফাইল রিনেম করতে হলে ভিডিও মেসেজের উপর reply দিন।")
        return

    replied = m.reply_to_message
    if not replied.video and not (replied.document and replied.document.mime_type and replied.document.mime_type.startswith("video")):
        await m.reply_text("দুঃখিত, শুধুমাত্র ভিডিও ফাইল রিনেম করা যায়।")
        return

    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /rename <newfilename.ext>\nউদাহরণ: /rename newvideo.mp4")
        return

    newname = m.text.split(None, 1)[1].strip()
    newname = re.sub(r"[\\/*?\"<>|:]", "_", newname)

    thumb_path = USER_THUMBS.get(uid)
    if thumb_path and not Path(thumb_path).exists():
        thumb_path = None

    try:
        TMP.mkdir(parents=True, exist_ok=True)
        tmp_file = TMP / f"rename_{uid}_{int(datetime.now().timestamp())}_{newname}"

        status_msg = await m.reply_text("রিনেম ভিডিও ডাউনলোড হচ্ছে...")
        start_time = datetime.now()

        # use pyrogram download_media
        await c.download_media(
            message=replied,
            file_name=str(tmp_file),
            progress=progress_callback,
            progress_args=(status_msg, start_time, "Downloading")
        )

        await status_msg.edit("রিনেম ভিডিও আপলোড শুরু হচ্ছে...")
        start_time_upload = datetime.now()

        duration_sec = get_video_duration(tmp_file) if tmp_file.exists() else 0
        logging.info(f"Rename video duration: {duration_sec} seconds")

        await c.send_video(
            chat_id=m.chat.id,
            video=str(tmp_file),
            caption=newname,
            thumb=thumb_path,
            duration=duration_sec,
            progress=upload_progress,
            progress_args=(status_msg, start_time_upload)
        )

        try:
            tmp_file.unlink(missing_ok=True)
        except Exception:
            pass

        await status_msg.edit("রিনেম ভিডিও আপলোড সম্পন্ন।")
    except Exception as e:
        await m.reply_text(f"রিনেম প্রক্রিয়ায় ত্রুটি: {e}")

@app.on_message(filters.command("broadcast") & filters.private)
async def broadcast_cmd(c, m: Message):
    if m.from_user.id != ADMIN_ID:
        await m.reply_text("দুঃখিত, এই কমান্ডটি শুধুমাত্র অ্যাডমিন ব্যবহার করতে পারেন।")
        return
    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /broadcast <message>")
        return
    text = m.text.split(None, 1)[1].strip()
    await m.reply_text("ব্রডকাস্ট শুরু হচ্ছে...")

    # NOTE: In production maintain a proper DB of user ids; for demo we broadcast to LAST_FILE keys
    users = list(LAST_FILE.keys())
    sent = 0
    failed = 0
    for uid in users:
        try:
            await c.send_message(uid, text)
            sent += 1
        except Exception:
            failed += 1
        await asyncio.sleep(0.05)  # small delay
    await m.reply_text(f"ব্রডকাস্ট শেষ হয়েছে। সফল: {sent}, ব্যর্থ: {failed}")

# -------------------------
# Process and upload file (central)
# -------------------------
async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None):
    uid = m.from_user.id
    try:
        final_name = original_name or in_path.name

        thumb_path = USER_THUMBS.get(uid)
        if thumb_path and not Path(thumb_path).exists():
            thumb_path = None

        is_video = False
        if str(in_path).lower().endswith((".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm")):
            is_video = True
            if not thumb_path:
                thumb_path_tmp = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
                ok = await generate_video_thumbnail(in_path, thumb_path_tmp)
                if ok:
                    thumb_path = str(thumb_path_tmp)

        status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...")
        start_time = datetime.now()

        duration_sec = get_video_duration(in_path) if in_path.exists() else 0
        logging.info(f"Video duration detected: {duration_sec} seconds")

        if is_video:
            await c.send_video(
                chat_id=m.chat.id,
                video=str(in_path),
                caption=final_name,
                thumb=thumb_path,
                duration=duration_sec,
                progress=upload_progress,
                progress_args=(status_msg, start_time)
            )
        else:
            await c.send_document(
                chat_id=m.chat.id,
                document=str(in_path),
                file_name=final_name,
                caption=final_name,
                progress=upload_progress,
                progress_args=(status_msg, start_time)
            )

        LAST_FILE[uid] = {"path": str(in_path), "name": final_name, "is_video": is_video, "thumb": thumb_path}
        await status_msg.edit("আপলোড সম্পন্ন।")
    except Exception as e:
        await m.reply_text(f"আপলোডে ত্রুটি: {e}")

# -------------------------
# Flask keepalive server (for platforms that require a bound port)
# -------------------------
flask_app = Flask(__name__)

@flask_app.route("/")
def home():
    return "Bot is running!"

def run_flask():
    port = int(os.environ.get("PORT", 8080))
    # set threaded=False to keep behavior predictable
    flask_app.run(host="0.0.0.0", port=port, threaded=False)

# -------------------------
# Main entry
# -------------------------
def main():
    # start flask in background so hosting platform sees a bound port
    Thread(target=run_flask, daemon=True).start()
    logging.info("Starting Pyrogram client...")
    app.run()

if __name__ == "__main__":
    main()
