#!/usr/bin/env python3
import os
import re
import asyncio
from pathlib import Path
from datetime import datetime

from pyrogram import Client, filters, idle
from pyrogram.types import Message, BotCommand
from PIL import Image
from hachoir.parser import createParser
from hachoir.metadata import extractMetadata
from aiohttp import web, ClientSession, ClientTimeout, TCPConnector

# ---- CONFIG (from env) ----
API_ID = int(os.environ.get("API_ID") or 0)
API_HASH = os.environ.get("API_HASH") or ""
BOT_TOKEN = os.environ.get("BOT_TOKEN") or ""
ADMIN_ID = int(os.environ.get("ADMIN_ID") or 0)
PORT = int(os.environ.get("PORT", 8080))

if not (API_ID and API_HASH and BOT_TOKEN):
    raise RuntimeError("Missing required environment variables: API_ID, API_HASH, BOT_TOKEN")

# ---- PATHS & GLOBALS ----
BASE_DIR = Path(__file__).parent.resolve()
TMP = BASE_DIR / "tmp"
TMP.mkdir(parents=True, exist_ok=True)

USER_THUMBS = {}
LAST_FILE = {}
MAX_DOWNLOAD_BYTES = 2048 * 1024 * 1024  # 2GB

# ---- Pyrogram client ----
app = Client("mybot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)


# ---- Utilities ----
def is_drive_url(url: str) -> bool:
    return "drive.google.com" in url


def extract_drive_id(url: str) -> str:
    patterns = [r"/d/([a-zA-Z0-9_-]+)", r"id=([a-zA-Z0-9_-]+)", r"file/d/([a-zA-Z0-9_-]+)"]
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
    except Exception as e:
        print("get_video_duration error:", e)
    return 0


async def progress_callback(current, total, message: Message, start_time, task="Downloading"):
    now = datetime.now()
    diff = (now - start_time).total_seconds() or 1
    speed = current / diff
    speed_mb = speed / (1024 * 1024)
    percentage = (current * 100 / total) if total else 0
    elapsed = int(diff)
    eta = int((total - current) / speed) if speed > 0 and total else 0
    bars = int(percentage // 5)
    progress_str = "[" + "█" * bars + "░" * (20 - bars) + "]"
    text = (
        f"{task}...\n"
        f"{progress_str} {percentage:.2f}%\n"
        f"{current / (1024*1024):.2f}MB of {total / (1024*1024):.2f}MB\n"
        f"Speed: {speed_mb:.2f} MB/s\n"
        f"Elapsed: {elapsed}s | ETA: {eta}s"
    )
    try:
        await message.edit_text(text)
    except Exception:
        pass


async def download_stream(resp, out_path: Path, message: Message = None, start_time=None, task="Downloading"):
    total = 0
    size = int(resp.headers.get("Content-Length", 0))
    chunk_size = 256 * 1024
    with out_path.open("wb") as f:
        async for chunk in resp.content.iter_chunked(chunk_size):
            if not chunk:
                break
            total += len(chunk)
            if total > MAX_DOWNLOAD_BYTES:
                return False, "Exceeded max allowed size (2GB)."
            f.write(chunk)
            if message and start_time:
                await progress_callback(total, size, message, start_time, task=task)
    return True, None


async def download_url_generic(url: str, out_path: Path, message: Message = None):
    try:
        timeout = ClientTimeout(total=3600)
        headers = {"User-Agent": "Mozilla/5.0"}
        connector = TCPConnector(limit=0, ttl_dns_cache=300)
        start_time = datetime.now()
        async with ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
            async with sess.get(url, allow_redirects=True) as resp:
                if resp.status != 200:
                    return False, f"HTTP {resp.status}"
                return await download_stream(resp, out_path, message, start_time)
    except Exception as e:
        return False, str(e)


async def download_drive_file(file_id: str, out_path: Path, message: Message = None):
    base = f"https://drive.google.com/uc?export=download&id={file_id}"
    try:
        timeout = ClientTimeout(total=3600)
        headers = {"User-Agent": "Mozilla/5.0"}
        connector = TCPConnector(limit=0, ttl_dns_cache=300)
        start_time = datetime.now()
        async with ClientSession(timeout=timeout, headers=headers, connector=connector) as sess:
            async with sess.get(base, allow_redirects=True) as resp:
                text = await resp.text(errors="ignore")
                if "content-disposition" in (k.lower() for k in resp.headers.keys()):
                    async with sess.get(base) as r2:
                        return await download_stream(r2, out_path, message, start_time)
                m = re.search(r"confirm=([0-9A-Za-z_-]+)", text)
                if m:
                    token = m.group(1)
                    download_url = f"https://drive.google.com/uc?export=download&confirm={token}&id={file_id}"
                    async with sess.get(download_url, allow_redirects=True) as resp2:
                        if resp2.status != 200:
                            return False, f"HTTP {resp2.status}"
                        return await download_stream(resp2, out_path, message, start_time)
                m2 = re.search(r'href="(/uc\?export=download[^"]+)"', text)
                if m2:
                    href = m2.group(1)
                    full = "https://drive.google.com" + href
                    async with sess.get(full, allow_redirects=True) as resp3:
                        if resp3.status != 200:
                            return False, f"HTTP {resp3.status}"
                        return await download_stream(resp3, out_path, message, start_time)
                async with sess.get(base, allow_redirects=True) as resp4:
                    if resp4.status != 200:
                        return False, f"HTTP {resp4.status}"
                    return await download_stream(resp4, out_path, message, start_time)
    except Exception as e:
        return False, str(e)


# ---- Bot commands and handlers ----
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
        print("Set commands error:", e)


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


async def upload_progress(current, total, message: Message, start_time):
    await progress_callback(current, total, message, start_time, task="Uploading")


async def generate_video_thumbnail(video_path: Path, thumb_path: Path):
    try:
        import subprocess
        duration = get_video_duration(video_path)
        timestamp = 1 if duration > 1 else 0
        cmd = [
            "ffmpeg", "-y", "-i", str(video_path),
            "-ss", str(timestamp), "-vframes", "1",
            "-vf", "scale=320:-1", str(thumb_path)
        ]
        subprocess.run(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, check=False)
        return thumb_path.exists() and thumb_path.stat().st_size > 0
    except Exception as e:
        print("generate_video_thumbnail error:", e)
        return False


async def process_file_and_upload(c: Client, m: Message, in_path: Path, original_name: str = None):
    uid = m.from_user.id
    try:
        final_name = original_name or in_path.name
        thumb_path = USER_THUMBS.get(uid)
        if thumb_path and not Path(thumb_path).exists():
            thumb_path = None
        is_video = str(in_path).lower().endswith((".mp4", ".mkv", ".avi", ".mov", ".flv", ".wmv", ".webm"))
        if is_video and not thumb_path:
            thumb_tmp = TMP / f"thumb_{uid}_{int(datetime.now().timestamp())}.jpg"
            ok = await generate_video_thumbnail(in_path, thumb_tmp)
            if ok:
                thumb_path = str(thumb_tmp)

        status_msg = await m.reply_text("আপলোড শুরু হচ্ছে...")
        start_time = datetime.now()
        duration_sec = get_video_duration(in_path) if in_path.exists() else 0

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
        if tmp_in.exists():
            tmp_in.unlink(missing_ok=True)
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
    if not replied.video:
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
        tmp_file = TMP / f"rename_{uid}_{int(datetime.now().timestamp())}_{newname}"
        await replied.download(file_name=str(tmp_file))
        duration_sec = get_video_duration(tmp_file)
        status_msg = await m.reply_text("রিনেম করা ভিডিও Telegram-এ আপলোড হচ্ছে...")
        start_time = datetime.now()
        await c.send_video(
            chat_id=m.chat.id,
            video=str(tmp_file),
            caption=newname,
            thumb=thumb_path,
            duration=duration_sec,
            progress=upload_progress,
            progress_args=(status_msg, start_time)
        )
        await status_msg.edit("রিনেম করা ভিডিও আপলোড সম্পন্ন।")
    except Exception as e:
        await m.reply_text(f"রিনেমে ত্রুটি: {e}")


@app.on_message(filters.command("broadcast") & filters.private & filters.user(ADMIN_ID))
async def broadcast_cmd(c, m: Message):
    if len(m.command) < 2:
        await m.reply_text("ব্যবহার: /broadcast <message>")
        return
    text = m.text.split(None, 1)[1].strip()
    count = 0
    failed = 0
    async for dialog in c.iter_dialogs():
        try:
            await c.send_message(dialog.chat.id, text)
            count += 1
            await asyncio.sleep(0.05)
        except Exception:
            failed += 1
    await m.reply_text(f"Broadcast সম্পন্ন। সফল: {count}, ব্যর্থ: {failed}")


# ---- Webserver (health) ----
async def web_handler(request):
    return web.Response(text="Bot is running (pyrogram + aiohttp)")

async def run_webserver():
    app_web = web.Application()
    app_web.router.add_get("/", web_handler)
    runner = web.AppRunner(app_web)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    print(f"Web server running on port {PORT}")


# ---- Main ----
async def main():
    await app.start()
    print("Bot started")
    await run_webserver()
    try:
        await idle()
    finally:
        await app.stop()

if __name__ == "__main__":
    asyncio.run(main())

