import os
import sqlite3
import threading
import time
import logging
import re
import csv
from typing import Any, Dict, List, Optional, Tuple

import telebot
from telebot.types import (
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaAudio,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
)


BOT_TOKEN = "8647557552:AAEYbCBHPD6gdt4Zy2wlJzQSiTw9oYGdelY"
if not BOT_TOKEN:
    raise RuntimeError("Missing BOT_TOKEN environment variable")

DB_PATH = os.getenv("DB_PATH", "newanon.db")
ADMIN_ID = 8305774350
ADMIN_ID_ENV = os.getenv("ADMIN_ID", "").strip()
LOG_FILE = os.getenv("LOG_FILE", "newanon.log").strip()
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").strip().upper()
FILE_IDS_CSV = os.getenv("FILE_IDS_CSV", "file_ids.csv").strip()

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="HTML")
db_lock = threading.Lock()
album_lock = threading.Lock()

# (chat_id, media_group_id) -> {"messages": [Message, ...], "timer": Timer, "retries": int}
album_buffers: Dict[Tuple[int, str], Dict[str, Any]] = {}
admin_pending_actions: Dict[int, str] = {}
send_lock = threading.Lock()
file_ids_lock = threading.Lock()
last_send_ts = 0.0
MIN_SEND_GAP = float(os.getenv("MIN_SEND_GAP", "0.10"))
SEND_MAX_RETRIES = int(os.getenv("SEND_MAX_RETRIES", "6"))
SCHEDULE_BATCH_SIZE = 100


def setup_logging() -> None:
    level = getattr(logging, LOG_LEVEL, logging.INFO)
    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger()
    logger.setLevel(level)
    logger.handlers.clear()

    file_handler = logging.FileHandler(LOG_FILE, encoding="utf-8")
    file_handler.setLevel(level)
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setLevel(level)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)


def user_tag(user) -> str:
    uname = user.username if user and user.username else "NoUsername"
    uid = user.id if user else "NA"
    return f"{uname}({uid})"


def parse_retry_after_seconds(exc: Exception) -> int:
    text = str(exc)
    match = re.search(r"retry after\s+(\d+)", text, flags=re.IGNORECASE)
    if match:
        return max(1, int(match.group(1)))
    return 1


def safe_telegram_call(fn, op: str, max_retries: Optional[int] = None):
    global last_send_ts
    attempt = 0
    retries = SEND_MAX_RETRIES if max_retries is None else max_retries
    while True:
        attempt += 1
        try:
            with send_lock:
                now = time.time()
                wait = MIN_SEND_GAP - (now - last_send_ts)
                if wait > 0:
                    time.sleep(wait)
                result = fn()
                last_send_ts = time.time()
            return result
        except Exception as e:
            msg = str(e).lower()
            is_flood = (
                "too many requests" in msg
                or "error code: 429" in msg
                or "retry after" in msg
            )
            if is_flood and attempt <= retries:
                retry_after = parse_retry_after_seconds(e)
                delay = retry_after + 0.2
                logging.warning(
                    "rate_limit_wait | op=%s | attempt=%s | sleep=%.1fs | err=%s",
                    op,
                    attempt,
                    delay,
                    e,
                )
                time.sleep(delay)
                continue
            raise


# -----------------------------
# Database helpers
# -----------------------------
def db_connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


conn = db_connect()


def init_db() -> None:
    with db_lock:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_seen INTEGER,
                last_seen INTEGER
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                user_id INTEGER PRIMARY KEY
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS forward_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                from_chat_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                user_id INTEGER,
                media_group_id TEXT,
                created_at INTEGER,
                status TEXT DEFAULT 'pending',
                sent_at INTEGER,
                fail_reason TEXT
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS file_ids (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                file_id TEXT UNIQUE,
                media_type TEXT,
                user_id INTEGER,
                username TEXT,
                message_id INTEGER,
                media_group_id TEXT,
                created_at INTEGER
            )
            """
        )
        
        
        conn.commit()

        defaults = {
            "total_media": "0",
            "firewall_enabled": "0",
            "forward_enabled": "0",
            "firewall_group_id": "",
            "forward_group_id": "",
            "firewall_join_link": "",
            "forward_send_mode": "auto",
            "schedule_break_seconds": "7200",
        }
        for key, value in defaults.items():
            conn.execute(
                "INSERT OR IGNORE INTO settings(key, value) VALUES(?, ?)", (key, value)
            )

        if ADMIN_ID_ENV:
            for raw in ADMIN_ID_ENV.split(","):
                raw = raw.strip()
                if raw and raw.lstrip("-").isdigit():
                    conn.execute(
                        "INSERT OR IGNORE INTO admins(user_id) VALUES(?)", (int(raw),)
                    )
        conn.commit()
    ensure_file_ids_csv_header()


def get_setting(key: str, default: str = "") -> str:
    with db_lock:
        row = conn.execute("SELECT value FROM settings WHERE key = ?", (key,)).fetchone()
    return row["value"] if row else default


def set_setting(key: str, value: str) -> None:
    with db_lock:
        conn.execute(
            "INSERT INTO settings(key, value) VALUES(?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            (key, value),
        )
        conn.commit()


def is_admin(user_id: int) -> bool:
    with db_lock:
        row = conn.execute("SELECT 1 FROM admins WHERE user_id = ?", (user_id,)).fetchone()
    return bool(row)


def upsert_user(user) -> None:
    now = int(time.time())
    with db_lock:
        conn.execute(
            """
            INSERT INTO users(user_id, username, first_seen, last_seen)
            VALUES(?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username = excluded.username,
                last_seen = excluded.last_seen
            """,
            (user.id, user.username or "", now, now),
        )
        conn.commit()


def get_user_count() -> int:
    with db_lock:
        row = conn.execute("SELECT COUNT(*) AS c FROM users").fetchone()
    return int(row["c"] if row else 0)


def get_all_user_ids() -> List[int]:
    with db_lock:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
    return [int(r["user_id"]) for r in rows]

def get_all_users() -> List[Tuple[int, str]]:
    with db_lock:
        rows = conn.execute("SELECT user_id, username FROM users").fetchall()
    return [(int(r["user_id"]), r["username"] or "NoUsername") for r in rows]

def inc_total_media(n: int = 1) -> None:
    current = int(get_setting("total_media", "0") or "0")
    set_setting("total_media", str(current + n))


def get_total_media() -> int:
    return int(get_setting("total_media", "0") or "0")


def ensure_file_ids_csv_header() -> None:
    with file_ids_lock:
        if os.path.exists(FILE_IDS_CSV):
            return
        with open(FILE_IDS_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "file_id",
                    "media_type",
                    "user_id",
                    "username",
                    "message_id",
                    "media_group_id",
                    "created_at",
                ]
            )


def get_file_ids_count() -> int:
    with db_lock:
        row = conn.execute("SELECT COUNT(*) AS c FROM file_ids").fetchone()
    return int(row["c"] if row else 0)

def get_user_media_count(user_id: int) -> int:
    with db_lock:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM file_ids WHERE user_id = ?",
            (user_id,),
        ).fetchone()
    return int(row["c"] if row else 0)

def append_file_id_csv_row(
    file_id: str,
    media_type: str,
    user_id: Optional[int],
    username: str,
    message_id: Optional[int],
    media_group_id: Optional[str],
    created_at: int,
) -> None:
    ensure_file_ids_csv_header()
    with file_ids_lock:
        with open(FILE_IDS_CSV, "a", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    file_id,
                    media_type,
                    user_id if user_id is not None else "",
                    username or "",
                    message_id if message_id is not None else "",
                    media_group_id or "",
                    created_at,
                ]
            )


def rebuild_file_ids_csv_from_db() -> None:
    with db_lock:
        rows = conn.execute(
            """
            SELECT file_id, media_type, user_id, username, message_id, media_group_id, created_at
            FROM file_ids
            ORDER BY id ASC
            """
        ).fetchall()
    with file_ids_lock:
        with open(FILE_IDS_CSV, "w", newline="", encoding="utf-8") as f:
            w = csv.writer(f)
            w.writerow(
                [
                    "file_id",
                    "media_type",
                    "user_id",
                    "username",
                    "message_id",
                    "media_group_id",
                    "created_at",
                ]
            )
            for r in rows:
                w.writerow(
                    [
                        r["file_id"],
                        r["media_type"] or "",
                        r["user_id"] if r["user_id"] is not None else "",
                        r["username"] or "",
                        r["message_id"] if r["message_id"] is not None else "",
                        r["media_group_id"] or "",
                        r["created_at"] if r["created_at"] is not None else "",
                    ]
                )


def record_file_id_from_message(message) -> None:
    media_type, file_id = media_kind_and_file_id(message)
    if not file_id:
        return
    created_at = int(time.time())
    uname = message.from_user.username if message.from_user and message.from_user.username else ""
    uid = message.from_user.id if message.from_user else None
    with db_lock:
        cur = conn.execute(
            """
            INSERT OR IGNORE INTO file_ids(
                file_id, media_type, user_id, username, message_id, media_group_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_id,
                media_type or "",
                uid,
                uname,
                message.message_id,
                message.media_group_id,
                created_at,
            ),
        )
        inserted = cur.rowcount
        conn.commit()
    if inserted:
        append_file_id_csv_row(
            file_id=file_id,
            media_type=media_type or "",
            user_id=uid,
            username=uname,
            message_id=message.message_id,
            media_group_id=message.media_group_id,
            created_at=created_at,
        )


def import_file_ids_csv_bytes(file_bytes: bytes) -> Tuple[int, int]:
    text = file_bytes.decode("utf-8", errors="replace")
    lines = [ln for ln in text.splitlines() if ln.strip()]
    if not lines:
        return 0, 0
    reader = csv.DictReader(lines)
    added = 0
    skipped = 0
    now = int(time.time())
    with db_lock:
        for row in reader:
            file_id = (row.get("file_id") or row.get("FILE_ID") or "").strip()
            if not file_id:
                skipped += 1
                continue
            media_type = (row.get("media_type") or "").strip()
            user_id_raw = (row.get("user_id") or "").strip()
            message_id_raw = (row.get("message_id") or "").strip()
            created_at_raw = (row.get("created_at") or "").strip()
            media_group_id = (row.get("media_group_id") or "").strip()
            username = (row.get("username") or "").strip()
            try:
                user_id_val = int(user_id_raw) if user_id_raw else None
            except Exception:
                user_id_val = None
            try:
                message_id_val = int(message_id_raw) if message_id_raw else None
            except Exception:
                message_id_val = None
            try:
                created_at_val = int(created_at_raw) if created_at_raw else now
            except Exception:
                created_at_val = now
            cur = conn.execute(
                """
                INSERT OR IGNORE INTO file_ids(
                    file_id, media_type, user_id, username, message_id, media_group_id, created_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    file_id,
                    media_type,
                    user_id_val,
                    username,
                    message_id_val,
                    media_group_id,
                    created_at_val,
                ),
            )
            if cur.rowcount:
                added += 1
            else:
                skipped += 1
        conn.commit()
    rebuild_file_ids_csv_from_db()
    return added, skipped


def get_forward_send_mode() -> str:
    mode = (get_setting("forward_send_mode", "auto") or "auto").strip().lower()
    return "scheduled" if mode == "scheduled" else "auto"


def get_schedule_break_seconds() -> int:
    try:
        sec = int(get_setting("schedule_break_seconds", "7200") or "7200")
    except Exception:
        sec = 7200
    return max(60, sec)


def enqueue_forward_message(message) -> None:
    with db_lock:
        conn.execute(
            """
            INSERT INTO forward_queue(from_chat_id, message_id, user_id, media_group_id, created_at, status)
            VALUES(?, ?, ?, ?, ?, 'pending')
            """,
            (
                message.chat.id,
                message.message_id,
                message.from_user.id if message.from_user else None,
                message.media_group_id,
                int(time.time()),
            ),
        )
        conn.commit()
    logging.info(
        "queue_add | user=%s | from_chat=%s | msg_id=%s | media_group_id=%s",
        user_tag(message.from_user),
        message.chat.id,
        message.message_id,
        message.media_group_id,
    )


def enqueue_forward_messages(messages) -> None:
    if not messages:
        return
    now = int(time.time())
    rows = []
    for m in messages:
        rows.append(
            (
                m.chat.id,
                m.message_id,
                m.from_user.id if m.from_user else None,
                m.media_group_id,
                now,
            )
        )
    with db_lock:
        conn.executemany(
            """
            INSERT INTO forward_queue(from_chat_id, message_id, user_id, media_group_id, created_at, status)
            VALUES(?, ?, ?, ?, ?, 'pending')
            """,
            rows,
        )
        conn.commit()
    logging.info(
        "queue_add_batch | user=%s | items=%s | media_group_id=%s",
        user_tag(messages[0].from_user),
        len(messages),
        messages[0].media_group_id,
    )


def get_pending_queue_count() -> int:
    with db_lock:
        row = conn.execute(
            "SELECT COUNT(*) AS c FROM forward_queue WHERE status = 'pending'"
        ).fetchone()
    return int(row["c"] if row else 0)


def fetch_pending_queue(limit: int) -> List[sqlite3.Row]:
    with db_lock:
        rows = conn.execute(
            """
            SELECT id, from_chat_id, message_id, user_id, media_group_id
            FROM forward_queue
            WHERE status = 'pending'
            ORDER BY id ASC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return rows


def mark_queue_sent(row_id: int) -> None:
    with db_lock:
        conn.execute(
            "UPDATE forward_queue SET status='sent', sent_at=? WHERE id=?",
            (int(time.time()), row_id),
        )
        conn.commit()


def mark_queue_failed(row_id: int, reason: str) -> None:
    with db_lock:
        conn.execute(
            "UPDATE forward_queue SET status='failed', fail_reason=? WHERE id=?",
            (reason[:500], row_id),
        )
        conn.commit()


# -----------------------------
# UI helpers
# -----------------------------
def bool_setting(key: str) -> bool:
    return get_setting(key, "0") == "1"


def main_admin_keyboard() -> InlineKeyboardMarkup:
    fw = "ON" if bool_setting("firewall_enabled") else "OFF"
    fwd = "ON" if bool_setting("forward_enabled") else "OFF"

    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📊 Stats", callback_data="admin:stats"),
        InlineKeyboardButton("👥 Users List", callback_data="admin:users_list"),
    )
    kb.add(
        InlineKeyboardButton("📊 Stats", callback_data="admin:stats"),
        InlineKeyboardButton("📢 Broadcast", callback_data="admin:broadcast"),
    )
    kb.add(
        InlineKeyboardButton(f"🔥 Firewall: {fw}", callback_data="admin:toggle_firewall"),
        InlineKeyboardButton(f"📤 Forward: {fwd}", callback_data="admin:toggle_forward"),
    )
    kb.add(
        InlineKeyboardButton("📮 Media Send", callback_data="admin:media_send_menu"),
        InlineKeyboardButton("💾 File IDs", callback_data="admin:file_ids_menu"),
    )
    kb.add(
        InlineKeyboardButton("⚙️ Set Group", callback_data="admin:set_group_menu"),
        InlineKeyboardButton("🧾 Recent Logs", callback_data="admin:recent_logs"),
    )
    kb.add(InlineKeyboardButton("🔄 Refresh", callback_data="admin:refresh"))
    return kb


def media_send_keyboard() -> InlineKeyboardMarkup:
    mode = get_forward_send_mode()
    auto_label = "✅ Automatic" if mode == "auto" else "⚡ Automatic"
    sch_label = "✅ Scheduled" if mode == "scheduled" else "⏱ Scheduled"
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton(auto_label, callback_data="admin:send_mode_auto"),
        InlineKeyboardButton(sch_label, callback_data="admin:send_mode_scheduled"),
    )
    kb.add(InlineKeyboardButton("🕒 Set Break Time", callback_data="admin:set_break_time"))
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data="admin:back_main"))
    return kb


def file_ids_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=2)
    kb.add(
        InlineKeyboardButton("📤 Export CSV", callback_data="admin:file_ids_export"),
        InlineKeyboardButton("📥 Import CSV", callback_data="admin:file_ids_import"),
    )
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data="admin:back_main"))
    return kb

def group_menu_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    kb.add(InlineKeyboardButton("🛡 Set Firewall Group", callback_data="admin:set_fw_group"))
    kb.add(InlineKeyboardButton("📥 Set Forward Group", callback_data="admin:set_fwd_group"))
    kb.add(InlineKeyboardButton("⬅️ Back", callback_data="admin:back_main"))
    return kb


def firewall_user_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardMarkup(row_width=1)
    join_link = get_or_create_join_link()
    if join_link:
        kb.add(InlineKeyboardButton("➕ Join Required Group", url=join_link))
    kb.add(InlineKeyboardButton("✅ I've Joined", callback_data="user:check_join"))
    return kb


def panel_text() -> str:
    fw_enabled = "ON" if bool_setting("firewall_enabled") else "OFF"
    fwd_enabled = "ON" if bool_setting("forward_enabled") else "OFF"
    send_mode = get_forward_send_mode().upper()
    break_hours = round(get_schedule_break_seconds() / 3600, 2)
    q_pending = get_pending_queue_count()
    file_ids_count = get_file_ids_count()
    fw_gid = get_setting("firewall_group_id", "") or "Not set"
    fwd_gid = get_setting("forward_group_id", "") or "Not set"

    return (
        "<b>👑 Admin Panel</b>\n\n"
        f"<b>📊 Users:</b> {get_user_count()}\n"
        f"<b>🎞 Total Media:</b> {get_total_media()}\n"
        f"<b>🔥 Firewall:</b> {fw_enabled}\n"
        f"<b>📤 Forward Mode:</b> {fwd_enabled}\n"
        f"<b>📮 Send Type:</b> {send_mode}\n"
        f"<b>⏱ Break Hours:</b> {break_hours}\n"
        f"<b>📦 Queue Pending:</b> {q_pending}\n"
        f"<b>💾 File IDs:</b> {file_ids_count}\n"
        f"<b>🛡 Firewall Group:</b> <code>{fw_gid}</code>\n"
        f"<b>📥 Forward Group:</b> <code>{fwd_gid}</code>"
    )


def send_admin_panel(chat_id: int) -> None:
    bot.send_message(chat_id, panel_text(), reply_markup=main_admin_keyboard())


# -----------------------------
# Firewall helpers
# -----------------------------
def get_or_create_join_link() -> str:
    cached = get_setting("firewall_join_link", "")
    if cached:
        return cached

    raw_gid = get_setting("firewall_group_id", "")
    if not raw_gid:
        return ""

    try:
        gid = int(raw_gid)
    except ValueError:
        return ""

    try:
        inv = bot.create_chat_invite_link(gid, name="Firewall Access", creates_join_request=True)
        link = inv.invite_link
        set_setting("firewall_join_link", link)
        return link
    except Exception:
        return ""


def user_is_group_member(user_id: int, group_id: int) -> bool:
    try:
        member = bot.get_chat_member(group_id, user_id)
        return member.status in ("creator", "administrator", "member", "restricted")
    except Exception:
        return False


def firewall_allows(message, user_id: Optional[int] = None) -> bool:
    if not bool_setting("firewall_enabled"):
        return True

    raw_gid = get_setting("firewall_group_id", "")
    if not raw_gid:
        bot.reply_to(
            message,
            "🚫 Firewall is ON but no firewall group is set by admin yet.",
        )
        return False

    group_id = int(raw_gid)
    uid = user_id if user_id is not None else message.from_user.id
    if user_is_group_member(uid, group_id):
        return True

    bot.reply_to(
        message,
        "🔥 Firewall is ON. Join the required group first, then tap \"I've Joined\".",
        reply_markup=firewall_user_keyboard(),
    )
    return False


# -----------------------------
# Message copying helpers
# -----------------------------
def media_kind_and_file_id(message) -> Tuple[Optional[str], Optional[str]]:
    if message.photo:
        return "photo", message.photo[-1].file_id
    if message.video:
        return "video", message.video.file_id
    if message.document:
        return "document", message.document.file_id
    if message.audio:
        return "audio", message.audio.file_id
    if message.voice:
        return "voice", message.voice.file_id
    if message.sticker:
        return "sticker", message.sticker.file_id
    if message.animation:
        return "animation", message.animation.file_id
    return None, None


def is_media_message(message) -> bool:
    kind, _ = media_kind_and_file_id(message)
    return kind is not None


def send_clean_single(chat_id: int, message) -> None:
    logging.info(
        "single_send_start | user=%s | chat_id=%s | msg_id=%s | type=%s | media_group_id=%s",
        user_tag(message.from_user),
        chat_id,
        message.message_id,
        message.content_type,
        message.media_group_id,
    )
    if message.content_type == "text":
        try:
            safe_telegram_call(
                lambda: bot.send_message(chat_id, message.text),
                op=f"user_text:{chat_id}:{message.message_id}",
            )
            logging.info(
                "single_send_ok | user=%s | chat_id=%s | msg_id=%s | type=text",
                user_tag(message.from_user),
                chat_id,
                message.message_id,
            )
        except Exception as e:
            logging.exception(
                "single_send_fail | user=%s | chat_id=%s | msg_id=%s | type=text | err=%s",
                user_tag(message.from_user),
                chat_id,
                message.message_id,
                e,
            )
        return

    if message.content_type in (
        "photo",
        "video",
        "document",
        "audio",
        "voice",
        "sticker",
        "animation",
    ):
        try:
            safe_telegram_call(
                lambda: bot.copy_message(chat_id, chat_id, message.message_id),
                op=f"user_copy:{chat_id}:{message.message_id}",
            )
            inc_total_media(1)
            logging.info(
                "single_send_ok | user=%s | chat_id=%s | msg_id=%s | type=%s",
                user_tag(message.from_user),
                chat_id,
                message.message_id,
                message.content_type,
            )
        except Exception as e:
            logging.exception(
                "single_send_fail | user=%s | chat_id=%s | msg_id=%s | type=%s | err=%s",
                user_tag(message.from_user),
                chat_id,
                message.message_id,
                message.content_type,
                e,
            )
        return

    # if message.caption:
    #     bot.send_message(chat_id, message.caption)


def build_input_media(msg):
    if msg.photo:
        return InputMediaPhoto(msg.photo[-1].file_id)
    if msg.video:
        return InputMediaVideo(msg.video.file_id)
    if msg.document:
        return InputMediaDocument(msg.document.file_id)
    if msg.audio:
        return InputMediaAudio(msg.audio.file_id)
    return None


def forward_to_group_header(user) -> str:
    uname = f"@{user.username}" if user.username else "NoUsername"
    return f"👤 User: {uname} | <code>{user.id}</code>"


def maybe_forward_single_to_group(message) -> None:
    if not bool_setting("forward_enabled"):
        return

    if get_forward_send_mode() == "scheduled":
        enqueue_forward_message(message)
        return

    raw_gid = get_setting("forward_group_id", "")
    if not raw_gid:
        return

    gid = int(raw_gid)
    try:
        logging.info(
            "forward_group_single_start | user=%s | from_chat=%s | to_group=%s | msg_id=%s | type=%s",
            user_tag(message.from_user),
            message.chat.id,
            gid,
            message.message_id,
            message.content_type,
        )
        safe_telegram_call(
            lambda: bot.forward_message(gid, message.chat.id, message.message_id),
            op=f"fwd_as_is:{gid}:{message.message_id}",
        )
        logging.info(
            "forward_group_single_ok | user=%s | to_group=%s | msg_id=%s",
            user_tag(message.from_user),
            gid,
            message.message_id,
        )
    except Exception:
        logging.exception(
            "forward_group_single_fail | user=%s | to_group=%s | msg_id=%s",
            user_tag(message.from_user),
            gid,
            message.message_id,
        )


def maybe_forward_album_to_group(messages, gid: int, media_group_id: str) -> None:
    if not messages:
        return
    if get_forward_send_mode() == "scheduled":
        enqueue_forward_messages(messages)
        return

    src_chat_id = messages[0].chat.id
    message_ids = [m.message_id for m in messages]
    try:
        logging.info(
            "forward_group_album_start | user=%s | from_chat=%s | to_group=%s | media_group_id=%s | items=%s",
            user_tag(messages[0].from_user),
            src_chat_id,
            gid,
            media_group_id,
            len(message_ids),
        )
        safe_telegram_call(
            lambda: bot.forward_messages(gid, src_chat_id, message_ids),
            op=f"fwd_album_batch:{gid}:{media_group_id}:{len(message_ids)}",
            max_retries=20,
        )
        logging.info(
            "forward_group_album_ok | user=%s | to_group=%s | media_group_id=%s | items=%s",
            user_tag(messages[0].from_user),
            gid,
            media_group_id,
            len(message_ids),
        )
    except Exception:
        logging.exception(
            "forward_group_album_fail | user=%s | to_group=%s | media_group_id=%s | fallback=single",
            user_tag(messages[0].from_user),
            gid,
            media_group_id,
        )
        for m in messages:
            maybe_forward_single_to_group(m)


def delete_message_by_ids(chat_id: int, message_id: int, context: str = "") -> None:
    try:
        safe_telegram_call(
            lambda: bot.delete_message(chat_id, message_id),
            op=f"delete_user_msg:{chat_id}:{message_id}:{context}",
        )
        logging.info(
            "delete_user_msg_ok | chat_id=%s | msg_id=%s | context=%s",
            chat_id,
            message_id,
            context,
        )
    except Exception:
        logging.exception(
            "delete_user_msg_fail | chat_id=%s | msg_id=%s | context=%s",
            chat_id,
            message_id,
            context,
        )


def delete_user_message(message, context: str = "") -> None:
    delete_message_by_ids(message.chat.id, message.message_id, context=context)


def scheduled_forward_worker() -> None:
    sent_in_cycle = 0
    pause_until = 0.0

    while True:
        try:
            if not bool_setting("forward_enabled"):
                sent_in_cycle = 0
                pause_until = 0.0
                time.sleep(1.5)
                continue

            if get_forward_send_mode() != "scheduled":
                sent_in_cycle = 0
                pause_until = 0.0
                time.sleep(1.5)
                continue

            raw_gid = get_setting("forward_group_id", "")
            if not raw_gid:
                time.sleep(1.5)
                continue
            gid = int(raw_gid)

            now = time.time()
            if pause_until > now:
                time.sleep(min(2.0, pause_until - now))
                continue

            if sent_in_cycle >= SCHEDULE_BATCH_SIZE:
                break_seconds = get_schedule_break_seconds()
                pause_until = time.time() + break_seconds
                logging.info(
                    "schedule_pause_start | batch=%s | break_seconds=%s | pending=%s",
                    SCHEDULE_BATCH_SIZE,
                    break_seconds,
                    get_pending_queue_count(),
                )
                sent_in_cycle = 0
                continue

            rows = fetch_pending_queue(SCHEDULE_BATCH_SIZE - sent_in_cycle)
            if not rows:
                time.sleep(1.0)
                continue

            for row in rows:
                try:
                    safe_telegram_call(
                        lambda r=row: bot.forward_message(gid, r["from_chat_id"], r["message_id"]),
                        op=f"scheduled_fwd:{row['id']}",
                        max_retries=20,
                    )
                    mark_queue_sent(int(row["id"]))
                    delete_message_by_ids(
                        int(row["from_chat_id"]),
                        int(row["message_id"]),
                        context=f"scheduled:{row['id']}",
                    )
                    sent_in_cycle += 1
                    logging.info(
                        "schedule_send_ok | row_id=%s | to_group=%s | from_chat=%s | msg_id=%s | sent_in_cycle=%s",
                        row["id"],
                        gid,
                        row["from_chat_id"],
                        row["message_id"],
                        sent_in_cycle,
                    )
                    if sent_in_cycle >= SCHEDULE_BATCH_SIZE:
                        break
                except Exception as e:
                    mark_queue_failed(int(row["id"]), str(e))
                    logging.exception(
                        "schedule_send_fail | row_id=%s | to_group=%s | from_chat=%s | msg_id=%s | err=%s",
                        row["id"],
                        gid,
                        row["from_chat_id"],
                        row["message_id"],
                        e,
                    )
        except Exception:
            logging.exception("schedule_worker_loop_fail")
            time.sleep(2.0)


def process_album(chat_id: int, media_group_id: str) -> None:
    with album_lock:
        payload = album_buffers.pop((chat_id, media_group_id), None)

    if not payload:
        return

    retries = int(payload.get("retries", 0))
    uniq = {m.message_id: m for m in payload["messages"]}
    messages = sorted(uniq.values(), key=lambda m: m.message_id)
    if not messages:
        return
    logging.info(
        "album_flush_start | user=%s | chat_id=%s | media_group_id=%s | items=%s | retries=%s",
        user_tag(messages[0].from_user),
        chat_id,
        media_group_id,
        len(messages),
        retries,
    )

    # Late album parts sometimes arrive after initial timeout. Retry a few times.
    if len(messages) == 1 and retries < 5:
        retry_delay = 2.0
        logging.info(
            "album_flush_retry | user=%s | chat_id=%s | media_group_id=%s | retry=%s | delay=%.1fs",
            user_tag(messages[0].from_user),
            chat_id,
            media_group_id,
            retries + 1,
            retry_delay,
        )
        with album_lock:
            timer = threading.Timer(retry_delay, process_album, args=(chat_id, media_group_id))
            album_buffers[(chat_id, media_group_id)] = {
                "messages": messages,
                "timer": timer,
                "retries": retries + 1,
            }
            timer.start()
        return

    media_items = []

    for m in messages:
        media = build_input_media(m)
        if media:
            media_items.append(media)

    # If media group cannot be rebuilt (unlikely), fall back to single-message copies.
    if not media_items:
        for m in messages:
            send_clean_single(chat_id, m)
            maybe_forward_single_to_group(m)
        return

    sent_count = 0
    for i in range(0, len(media_items), 10):
        chunk = media_items[i : i + 10]
        try:
            if len(chunk) == 1:
                logging.info(
                    "album_chunk_single_fallback | user=%s | chat_id=%s | media_group_id=%s | idx=%s",
                    user_tag(messages[0].from_user),
                    chat_id,
                    media_group_id,
                    i,
                )
                send_clean_single(chat_id, messages[i])
                sent_count += 1
            else:
                safe_telegram_call(
                    lambda: bot.send_media_group(chat_id, chunk),
                    op=f"user_album:{chat_id}:{media_group_id}:{i}:{len(chunk)}",
                    max_retries=20,
                )
                sent_count += len(chunk)
                logging.info(
                    "album_chunk_send_ok | user=%s | chat_id=%s | media_group_id=%s | chunk_start=%s | chunk_size=%s",
                    user_tag(messages[0].from_user),
                    chat_id,
                    media_group_id,
                    i,
                    len(chunk),
                )
        except Exception:
            logging.exception(
                "album_chunk_send_fail | user=%s | chat_id=%s | media_group_id=%s | chunk_start=%s | chunk_size=%s",
                user_tag(messages[0].from_user),
                chat_id,
                media_group_id,
                i,
                len(chunk),
            )
            for j in range(i, i + len(chunk)):
                send_clean_single(chat_id, messages[j])
                sent_count += 1
    if sent_count:
        inc_total_media(sent_count)
    logging.info(
        "album_flush_done | user=%s | chat_id=%s | media_group_id=%s | delivered=%s",
        user_tag(messages[0].from_user),
        chat_id,
        media_group_id,
        sent_count,
    )

    if bool_setting("forward_enabled"):
        raw_gid = get_setting("forward_group_id", "")
        if raw_gid:
            gid = int(raw_gid)
            maybe_forward_album_to_group(messages, gid, media_group_id)

    # In scheduled mode we must keep source messages until worker forwards them.
    if not (bool_setting("forward_enabled") and get_forward_send_mode() == "scheduled"):
        for m in messages:
            delete_user_message(m, context=f"album:{media_group_id}")


def queue_album_message(message) -> None:
    key = (message.chat.id, message.media_group_id)
    flush_delay = 2.0

    with album_lock:
        if key not in album_buffers:
            timer = threading.Timer(
                flush_delay, process_album, args=(message.chat.id, message.media_group_id)
            )
            album_buffers[key] = {"messages": [message], "timer": timer, "retries": 0}
            timer.start()
            logging.info(
                "album_queue_new | user=%s | chat_id=%s | media_group_id=%s | msg_id=%s | delay=%.1fs",
                user_tag(message.from_user),
                message.chat.id,
                message.media_group_id,
                message.message_id,
                flush_delay,
            )
        else:
            album_buffers[key]["messages"].append(message)
            album_buffers[key]["retries"] = 0
            # Debounce flush so late parts of the same album are still grouped.
            old_timer = album_buffers[key].get("timer")
            if old_timer:
                try:
                    old_timer.cancel()
                except Exception:
                    pass
            new_timer = threading.Timer(
                flush_delay, process_album, args=(message.chat.id, message.media_group_id)
            )
            album_buffers[key]["timer"] = new_timer
            new_timer.start()
            logging.info(
                "album_queue_append | user=%s | chat_id=%s | media_group_id=%s | msg_id=%s | total_buffered=%s",
                user_tag(message.from_user),
                message.chat.id,
                message.media_group_id,
                message.message_id,
                len(album_buffers[key]["messages"]),
            )


# -----------------------------
# Admin actions
# -----------------------------
def show_stats(chat_id: int) -> None:
    bot.send_message(
        chat_id,
        f"📊 <b>Stats</b>\n\n"
        f"👥 Users: <b>{get_user_count()}</b>\n"
        f"🎞 Total Media Sent: <b>{get_total_media()}</b>",
    )


def read_recent_logs(max_lines: int = 30) -> str:
    try:
        with open(LOG_FILE, "r", encoding="utf-8", errors="replace") as f:
            lines = f.readlines()
        if not lines:
            return "No logs yet."
        return "".join(lines[-max_lines:]).rstrip() or "No logs yet."
    except FileNotFoundError:
        return f"Log file not found: {LOG_FILE}"
    except Exception as e:
        return f"Failed to read logs: {e}"


def toggle_setting_with_feedback(chat_id: int, key: str, label: str) -> None:
    new_val = "0" if bool_setting(key) else "1"
    set_setting(key, new_val)
    human = "ON" if new_val == "1" else "OFF"
    bot.send_message(chat_id, f"{label} is now <b>{human}</b>.")


@bot.callback_query_handler(func=lambda call: True)
def on_callback(call) -> None:
    user_id = call.from_user.id

    # User-side callback: firewall re-check
    if call.data == "user:check_join":
        if firewall_allows(call.message, user_id=call.from_user.id):
            bot.answer_callback_query(call.id, "Access granted ✅")
            bot.send_message(call.message.chat.id, "✅ You're verified. Send media/text now.")
        else:
            bot.answer_callback_query(call.id, "Still not joined")
        return

    if not is_admin(user_id):
        bot.answer_callback_query(call.id, "Not allowed", show_alert=False)
        return

    if not call.data.startswith("admin:"):
        return

    action = call.data.split(":", 1)[1]
    if action == "users_list":
        bot.answer_callback_query(call.id, "Loading users...")

        users = get_all_users()

        if not users:
            bot.send_message(call.message.chat.id, "No users found.")
            return

        text = "<b>👥 Users List</b>\n\n"

        #for uid, uname in users:
            #text += f"👤 {uname} | <code>{uid}</code>\n"
        for uid, uname, fname, lname in users:
            full_name = f"{fname} {lname}".strip() or "No Name"
            media_count = get_user_media_count(uid)

            text += (
                f"👤 {full_name} (@{uname}) | <code>{uid}</code>\n"
                f"📦 Media Sent: {media_count}\n\n"
            )

        # Telegram message limit fix
        for i in range(0, len(text), 4000):
            bot.send_message(call.message.chat.id, text[i:i+4000])

        return

    if action == "stats":
        bot.answer_callback_query(call.id, "Stats updated")
        try:
            bot.edit_message_text(
                panel_text(),
                call.message.chat.id,
                call.message.message_id,
                reply_markup=main_admin_keyboard(),
            )
        except Exception:
            send_admin_panel(call.message.chat.id)
        return

    if action == "broadcast":
        admin_pending_actions[user_id] = "broadcast"
        bot.answer_callback_query(call.id, "Broadcast mode")
        bot.send_message(
            call.message.chat.id,
            "📢 Send one message now (text/media). It will be broadcast to all users."
            "\nSend /cancel to stop.",
        )
        return

    if action == "toggle_firewall":
        toggle_setting_with_feedback(call.message.chat.id, "firewall_enabled", "🔥 Firewall")
        bot.answer_callback_query(call.id, "Toggled")
        send_admin_panel(call.message.chat.id)
        return

    if action == "toggle_forward":
        toggle_setting_with_feedback(call.message.chat.id, "forward_enabled", "📤 Forward mode")
        bot.answer_callback_query(call.id, "Toggled")
        send_admin_panel(call.message.chat.id)
        return

    if action == "media_send_menu":
        bot.answer_callback_query(call.id)
        mode = get_forward_send_mode().upper()
        break_hours = round(get_schedule_break_seconds() / 3600, 2)
        bot.send_message(
            call.message.chat.id,
            (
                "<b>📮 Media Send Settings</b>\n\n"
                f"<b>Current Mode:</b> {mode}\n"
                f"<b>Batch:</b> {SCHEDULE_BATCH_SIZE}\n"
                f"<b>Break Hours:</b> {break_hours}\n"
                f"<b>Queue Pending:</b> {get_pending_queue_count()}\n\n"
                "Automatic: forwards immediately (current behavior).\n"
                "Scheduled: sends 100 messages, then pauses for break time."
            ),
            reply_markup=media_send_keyboard(),
        )
        return

    if action == "file_ids_menu":
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            (
                "<b>💾 File IDs Storage</b>\n\n"
                f"<b>Total Saved:</b> {get_file_ids_count()}\n"
                f"<b>CSV File:</b> <code>{FILE_IDS_CSV}</code>\n\n"
                "Export: download all saved file_ids.\n"
                "Import: upload a CSV to merge file_ids."
            ),
            reply_markup=file_ids_keyboard(),
        )
        return

    if action == "file_ids_export":
        bot.answer_callback_query(call.id, "Preparing CSV")
        rebuild_file_ids_csv_from_db()
        try:
            with open(FILE_IDS_CSV, "rb") as f:
                safe_telegram_call(
                    lambda: (
                        f.seek(0),
                        bot.send_document(
                            call.message.chat.id,
                            f,
                            caption=f"💾 File IDs Export\nTotal: {get_file_ids_count()}",
                        ),
                    )[1],
                    op=f"file_ids_export:{call.message.chat.id}",
                )
        except Exception:
            logging.exception("file_ids_export_fail")
            bot.send_message(call.message.chat.id, "❌ Failed to export CSV.")
        return

    if action == "file_ids_import":
        admin_pending_actions[user_id] = "file_ids_import"
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "📥 Send CSV file now to import file_ids.\nUse /cancel to stop.",
        )
        return

    if action == "send_mode_auto":
        set_setting("forward_send_mode", "auto")
        bot.answer_callback_query(call.id, "Automatic mode enabled")
        send_admin_panel(call.message.chat.id)
        return

    if action == "send_mode_scheduled":
        set_setting("forward_send_mode", "scheduled")
        bot.answer_callback_query(call.id, "Scheduled mode enabled")
        send_admin_panel(call.message.chat.id)
        return

    if action == "set_break_time":
        admin_pending_actions[user_id] = "set_break_time"
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "🕒 Send break time in hours (example: <code>2</code> or <code>1.5</code>).",
        )
        return

    if action == "recent_logs":
        bot.answer_callback_query(call.id, "Loading logs")
        logs = read_recent_logs(30)
        # Use plain text so log characters are shown exactly as-is.
        bot.send_message(call.message.chat.id, f"🧾 Recent Logs\n\n{logs}", parse_mode=None)
        return

    if action == "set_group_menu":
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "⚙️ Choose which group you want to set:",
            reply_markup=group_menu_keyboard(),
        )
        return

    if action == "set_fw_group":
        admin_pending_actions[user_id] = "set_fw_group"
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "🛡 Send firewall group ID (example: <code>-1001234567890</code>)"
            "\nOr forward any message from that group here.",
        )
        return

    if action == "set_fwd_group":
        admin_pending_actions[user_id] = "set_fwd_group"
        bot.answer_callback_query(call.id)
        bot.send_message(
            call.message.chat.id,
            "📥 Send forward group ID (example: <code>-1001234567890</code>)"
            "\nOr forward any message from that group here.",
        )
        return

    if action in ("back_main", "refresh"):
        bot.answer_callback_query(call.id)
        send_admin_panel(call.message.chat.id)
        return


def parse_group_id_from_message(message) -> Optional[int]:
    if message.text and message.text.strip().lstrip("-").isdigit():
        return int(message.text.strip())

    fchat = getattr(message, "forward_from_chat", None)
    if fchat:
        return int(fchat.id)

    if getattr(message, "sender_chat", None):
        return int(message.sender_chat.id)

    return None


def process_admin_pending(message) -> bool:
    uid = message.from_user.id
    if uid not in admin_pending_actions:
        return False

    action = admin_pending_actions[uid]

    if message.text and message.text.strip().lower() == "/cancel":
        admin_pending_actions.pop(uid, None)
        bot.reply_to(message, "❌ Pending admin action cancelled.")
        return True

    if action == "set_fw_group":
        gid = parse_group_id_from_message(message)
        if gid is None:
            bot.reply_to(message, "Invalid input. Send a group ID or forward a group message.")
            return True

        set_setting("firewall_group_id", str(gid))
        set_setting("firewall_join_link", "")
        admin_pending_actions.pop(uid, None)
        bot.reply_to(message, f"✅ Firewall group set to <code>{gid}</code>")
        send_admin_panel(message.chat.id)
        return True

    if action == "set_fwd_group":
        gid = parse_group_id_from_message(message)
        if gid is None:
            bot.reply_to(message, "Invalid input. Send a group ID or forward a group message.")
            return True

        set_setting("forward_group_id", str(gid))
        admin_pending_actions.pop(uid, None)
        bot.reply_to(message, f"✅ Forward group set to <code>{gid}</code>")
        send_admin_panel(message.chat.id)
        return True

    if action == "broadcast":
        admin_pending_actions.pop(uid, None)
        user_ids = get_all_user_ids()
        ok = 0
        fail = 0

        for user_id in user_ids:
            try:
                bot.copy_message(user_id, message.chat.id, message.message_id)
                ok += 1
            except Exception:
                fail += 1

        bot.reply_to(
            message,
            f"📢 Broadcast finished.\n✅ Sent: <b>{ok}</b>\n❌ Failed: <b>{fail}</b>",
        )
        return True

    if action == "file_ids_import":
        if not message.document:
            bot.reply_to(message, "Please send a CSV document file.")
            return True
        file_name = (message.document.file_name or "").lower()
        if not file_name.endswith(".csv"):
            bot.reply_to(message, "Invalid file type. Please send a .csv file.")
            return True
        try:
            tg_file = bot.get_file(message.document.file_id)
            file_bytes = bot.download_file(tg_file.file_path)
            added, skipped = import_file_ids_csv_bytes(file_bytes)
            admin_pending_actions.pop(uid, None)
            bot.reply_to(
                message,
                f"✅ CSV imported.\nAdded: <b>{added}</b>\nSkipped: <b>{skipped}</b>\nTotal: <b>{get_file_ids_count()}</b>",
            )
            send_admin_panel(message.chat.id)
        except Exception as e:
            logging.exception("file_ids_import_fail | err=%s", e)
            bot.reply_to(message, "❌ Failed to import CSV. Please check format and try again.")
        return True

    if action == "set_break_time":
        raw = (message.text or "").strip()
        try:
            hours = float(raw)
        except Exception:
            bot.reply_to(message, "Invalid value. Send a number like 2 or 1.5")
            return True
        if hours <= 0:
            bot.reply_to(message, "Break hours must be greater than 0.")
            return True
        seconds = int(hours * 3600)
        set_setting("schedule_break_seconds", str(seconds))
        admin_pending_actions.pop(uid, None)
        bot.reply_to(
            message,
            f"✅ Scheduled break set to <b>{hours}</b> hour(s).",
        )
        send_admin_panel(message.chat.id)
        return True

    return False


# -----------------------------
# Commands + message routing
# -----------------------------
@bot.message_handler(commands=["start", "admin"])
def on_start(message) -> None:
    if message.chat.type != "private":
        return

    upsert_user(message.from_user)

    if is_admin(message.from_user.id):
        send_admin_panel(message.chat.id)
        return

    bot.send_message(
        message.chat.id,
        "👋 Welcome to Anonymous Forward Bot.Send me any message\nphoto\nvideo\ndocument\nvoice or sticker \nI will forward it back to you anonymously.",
    )


@bot.message_handler(commands=["cancel"])
def on_cancel(message) -> None:
    if not is_admin(message.from_user.id):
        return

    if admin_pending_actions.pop(message.from_user.id, None):
        bot.reply_to(message, "❌ Pending action cancelled.")
    else:
        bot.reply_to(message, "No pending action.")


@bot.message_handler(
    content_types=[
        "text",
        "photo",
        "video",
        "document",
        "audio",
        "voice",
        "sticker",
        "animation",
    ]
)
def on_private_message(message) -> None:
    if message.chat.type != "private":
        return

    upsert_user(message.from_user)
    logging.info(
        "incoming_private | user=%s | chat_id=%s | msg_id=%s | type=%s | media_group_id=%s",
        user_tag(message.from_user),
        message.chat.id,
        message.message_id,
        message.content_type,
        message.media_group_id,
    )

    if is_admin(message.from_user.id) and process_admin_pending(message):
        return

    if is_admin(message.from_user.id):
        # Admin normal messages are ignored unless they are in a pending flow.
        return

    record_file_id_from_message(message)

    if not firewall_allows(message):
        delete_user_message(message, context="blocked_firewall")
        return

    # User message flow: clean sendback + optional forward mode.
    if message.media_group_id and message.content_type in ("photo", "video", "document", "audio"):
        queue_album_message(message)
        return

    send_clean_single(message.chat.id, message)
    maybe_forward_single_to_group(message)
    if not (bool_setting("forward_enabled") and get_forward_send_mode() == "scheduled"):
        delete_user_message(message, context="single")


# def main() -> None:
#     setup_logging()
#     init_db()
#     threading.Thread(target=scheduled_forward_worker, daemon=True).start()
#     logging.info(
#         "bot_start | db_path=%s | log_file=%s | log_level=%s",
#         DB_PATH,
#         LOG_FILE,
#         LOG_LEVEL,
#     )
#     bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)


def main():
    setup_logging()
    init_db()

    bot.remove_webhook()

    threading.Thread(target=scheduled_forward_worker, daemon=True).start()

    logging.info("Bot started...")
    bot.infinity_polling(skip_pending=True, timeout=30, long_polling_timeout=30)


if __name__ == "__main__":
    main()
