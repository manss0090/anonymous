import logging
import os
import sqlite3
import time
import asyncio
from datetime import datetime, timezone

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.error import Conflict
from telegram.ext import (
    Application,
    CallbackQueryHandler,
    CommandHandler,
    ContextTypes,
    MessageHandler,
    filters,
)


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)
logger = logging.getLogger(__name__)
logging.getLogger("httpx").setLevel(logging.WARNING)


DB_PATH = "bot_data.db"
ADMIN_MENU_PREFIX = "admin:"
USER_PAGE_SIZE = 10

WELCOME_TEXT = (
    "👋 Welcome to Anonymous Forward Bot.\n\n"
    "Send me any message, photo, video, document, voice, or sticker and "
    "I will forward it back to you anonymously."
)
ADMIN_TEXT = "🛠️ Admin Panel"
MEDIA_TYPES = {
    "photo",
    "video",
    "document",
    "voice",
    "audio",
    "sticker",
    "animation",
}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def init_db(db_path: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                first_name TEXT,
                last_name TEXT,
                joined_at TEXT NOT NULL,
                last_seen_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS media_messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                message_id INTEGER NOT NULL,
                media_type TEXT NOT NULL,
                created_at TEXT NOT NULL,
                FOREIGN KEY (user_id) REFERENCES users(user_id)
            )
            """
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_media_user ON media_messages(user_id)"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_users_last_seen ON users(last_seen_at)"
        )
        conn.commit()


def upsert_user(db_path: str, tg_user) -> None:
    now = utc_now_iso()
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO users(user_id, username, first_name, last_name, joined_at, last_seen_at)
            VALUES(?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id) DO UPDATE SET
                username=excluded.username,
                first_name=excluded.first_name,
                last_name=excluded.last_name,
                last_seen_at=excluded.last_seen_at
            """,
            (
                tg_user.id,
                tg_user.username,
                tg_user.first_name,
                tg_user.last_name,
                now,
                now,
            ),
        )
        conn.commit()


def store_media_message(db_path: str, user_id: int, message_id: int, media_type: str) -> None:
    with sqlite3.connect(db_path) as conn:
        conn.execute(
            """
            INSERT INTO media_messages(user_id, message_id, media_type, created_at)
            VALUES(?, ?, ?, ?)
            """,
            (user_id, message_id, media_type, utc_now_iso()),
        )
        conn.commit()


def get_total_users(db_path: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM users").fetchone()
        return int(row[0] if row else 0)


def get_total_media(db_path: str) -> int:
    with sqlite3.connect(db_path) as conn:
        row = conn.execute("SELECT COUNT(*) FROM media_messages").fetchone()
        return int(row[0] if row else 0)


def get_users_page(db_path: str, page: int, page_size: int) -> list[tuple]:
    offset = page * page_size
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            """
            SELECT user_id, username, first_name, last_name
            FROM users
            ORDER BY last_seen_at DESC
            LIMIT ? OFFSET ?
            """,
            (page_size, offset),
        ).fetchall()


def get_user_count(db_path: str) -> int:
    return get_total_users(db_path)


def get_all_user_ids(db_path: str) -> list[int]:
    with sqlite3.connect(db_path) as conn:
        rows = conn.execute("SELECT user_id FROM users").fetchall()
        return [int(r[0]) for r in rows]


def get_user_media(db_path: str, user_id: int, limit: int = 10) -> list[tuple]:
    with sqlite3.connect(db_path) as conn:
        return conn.execute(
            """
            SELECT message_id, media_type, created_at
            FROM media_messages
            WHERE user_id = ?
            ORDER BY id DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()


def display_name(username: str | None, first_name: str | None, last_name: str | None) -> str:
    if username:
        return f"@{username}"
    full_name = " ".join(x for x in [first_name, last_name] if x).strip()
    return full_name or "Unknown User"


def admin_menu_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📢 Broadcast", callback_data=f"{ADMIN_MENU_PREFIX}broadcast"),
                InlineKeyboardButton("👥 Total Users", callback_data=f"{ADMIN_MENU_PREFIX}total_users"),
            ],
            [
                InlineKeyboardButton("🖼️ Total Media", callback_data=f"{ADMIN_MENU_PREFIX}total_media"),
                InlineKeyboardButton("📋 Users", callback_data=f"{ADMIN_MENU_PREFIX}users:0"),
            ],
        ]
    )


def users_keyboard(db_path: str, page: int) -> InlineKeyboardMarkup:
    users = get_users_page(db_path, page=page, page_size=USER_PAGE_SIZE)
    total_users = get_user_count(db_path)
    rows: list[list[InlineKeyboardButton]] = []

    for user_id, username, first_name, last_name in users:
        name = display_name(username, first_name, last_name)
        rows.append(
            [
                InlineKeyboardButton(
                    f"{name} ({user_id})",
                    callback_data=f"{ADMIN_MENU_PREFIX}user:{user_id}",
                )
            ]
        )

    nav_row: list[InlineKeyboardButton] = []
    if page > 0:
        nav_row.append(
            InlineKeyboardButton("⬅️ Prev", callback_data=f"{ADMIN_MENU_PREFIX}users:{page - 1}")
        )
    if (page + 1) * USER_PAGE_SIZE < total_users:
        nav_row.append(
            InlineKeyboardButton("Next ➡️", callback_data=f"{ADMIN_MENU_PREFIX}users:{page + 1}")
        )
    if nav_row:
        rows.append(nav_row)
    rows.append([InlineKeyboardButton("⬅️ Back", callback_data=f"{ADMIN_MENU_PREFIX}back")])
    return InlineKeyboardMarkup(rows)


def get_media_type(message) -> str | None:
    if message.photo:
        return "photo"
    for media_type in MEDIA_TYPES - {"photo"}:
        if getattr(message, media_type, None):
            return media_type
    return None


def is_admin(update: Update, admin_user_id: int) -> bool:
    return bool(update.effective_user and update.effective_user.id == admin_user_id)


async def flush_media_group(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, media_group_id: str
) -> None:
    key = (chat_id, media_group_id)
    pending_groups = context.application.bot_data.setdefault("pending_media_groups", {})
    group_data = pending_groups.pop(key, None)
    if not group_data:
        return

    message_ids = sorted(group_data["message_ids"])
    try:
        # copy_messages keeps album behavior and anonymity.
        await context.bot.copy_messages(
            chat_id=chat_id,
            from_chat_id=chat_id,
            message_ids=message_ids,
        )
        return
    except Exception as exc:
        logger.warning(
            "Album copy failed for chat_id=%s media_group_id=%s: %s",
            chat_id,
            media_group_id,
            exc,
        )

    # Fallback: copy one by one if grouped copy fails.
    for message_id in message_ids:
        try:
            await context.bot.copy_message(
                chat_id=chat_id,
                from_chat_id=chat_id,
                message_id=message_id,
            )
        except Exception as exc:
            logger.warning(
                "Fallback copy failed for chat_id=%s message_id=%s: %s",
                chat_id,
                message_id,
                exc,
            )


async def schedule_media_group_flush(
    context: ContextTypes.DEFAULT_TYPE, chat_id: int, media_group_id: str
) -> None:
    await asyncio.sleep(1.0)
    await flush_media_group(context, chat_id, media_group_id)


async def on_error(update: object, context: ContextTypes.DEFAULT_TYPE) -> None:
    if isinstance(context.error, Conflict):
        logger.warning(
            "Telegram getUpdates conflict detected. "
            "Another instance is polling with the same bot token."
        )
        return
    logger.exception("Unhandled error while processing update: %s", context.error)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.message:
        upsert_user(context.bot_data["db_path"], update.effective_user)
        if is_admin(update, context.bot_data["admin_user_id"]):
            await update.message.reply_text(
                "👋 Welcome Admin.\nClick the button below to open admin controls.",
                reply_markup=InlineKeyboardMarkup(
                    [
                        [
                            InlineKeyboardButton(
                                "🛠️ Admin Panel",
                                callback_data=f"{ADMIN_MENU_PREFIX}open_panel",
                            )
                        ]
                    ]
                ),
            )
            return
        await update.message.reply_text(WELCOME_TEXT)


async def admin(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    admin_user_id = context.bot_data["admin_user_id"]
    if not is_admin(update, admin_user_id):
        if update.message:
            await update.message.reply_text("❌ You are not allowed to access admin panel.")
        return
    if update.message:
        await update.message.reply_text(ADMIN_TEXT, reply_markup=admin_menu_keyboard())


async def admin_callbacks(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    query = update.callback_query
    if not query:
        return
    admin_user_id = context.bot_data["admin_user_id"]
    if not is_admin(update, admin_user_id):
        await query.answer("Not allowed.", show_alert=True)
        return

    await query.answer()
    data = query.data or ""
    db_path = context.bot_data["db_path"]

    if data in {
        f"{ADMIN_MENU_PREFIX}back",
        f"{ADMIN_MENU_PREFIX}open_panel",
    }:
        await query.edit_message_text(ADMIN_TEXT, reply_markup=admin_menu_keyboard())
        return

    if data == f"{ADMIN_MENU_PREFIX}broadcast":
        context.user_data["awaiting_broadcast"] = True
        await query.edit_message_text(
            "📢 Send the message you want to broadcast to all users.\n"
            "You can send text or media with caption.",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data=f"{ADMIN_MENU_PREFIX}back")]]
            ),
        )
        return

    if data == f"{ADMIN_MENU_PREFIX}total_users":
        total = get_total_users(db_path)
        await query.edit_message_text(
            f"👥 Total users: {total}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data=f"{ADMIN_MENU_PREFIX}back")]]
            ),
        )
        return

    if data == f"{ADMIN_MENU_PREFIX}total_media":
        total = get_total_media(db_path)
        await query.edit_message_text(
            f"🖼️ Total media sent: {total}",
            reply_markup=InlineKeyboardMarkup(
                [[InlineKeyboardButton("⬅️ Back", callback_data=f"{ADMIN_MENU_PREFIX}back")]]
            ),
        )
        return

    if data.startswith(f"{ADMIN_MENU_PREFIX}users:"):
        page = int(data.split(":")[-1])
        await query.edit_message_text(
            "📋 Users list:",
            reply_markup=users_keyboard(db_path, page),
        )
        return

    if data.startswith(f"{ADMIN_MENU_PREFIX}user:"):
        user_id = int(data.split(":")[-1])
        media_records = get_user_media(db_path, user_id=user_id, limit=10)
        if not media_records:
            await query.message.reply_text(f"ℹ️ User {user_id} has no media records.")
        else:
            await query.message.reply_text(
                f"🗂️ Last {len(media_records)} media messages from user {user_id}:"
            )
            for message_id, media_type, created_at in media_records:
                try:
                    await context.bot.copy_message(
                        chat_id=admin_user_id,
                        from_chat_id=user_id,
                        message_id=message_id,
                    )
                except Exception as exc:
                    logger.warning(
                        "Failed to copy media message_id=%s from user_id=%s: %s",
                        message_id,
                        user_id,
                        exc,
                    )
                    await query.message.reply_text(
                        f"⚠️ Failed: {media_type} ({message_id}) at {created_at}"
                    )
        await query.message.reply_text("🛠️ Admin Panel", reply_markup=admin_menu_keyboard())
        return


async def handle_broadcast_input(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    admin_user_id = context.bot_data["admin_user_id"]
    if not is_admin(update, admin_user_id):
        return
    if not context.user_data.get("awaiting_broadcast"):
        return

    context.user_data["awaiting_broadcast"] = False
    db_path = context.bot_data["db_path"]
    users = get_all_user_ids(db_path)
    success = 0
    failed = 0

    for user_id in users:
        try:
            await context.bot.copy_message(
                chat_id=user_id,
                from_chat_id=admin_user_id,
                message_id=update.message.message_id,
            )
            success += 1
            await asyncio.sleep(0.05)
        except Exception as exc:
            logger.warning("Broadcast failed to user_id=%s: %s", user_id, exc)
            failed += 1

    await update.message.reply_text(
        f"✅ Broadcast finished.\nDelivered: {success}\nFailed: {failed}",
        reply_markup=admin_menu_keyboard(),
    )


async def anonymous_forward(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if not update.message:
        return
    chat_id = update.effective_chat.id if update.effective_chat else None
    if chat_id is None:
        return

    upsert_user(context.bot_data["db_path"], update.effective_user)
    media_type = get_media_type(update.message)
    if media_type:
        store_media_message(
            context.bot_data["db_path"],
            user_id=update.effective_user.id,
            message_id=update.message.message_id,
            media_type=media_type,
        )

    media_group_id = update.message.media_group_id
    if media_group_id:
        pending_groups = context.application.bot_data.setdefault("pending_media_groups", {})
        key = (chat_id, str(media_group_id))
        if key not in pending_groups:
            task = asyncio.create_task(
                schedule_media_group_flush(context, chat_id, str(media_group_id))
            )
            pending_groups[key] = {"message_ids": [], "task": task}
        pending_groups[key]["message_ids"].append(update.message.message_id)
        return

    # copy_message keeps it anonymous. If Telegram rejects copy for a media type,
    # fall back to sending the same media by file_id.
    try:
        await update.message.copy(chat_id=chat_id)
        return
    except Exception as exc:
        logger.warning("Anonymous copy failed for message_id=%s: %s", update.message.message_id, exc)

    msg = update.message
    if msg.text:
        await context.bot.send_message(chat_id=chat_id, text=msg.text)
    elif msg.photo:
        await context.bot.send_photo(
            chat_id=chat_id,
            photo=msg.photo[-1].file_id,
            caption=msg.caption,
            caption_entities=msg.caption_entities,
        )
    elif msg.video:
        await context.bot.send_video(
            chat_id=chat_id,
            video=msg.video.file_id,
            caption=msg.caption,
            caption_entities=msg.caption_entities,
        )
    elif msg.document:
        await context.bot.send_document(
            chat_id=chat_id,
            document=msg.document.file_id,
            caption=msg.caption,
            caption_entities=msg.caption_entities,
        )
    elif msg.voice:
        await context.bot.send_voice(chat_id=chat_id, voice=msg.voice.file_id, caption=msg.caption)
    elif msg.audio:
        await context.bot.send_audio(
            chat_id=chat_id,
            audio=msg.audio.file_id,
            caption=msg.caption,
            caption_entities=msg.caption_entities,
        )
    elif msg.sticker:
        await context.bot.send_sticker(chat_id=chat_id, sticker=msg.sticker.file_id)
    elif msg.animation:
        await context.bot.send_animation(
            chat_id=chat_id,
            animation=msg.animation.file_id,
            caption=msg.caption,
            caption_entities=msg.caption_entities,
        )
    elif msg.video_note:
        await context.bot.send_video_note(chat_id=chat_id, video_note=msg.video_note.file_id)
    else:
        await context.bot.send_message(
            chat_id=chat_id,
            text="⚠️ I received your message, but this media type is not supported for anonymous echo yet.",
        )


async def main_message_handler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if (
        context.user_data.get("awaiting_broadcast")
        and is_admin(update, context.bot_data["admin_user_id"])
    ):
        await handle_broadcast_input(update, context)
        return
    await anonymous_forward(update, context)


def main() -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN")
    admin_user_id_raw = os.getenv("ADMIN_USER_ID")
    if not token:
        raise RuntimeError(
            "Missing TELEGRAM_BOT_TOKEN environment variable. "
            "Set it before running the bot."
        )
    if not admin_user_id_raw:
        raise RuntimeError(
            "Missing ADMIN_USER_ID environment variable. "
            "Set it to your Telegram numeric user ID."
        )
    admin_user_id = int(admin_user_id_raw)
    init_db(DB_PATH)

    application = Application.builder().token(token).build()
    application.bot_data["db_path"] = DB_PATH
    application.bot_data["admin_user_id"] = admin_user_id
    application.add_error_handler(on_error)

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("admin", admin))
    application.add_handler(
        CallbackQueryHandler(admin_callbacks, pattern=f"^{ADMIN_MENU_PREFIX}")
    )
    application.add_handler(MessageHandler(filters.ALL & ~filters.COMMAND, main_message_handler))

    startup_delay = int(os.getenv("STARTUP_DELAY_SECONDS", "0"))

    logger.info("Bot is running...")
    if startup_delay > 0:
        logger.info("Startup delay enabled: waiting %s seconds before connecting to Telegram.", startup_delay)
        time.sleep(startup_delay)
    application.run_polling(
        allowed_updates=Update.ALL_TYPES,
        drop_pending_updates=True,
    )


if __name__ == "__main__":
    main()
