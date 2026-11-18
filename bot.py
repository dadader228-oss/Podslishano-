#!/usr/bin/env python3
"""
Подслушано — полноценный Telegram-бот анонимных публикаций.
Python 3.11+ | aiogram 3.x | SQLite (aiosqlite)
Один файл. Готов к продакшену после заполнения BOT_TOKEN и CHANNEL_ID.
"""

from __future__ import annotations

import asyncio
import logging
import sqlite3
from contextlib import asynccontextmanager
from datetime import datetime, timedelta
from typing import Any

import aiosqlite
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest, TelegramForbiddenError, TelegramRetryAfter
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardMarkup,
    InputMediaAnimation,
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
    Message,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder

# ============================================================================
# 1. КОНФИГУРАЦИЯ
# ============================================================================

BOT_TOKEN = "YOUR_BOT_TOKEN_HERE"          # <-- вставь токен
CHANNEL_ID = -1003939319096 # <-- ID канала (отрицательный) или "@username"
ADMIN_ID = 5049231780
SECRET_CODE = "Qwerty12345"

DB_PATH = "podslushano.db"

# --- Антиспам / лимиты (все настройки в одном месте) ---
ANTISPAM = {
    "post_cooldown_seconds": 60,           # минимум между публикациями
    "comment_cooldown_seconds": 30,
    "report_cooldown_seconds": 120,
    "support_cooldown_seconds": 60,
    "max_posts_per_hour": 5,
    "max_comments_per_hour": 15,
    "max_reports_per_day": 10,
    "max_identical_posts": 2,              # одинаковый текст
    "max_file_size_mb": 50,
    "admin_session_timeout_minutes": 30,
    "broadcast_delay_seconds": 0.05,       # ~20 msg/sec safe
    "max_comment_length": 1000,
    "max_post_text_length": 4000,
}

# ============================================================================
# 2. ЛОГИРОВАНИЕ
# ============================================================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("podslushano")

# ============================================================================
# 3. FSM STATES
# ============================================================================

class UserStates(StatesGroup):
    waiting_post = State()
    waiting_comment = State()
    waiting_support = State()
    waiting_report_reason = State()


class AdminStates(StatesGroup):
    waiting_secret = State()
    waiting_broadcast = State()
    waiting_broadcast_confirm = State()
    waiting_ban_reason = State()
    waiting_support_reply = State()
    waiting_user_search = State()


# ============================================================================
# 4. IN-MEMORY STATE (сессии, rate-limit)
# ============================================================================

admin_sessions: dict[int, datetime] = {}          # user_id -> last_activity
rate_limits: dict[str, list[datetime]] = {}       # key -> timestamps
media_group_buffer: dict[str, list[Message]] = {} # media_group_id -> messages

# ============================================================================
# 5. DATABASE
# ============================================================================

async def init_db() -> None:
    async with aiosqlite.connect(DB_PATH) as db:
        await db.executescript("""
        PRAGMA journal_mode=WAL;
        PRAGMA foreign_keys=ON;

        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            username TEXT,
            first_name TEXT,
            registration_date TEXT NOT NULL,
            is_banned INTEGER DEFAULT 0,
            ban_reason TEXT,
            last_activity TEXT
        );

        CREATE TABLE IF NOT EXISTS posts (
            post_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            content_type TEXT NOT NULL,
            text TEXT,
            media_file_id TEXT,
            status TEXT NOT NULL DEFAULT 'pending',  -- pending / published / rejected
            created_at TEXT NOT NULL,
            published_at TEXT,
            channel_message_id INTEGER,
            comments_enabled INTEGER DEFAULT 1,
            media_group_id TEXT,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS post_media (
            media_id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            file_id TEXT NOT NULL,
            media_type TEXT NOT NULL,
            caption TEXT,
            position INTEGER DEFAULT 0,
            FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS comments (
            comment_id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            text TEXT NOT NULL,
            created_at TEXT NOT NULL,
            is_deleted INTEGER DEFAULT 0,
            FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS reports (
            report_id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            reason TEXT NOT NULL,
            status TEXT DEFAULT 'open',  -- open / resolved / rejected
            created_at TEXT NOT NULL,
            FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS support_tickets (
            ticket_id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            status TEXT DEFAULT 'open',  -- open / closed
            created_at TEXT NOT NULL,
            updated_at TEXT NOT NULL,
            FOREIGN KEY (user_id) REFERENCES users(user_id)
        );

        CREATE TABLE IF NOT EXISTS support_messages (
            message_id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticket_id INTEGER NOT NULL,
            sender_id INTEGER NOT NULL,
            is_admin INTEGER DEFAULT 0,
            content_type TEXT DEFAULT 'text',
            text TEXT,
            media_file_id TEXT,
            created_at TEXT NOT NULL,
            FOREIGN KEY (ticket_id) REFERENCES support_tickets(ticket_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS likes (
            like_id INTEGER PRIMARY KEY AUTOINCREMENT,
            post_id INTEGER NOT NULL,
            user_id INTEGER NOT NULL,
            created_at TEXT NOT NULL,
            UNIQUE(post_id, user_id),
            FOREIGN KEY (post_id) REFERENCES posts(post_id) ON DELETE CASCADE
        );

        CREATE TABLE IF NOT EXISTS logs (
            log_id INTEGER PRIMARY KEY AUTOINCREMENT,
            action TEXT NOT NULL,
            user_id INTEGER,
            details TEXT,
            created_at TEXT NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
        CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id);
        CREATE INDEX IF NOT EXISTS idx_comments_post ON comments(post_id);
        CREATE INDEX IF NOT EXISTS idx_reports_status ON reports(status);
        CREATE INDEX IF NOT EXISTS idx_support_status ON support_tickets(status);
        CREATE INDEX IF NOT EXISTS idx_logs_created ON logs(created_at);
        """)
        await db.commit()
    logger.info("Database initialized: %s", DB_PATH)


async def db_execute(query: str, params: tuple = (), fetch: str | None = None) -> Any:
    async with aiosqlite.connect(DB_PATH) as db:
        db.row_factory = aiosqlite.Row
        cursor = await db.execute(query, params)
        if fetch == "one":
            row = await cursor.fetchone()
            await db.commit()
            return dict(row) if row else None
        if fetch == "all":
            rows = await cursor.fetchall()
            await db.commit()
            return [dict(r) for r in rows]
        await db.commit()
        return cursor.lastrowid


async def log_action(action: str, user_id: int | None = None, details: str = "") -> None:
    # Никогда не пишем секретный код / токен
    safe = details.replace(SECRET_CODE, "***").replace(BOT_TOKEN, "***")
    await db_execute(
        "INSERT INTO logs (action, user_id, details, created_at) VALUES (?, ?, ?, ?)",
        (action, user_id, safe, datetime.utcnow().isoformat()),
    )


# ============================================================================
# 6. HELPERS
# ============================================================================

def now_iso() -> str:
    return datetime.utcnow().isoformat()


def is_admin(user_id: int) -> bool:
    return user_id == ADMIN_ID


def admin_session_active(user_id: int) -> bool:
    if not is_admin(user_id):
        return False
    last = admin_sessions.get(user_id)
    if not last:
        return False
    timeout = timedelta(minutes=ANTISPAM["admin_session_timeout_minutes"])
    if datetime.utcnow() - last > timeout:
        admin_sessions.pop(user_id, None)
        return False
    admin_sessions[user_id] = datetime.utcnow()  # refresh
    return True


def touch_admin_session(user_id: int) -> None:
    if is_admin(user_id):
        admin_sessions[user_id] = datetime.utcnow()


async def ensure_user(message_or_user) -> dict:
    """Создаёт/обновляет пользователя и возвращает запись."""
    if hasattr(message_or_user, "from_user"):
        u = message_or_user.from_user
    else:
        u = message_or_user

    user = await db_execute(
        "SELECT * FROM users WHERE user_id = ?", (u.id,), fetch="one"
    )
    if not user:
        await db_execute(
            "INSERT INTO users (user_id, username, first_name, registration_date, last_activity) "
            "VALUES (?, ?, ?, ?, ?)",
            (u.id, u.username, u.first_name, now_iso(), now_iso()),
        )
        await log_action("new_user", u.id, f"username=@{u.username}")
        user = await db_execute(
            "SELECT * FROM users WHERE user_id = ?", (u.id,), fetch="one"
        )
    else:
        await db_execute(
            "UPDATE users SET username=?, first_name=?, last_activity=? WHERE user_id=?",
            (u.username, u.first_name, now_iso(), u.id),
        )
    return user


async def is_banned(user_id: int) -> bool:
    row = await db_execute(
        "SELECT is_banned FROM users WHERE user_id = ?", (user_id,), fetch="one"
    )
    return bool(row and row["is_banned"])


def check_rate_limit(key: str, limit: int, window_seconds: int) -> bool:
    """True = можно, False = лимит превышен."""
    now = datetime.utcnow()
    window = timedelta(seconds=window_seconds)
    if key not in rate_limits:
        rate_limits[key] = []
    rate_limits[key] = [t for t in rate_limits[key] if now - t < window]
    if len(rate_limits[key]) >= limit:
        return False
    rate_limits[key].append(now)
    return True


def main_menu_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="📝 Отправить сообщение", callback_data="send_post")
    builder.button(text="🛠 Поддержка", callback_data="support")
    builder.adjust(1)
    return builder.as_markup()


def cancel_kb(callback: str = "cancel") -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="❌ Отмена", callback_data=callback)
    return builder.as_markup()


def admin_main_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    buttons = [
        ("📥 Последние сообщения", "admin:posts:0"),
        ("⏳ На модерации", "admin:moderation:0"),
        ("👤 Управление пользователями", "admin:users"),
        ("🚫 Заблокированные", "admin:banned:0"),
        ("🚩 Жалобы", "admin:reports:0"),
        ("💬 Поддержка", "admin:support:open:0"),
        ("📋 Логи", "admin:logs:0"),
        ("📢 Рассылка", "admin:broadcast"),
        ("📊 Статистика", "admin:stats"),
        ("🚪 Выход", "admin:logout"),
    ]
    for text, data in buttons:
        builder.button(text=text, callback_data=data)
    builder.adjust(1)
    return builder.as_markup()


def moderation_kb(post_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text="✅ Опубликовать", callback_data=f"mod:approve:{post_id}")
    builder.button(text="❌ Отклонить", callback_data=f"mod:reject:{post_id}")
    builder.button(text="🚫 Заблокировать", callback_data=f"mod:ban:{post_id}")
    builder.button(text="ℹ️ Об авторе", callback_data=f"mod:author:{post_id}")
    builder.adjust(2)
    return builder.as_markup()


def channel_post_kb(post_id: int, likes: int = 0) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.button(text=f"❤️ {likes}" if likes else "❤️ Нравится", callback_data=f"like:{post_id}")
    builder.button(text="💬 Комментарии", callback_data=f"comments:{post_id}:0")
    builder.button(text="🚩 Пожаловаться", callback_data=f"report:{post_id}")
    builder.adjust(3)
    return builder.as_markup()


# ============================================================================
# 7. BOT & ROUTERS
# ============================================================================

bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
dp = Dispatcher(storage=MemoryStorage())
router = Router()
dp.include_router(router)


# ============================================================================
# 8. USER HANDLERS
# ============================================================================

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    user = await ensure_user(message)
    if user["is_banned"]:
        await message.answer("⛔ Вы заблокированы и не можете пользоваться ботом.")
        return

    text = (
        "Добро пожаловать в <b>Подслушано</b>!\n\n"
        "Здесь ты можешь анонимно поделиться своей историей, мнением, "
        "признанием или задать вопрос.\n\n"
        "Твои публикации увидят другие пользователи, но твоё имя останется скрытым."
    )
    await message.answer(text, reply_markup=main_menu_kb())


@router.callback_query(F.data == "send_post")
async def cb_send_post(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if await is_banned(callback.from_user.id):
        await callback.message.answer("⛔ Вы заблокированы.")
        return

    # rate limit
    key = f"post:{callback.from_user.id}"
    if not check_rate_limit(key, ANTISPAM["max_posts_per_hour"], 3600):
        await callback.message.answer(
            f"⏳ Слишком много публикаций. Попробуй через час (лимит {ANTISPAM['max_posts_per_hour']}/час)."
        )
        return

    await state.set_state(UserStates.waiting_post)
    await callback.message.answer(
        "Отправь материал, который хочешь опубликовать анонимно.\n\n"
        "Поддерживаются: текст, фото, видео, GIF, голосовые, кружочки, "
        "документы, аудио и альбомы.\n\n"
        "Можешь добавить подпись.",
        reply_markup=cancel_kb("cancel_post"),
    )


@router.callback_query(F.data == "cancel_post")
async def cb_cancel_post(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Отменено")
    await callback.message.answer("Главное меню:", reply_markup=main_menu_kb())


@router.message(StateFilter(UserStates.waiting_post))
async def process_post(message: Message, state: FSMContext):
    user_id = message.from_user.id
    if await is_banned(user_id):
        await state.clear()
        await message.answer("⛔ Вы заблокированы.")
        return

    # cooldown
    key_cd = f"post_cd:{user_id}"
    if not check_rate_limit(key_cd, 1, ANTISPAM["post_cooldown_seconds"]):
        await message.answer(
            f"⏳ Подожди {ANTISPAM['post_cooldown_seconds']} сек. перед следующей публикацией."
        )
        return

    content_type = message.content_type
    text = message.text or message.caption or ""
    media_file_id = None
    media_group_id = message.media_group_id

    # file size check (rough)
    file_id = None
    if message.photo:
        file_id = message.photo[-1].file_id
        content_type = "photo"
    elif message.video:
        file_id = message.video.file_id
        content_type = "video"
    elif message.animation:
        file_id = message.animation.file_id
        content_type = "animation"
    elif message.voice:
        file_id = message.voice.file_id
        content_type = "voice"
    elif message.video_note:
        file_id = message.video_note.file_id
        content_type = "video_note"
    elif message.document:
        file_id = message.document.file_id
        content_type = "document"
    elif message.audio:
        file_id = message.audio.file_id
        content_type = "audio"
    elif message.text:
        content_type = "text"
    else:
        await message.answer("Этот тип контента пока не поддерживается.")
        return

    if text and len(text) > ANTISPAM["max_post_text_length"]:
        await message.answer(f"Текст слишком длинный (макс. {ANTISPAM['max_post_text_length']} символов).")
        return

    # identical text protection
    if text:
        recent = await db_execute(
            "SELECT COUNT(*) as cnt FROM posts WHERE user_id=? AND text=? AND created_at > ?",
            (user_id, text, (datetime.utcnow() - timedelta(hours=24)).isoformat()),
            fetch="one",
        )
        if recent and recent["cnt"] >= ANTISPAM["max_identical_posts"]:
            await message.answer("Ты уже отправлял очень похожий материал. Подожди.")
            return

    # media group handling
    if media_group_id:
        if media_group_id not in media_group_buffer:
            media_group_buffer[media_group_id] = []
        media_group_buffer[media_group_id].append(message)
        # ждём немного, чтобы собрать всю группу
        await asyncio.sleep(1.2)
        if media_group_id not in media_group_buffer:
            return  # уже обработали
        messages = media_group_buffer.pop(media_group_id)
        await _save_media_group(messages, state)
        return

    # single message
    post_id = await db_execute(
        "INSERT INTO posts (user_id, content_type, text, media_file_id, status, created_at) "
        "VALUES (?, ?, ?, ?, 'pending', ?)",
        (user_id, content_type, text, file_id, now_iso()),
    )
    if file_id and content_type != "text":
        await db_execute(
            "INSERT INTO post_media (post_id, file_id, media_type, caption, position) VALUES (?, ?, ?, ?, 0)",
            (post_id, file_id, content_type, text),
        )

    await log_action("post_submitted", user_id, f"post_id={post_id} type={content_type}")
    await state.clear()
    await message.answer(
        "Твоё сообщение отправлено на модерацию. После проверки оно может появиться в Подслушано.",
        reply_markup=main_menu_kb(),
    )
    await notify_admin_new_post(post_id)


async def _save_media_group(messages: list[Message], state: FSMContext):
    if not messages:
        return
    user_id = messages[0].from_user.id
    media_group_id = messages[0].media_group_id
    caption = None
    for m in messages:
        if m.caption:
            caption = m.caption
            break

    post_id = await db_execute(
        "INSERT INTO posts (user_id, content_type, text, status, created_at, media_group_id) "
        "VALUES (?, 'media_group', ?, 'pending', ?, ?)",
        (user_id, caption, now_iso(), media_group_id),
    )
    for i, m in enumerate(sorted(messages, key=lambda x: x.message_id)):
        fid = None
        mtype = "photo"
        if m.photo:
            fid = m.photo[-1].file_id
            mtype = "photo"
        elif m.video:
            fid = m.video.file_id
            mtype = "video"
        elif m.animation:
            fid = m.animation.file_id
            mtype = "animation"
        elif m.document:
            fid = m.document.file_id
            mtype = "document"
        if fid:
            await db_execute(
                "INSERT INTO post_media (post_id, file_id, media_type, caption, position) "
                "VALUES (?, ?, ?, ?, ?)",
                (post_id, fid, mtype, m.caption, i),
            )

    await log_action("post_submitted", user_id, f"post_id={post_id} media_group")
    await state.clear()
    await messages[0].answer(
        "Твоё сообщение (альбом) отправлено на модерацию. После проверки оно может появиться в Подслушано.",
        reply_markup=main_menu_kb(),
    )
    await notify_admin_new_post(post_id)


async def notify_admin_new_post(post_id: int):
    post = await db_execute("SELECT * FROM posts WHERE post_id=?", (post_id,), fetch="one")
    if not post:
        return
    user = await db_execute("SELECT * FROM users WHERE user_id=?", (post["user_id"],), fetch="one")
    media = await db_execute(
        "SELECT * FROM post_media WHERE post_id=? ORDER BY position", (post_id,), fetch="all"
    )

    author_info = (
        f"Автор: <code>{user['user_id']}</code>\n"
        f"Username: @{user['username'] or '—'}\n"
        f"Имя: {user['first_name'] or '—'}\n"
        f"Дата: {post['created_at'][:19]}\n"
        f"Тип: {post['content_type']}\n"
        f"ID поста: {post_id}"
    )

    try:
        if post["content_type"] == "text":
            await bot.send_message(
                ADMIN_ID,
                f"🆕 Новая публикация на модерации\n\n{author_info}\n\n"
                f"<b>Текст:</b>\n{post['text'] or '—'}",
                reply_markup=moderation_kb(post_id),
            )
        elif post["content_type"] == "media_group" and media:
            # отправляем как media group админу
            media_list = []
            for i, m in enumerate(media):
                caption = (author_info + f"\n\n{m['caption'] or ''}") if i == 0 else None
                if m["media_type"] == "photo":
                    media_list.append(InputMediaPhoto(media=m["file_id"], caption=caption))
                elif m["media_type"] == "video":
                    media_list.append(InputMediaVideo(media=m["file_id"], caption=caption))
                elif m["media_type"] == "animation":
                    media_list.append(InputMediaAnimation(media=m["file_id"], caption=caption))
                else:
                    media_list.append(InputMediaDocument(media=m["file_id"], caption=caption))
            await bot.send_media_group(ADMIN_ID, media_list)
            await bot.send_message(
                ADMIN_ID,
                f"🆕 Альбом на модерации (post_id={post_id})",
                reply_markup=moderation_kb(post_id),
            )
        else:
            # single media
            kwargs = {"caption": f"{author_info}\n\n{post['text'] or ''}", "reply_markup": moderation_kb(post_id)}
            if post["content_type"] == "photo":
                await bot.send_photo(ADMIN_ID, post["media_file_id"], **kwargs)
            elif post["content_type"] == "video":
                await bot.send_video(ADMIN_ID, post["media_file_id"], **kwargs)
            elif post["content_type"] == "animation":
                await bot.send_animation(ADMIN_ID, post["media_file_id"], **kwargs)
            elif post["content_type"] == "voice":
                await bot.send_voice(ADMIN_ID, post["media_file_id"], **kwargs)
            elif post["content_type"] == "video_note":
                await bot.send_video_note(ADMIN_ID, post["media_file_id"])
                await bot.send_message(ADMIN_ID, author_info, reply_markup=moderation_kb(post_id))
            elif post["content_type"] == "document":
                await bot.send_document(ADMIN_ID, post["media_file_id"], **kwargs)
            elif post["content_type"] == "audio":
                await bot.send_audio(ADMIN_ID, post["media_file_id"], **kwargs)
            else:
                await bot.send_message(ADMIN_ID, f"{author_info}\n\n{post['text']}", reply_markup=moderation_kb(post_id))
    except Exception as e:
        logger.exception("Failed to notify admin about post %s: %s", post_id, e)
        await log_action("error", None, f"notify_admin_post {post_id}: {e}")


# ============================================================================
# 9. MODERATION ACTIONS
# ============================================================================

@router.callback_query(F.data.startswith("mod:"))
async def cb_moderation(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id) or not admin_session_active(callback.from_user.id):
        await callback.answer("⛔ Нет доступа", show_alert=True)
        return
    touch_admin_session(callback.from_user.id)

    parts = callback.data.split(":")
    action = parts[1]
    post_id = int(parts[2])

    post = await db_execute("SELECT * FROM posts WHERE post_id=?", (post_id,), fetch="one")
    if not post:
        await callback.answer("Пост не найден", show_alert=True)
        return

    if action == "approve":
        if post["status"] != "pending":
            await callback.answer("Уже обработан", show_alert=True)
            return
        try:
            channel_msg_id = await publish_to_channel(post_id)
            await db_execute(
                "UPDATE posts SET status='published', published_at=?, channel_message_id=? WHERE post_id=?",
                (now_iso(), channel_msg_id, post_id),
            )
            await log_action("post_approved", callback.from_user.id, f"post_id={post_id}")
            await callback.answer("✅ Опубликовано")
            await callback.message.edit_reply_markup(reply_markup=None)
            try:
                await bot.send_message(
                    post["user_id"],
                    "✅ Твоя публикация прошла модерацию и уже в канале Подслушано!",
                )
            except Exception:
                pass
        except Exception as e:
            logger.exception("Publish failed: %s", e)
            await callback.answer(f"Ошибка публикации: {e}", show_alert=True)

    elif action == "reject":
        if post["status"] != "pending":
            await callback.answer("Уже обработан", show_alert=True)
            return
        await db_execute(
            "UPDATE posts SET status='rejected' WHERE post_id=?", (post_id,)
        )
        await log_action("post_rejected", callback.from_user.id, f"post_id={post_id}")
        await callback.answer("Отклонено")
        await callback.message.edit_reply_markup(reply_markup=None)
        try:
            await bot.send_message(
                post["user_id"],
                "❌ К сожалению, твоя публикация не прошла модерацию.",
            )
        except Exception:
            pass

    elif action == "ban":
        await state.set_state(AdminStates.waiting_ban_reason)
        await state.update_data(ban_user_id=post["user_id"], ban_post_id=post_id)
        await callback.message.answer(
            f"Укажи причину блокировки пользователя <code>{post['user_id']}</code>:",
            reply_markup=cancel_kb("admin:cancel"),
        )
        await callback.answer()

    elif action == "author":
        user = await db_execute(
            "SELECT * FROM users WHERE user_id=?", (post["user_id"],), fetch="one"
        )
        posts_count = await db_execute(
            "SELECT COUNT(*) as cnt FROM posts WHERE user_id=?", (post["user_id"],), fetch="one"
        )
        text = (
            f"<b>Информация об авторе</b>\n\n"
            f"ID: <code>{user['user_id']}</code>\n"
            f"Username: @{user['username'] or '—'}\n"
            f"Имя: {user['first_name'] or '—'}\n"
            f"Регистрация: {user['registration_date'][:19]}\n"
            f"Заблокирован: {'да' if user['is_banned'] else 'нет'}\n"
            f"Публикаций: {posts_count['cnt'] if posts_count else 0}"
        )
        await callback.message.answer(text)
        await callback.answer()


async def publish_to_channel(post_id: int) -> int:
    """Публикует пост в канал и возвращает message_id."""
    post = await db_execute("SELECT * FROM posts WHERE post_id=?", (post_id,), fetch="one")
    media = await db_execute(
        "SELECT * FROM post_media WHERE post_id=? ORDER BY position", (post_id,), fetch="all"
    )
    likes = 0
    kb = channel_post_kb(post_id, likes)

    if post["content_type"] == "text":
        msg = await bot.send_message(CHANNEL_ID, post["text"] or "—", reply_markup=kb)
        return msg.message_id

    if post["content_type"] == "media_group" and media:
        media_list = []
        for i, m in enumerate(media):
            cap = m["caption"] if i == 0 else None
            if m["media_type"] == "photo":
                media_list.append(InputMediaPhoto(media=m["file_id"], caption=cap))
            elif m["media_type"] == "video":
                media_list.append(InputMediaVideo(media=m["file_id"], caption=cap))
            elif m["media_type"] == "animation":
                media_list.append(InputMediaAnimation(media=m["file_id"], caption=cap))
            else:
                media_list.append(InputMediaDocument(media=m["file_id"], caption=cap))
        msgs = await bot.send_media_group(CHANNEL_ID, media_list)
        # кнопки отдельным сообщением после альбома
        await bot.send_message(CHANNEL_ID, "ㅤ", reply_markup=kb, reply_to_message_id=msgs[0].message_id)
        return msgs[0].message_id

    # single media
    kwargs = {"reply_markup": kb, "caption": post["text"]}
    if post["content_type"] == "photo":
        msg = await bot.send_photo(CHANNEL_ID, post["media_file_id"], **kwargs)
    elif post["content_type"] == "video":
        msg = await bot.send_video(CHANNEL_ID, post["media_file_id"], **kwargs)
    elif post["content_type"] == "animation":
        msg = await bot.send_animation(CHANNEL_ID, post["media_file_id"], **kwargs)
    elif post["content_type"] == "voice":
        msg = await bot.send_voice(CHANNEL_ID, post["media_file_id"], **kwargs)
    elif post["content_type"] == "video_note":
        msg = await bot.send_video_note(CHANNEL_ID, post["media_file_id"])
        await bot.send_message(CHANNEL_ID, "ㅤ", reply_markup=kb, reply_to_message_id=msg.message_id)
        return msg.message_id
    elif post["content_type"] == "document":
        msg = await bot.send_document(CHANNEL_ID, post["media_file_id"], **kwargs)
    elif post["content_type"] == "audio":
        msg = await bot.send_audio(CHANNEL_ID, post["media_file_id"], **kwargs)
    else:
        msg = await bot.send_message(CHANNEL_ID, post["text"] or "—", reply_markup=kb)
    return msg.message_id


# ============================================================================
# 10. LIKES / COMMENTS / REPORTS
# ============================================================================

@router.callback_query(F.data.startswith("like:"))
async def cb_like(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    if await is_banned(user_id):
        await callback.answer("⛔ Заблокирован", show_alert=True)
        return

    existing = await db_execute(
        "SELECT like_id FROM likes WHERE post_id=? AND user_id=?",
        (post_id, user_id), fetch="one"
    )
    if existing:
        await db_execute("DELETE FROM likes WHERE post_id=? AND user_id=?", (post_id, user_id))
        await callback.answer("Лайк снят")
    else:
        await db_execute(
            "INSERT OR IGNORE INTO likes (post_id, user_id, created_at) VALUES (?, ?, ?)",
            (post_id, user_id, now_iso()),
        )
        await callback.answer("❤️")

    # обновляем счётчик на кнопке (если сообщение ещё доступно)
    count = await db_execute(
        "SELECT COUNT(*) as cnt FROM likes WHERE post_id=?", (post_id,), fetch="one"
    )
    try:
        await callback.message.edit_reply_markup(
            reply_markup=channel_post_kb(post_id, count["cnt"] if count else 0)
        )
    except TelegramBadRequest:
        pass


@router.callback_query(F.data.startswith("comments:"))
async def cb_comments(callback: CallbackQuery, state: FSMContext):
    parts = callback.data.split(":")
    post_id = int(parts[1])
    page = int(parts[2]) if len(parts) > 2 else 0
    user_id = callback.from_user.id

    if await is_banned(user_id):
        await callback.answer("⛔ Заблокирован", show_alert=True)
        return

    post = await db_execute("SELECT * FROM posts WHERE post_id=?", (post_id,), fetch="one")
    if not post or not post["comments_enabled"]:
        await callback.answer("Комментарии отключены", show_alert=True)
        return

    per_page = 5
    comments = await db_execute(
        "SELECT * FROM comments WHERE post_id=? AND is_deleted=0 ORDER BY created_at DESC LIMIT ? OFFSET ?",
        (post_id, per_page, page * per_page), fetch="all"
    )
    total = await db_execute(
        "SELECT COUNT(*) as cnt FROM comments WHERE post_id=? AND is_deleted=0",
        (post_id,), fetch="one"
    )
    total_cnt = total["cnt"] if total else 0

    if not comments and page == 0:
        text = "Комментариев пока нет."
    else:
        lines = []
        for c in comments:
            lines.append(
                f"<b>Анонимный пользователь</b> · {c['created_at'][:16]}\n"
                f"{c['text']}"
            )
        text = "\n\n".join(lines) if lines else "Больше комментариев нет."

    builder = InlineKeyboardBuilder()
    builder.button(text="✍️ Написать комментарий", callback_data=f"write_comment:{post_id}")
    if page > 0:
        builder.button(text="⬅️", callback_data=f"comments:{post_id}:{page-1}")
    if (page + 1) * per_page < total_cnt:
        builder.button(text="➡️", callback_data=f"comments:{post_id}:{page+1}")
    builder.button(text="◀️ Назад", callback_data="noop")
    builder.adjust(1, 2)

    await callback.message.answer(
        f"<b>Комментарии</b> (стр. {page+1})\n\n{text}",
        reply_markup=builder.as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data.startswith("write_comment:"))
async def cb_write_comment(callback: CallbackQuery, state: FSMContext):
    post_id = int(callback.data.split(":")[1])
    if await is_banned(callback.from_user.id):
        await callback.answer("⛔ Заблокирован", show_alert=True)
        return
    key = f"comment:{callback.from_user.id}"
    if not check_rate_limit(key, ANTISPAM["max_comments_per_hour"], 3600):
        await callback.answer("Слишком много комментариев", show_alert=True)
        return

    await state.set_state(UserStates.waiting_comment)
    await state.update_data(comment_post_id=post_id)
    await callback.message.answer(
        f"Напиши комментарий (макс. {ANTISPAM['max_comment_length']} символов):",
        reply_markup=cancel_kb("cancel_comment"),
    )
    await callback.answer()


@router.callback_query(F.data == "cancel_comment")
async def cb_cancel_comment(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Отменено")
    await callback.message.answer("Главное меню:", reply_markup=main_menu_kb())


@router.message(StateFilter(UserStates.waiting_comment))
async def process_comment(message: Message, state: FSMContext):
    data = await state.get_data()
    post_id = data.get("comment_post_id")
    if not post_id:
        await state.clear()
        return
    text = (message.text or "").strip()
    if not text or len(text) > ANTISPAM["max_comment_length"]:
        await message.answer("Некорректный комментарий.")
        return

    key_cd = f"comment_cd:{message.from_user.id}"
    if not check_rate_limit(key_cd, 1, ANTISPAM["comment_cooldown_seconds"]):
        await message.answer(f"Подожди {ANTISPAM['comment_cooldown_seconds']} сек.")
        return

    await db_execute(
        "INSERT INTO comments (post_id, user_id, text, created_at) VALUES (?, ?, ?, ?)",
        (post_id, message.from_user.id, text, now_iso()),
    )
    await log_action("comment_added", message.from_user.id, f"post_id={post_id}")

    # уведомление автору поста (без личности комментатора)
    post = await db_execute("SELECT user_id FROM posts WHERE post_id=?", (post_id,), fetch="one")
    if post and post["user_id"] != message.from_user.id:
        try:
            await bot.send_message(
                post["user_id"],
                f"💬 К твоей публикации появился новый анонимный комментарий.",
            )
        except Exception:
            pass

    await state.clear()
    await message.answer("Комментарий добавлен.", reply_markup=main_menu_kb())


@router.callback_query(F.data.startswith("report:"))
async def cb_report(callback: CallbackQuery, state: FSMContext):
    post_id = int(callback.data.split(":")[1])
    user_id = callback.from_user.id
    if await is_banned(user_id):
        await callback.answer("⛔", show_alert=True)
        return

    # защита от повторных жалоб
    existing = await db_execute(
        "SELECT report_id FROM reports WHERE post_id=? AND user_id=?",
        (post_id, user_id), fetch="one"
    )
    if existing:
        await callback.answer("Ты уже жаловался на эту публикацию", show_alert=True)
        return

    key = f"report:{user_id}"
    if not check_rate_limit(key, ANTISPAM["max_reports_per_day"], 86400):
        await callback.answer("Слишком много жалоб за сутки", show_alert=True)
        return

    builder = InlineKeyboardBuilder()
    reasons = ["Спам", "Оскорбления", "Неприемлемый контент", "Нарушение правил", "Другое"]
    for r in reasons:
        builder.button(text=r, callback_data=f"report_reason:{post_id}:{r}")
    builder.button(text="❌ Отмена", callback_data="cancel")
    builder.adjust(1)
    await callback.message.answer("Выбери причину жалобы:", reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("report_reason:"))
async def cb_report_reason(callback: CallbackQuery):
    parts = callback.data.split(":")
    post_id = int(parts[1])
    reason = parts[2]
    user_id = callback.from_user.id

    await db_execute(
        "INSERT INTO reports (post_id, user_id, reason, status, created_at) VALUES (?, ?, ?, 'open', ?)",
        (post_id, user_id, reason, now_iso()),
    )
    await log_action("report_created", user_id, f"post_id={post_id} reason={reason}")

    # уведомление админу
    try:
        await bot.send_message(
            ADMIN_ID,
            f"🚩 Новая жалоба\nПост: {post_id}\nПричина: {reason}\nОт: <code>{user_id}</code>",
            reply_markup=InlineKeyboardBuilder()
            .button(text="Открыть жалобы", callback_data="admin:reports:0")
            .as_markup(),
        )
    except Exception:
        pass

    await callback.answer("Жалоба отправлена")
    await callback.message.edit_text("Спасибо, жалоба передана модераторам.")


# ============================================================================
# 11. SUPPORT
# ============================================================================

@router.callback_query(F.data == "support")
async def cb_support(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    if await is_banned(callback.from_user.id):
        await callback.message.answer("⛔ Вы заблокированы.")
        return
    key = f"support:{callback.from_user.id}"
    if not check_rate_limit(key, 5, 3600):
        await callback.message.answer("Слишком много обращений. Подожди.")
        return

    await state.set_state(UserStates.waiting_support)
    await callback.message.answer(
        "Опиши проблему или задай вопрос. Можно прикрепить файл/фото.",
        reply_markup=cancel_kb("cancel_support"),
    )


@router.callback_query(F.data == "cancel_support")
async def cb_cancel_support(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Отменено")
    await callback.message.answer("Главное меню:", reply_markup=main_menu_kb())


@router.message(StateFilter(UserStates.waiting_support))
async def process_support(message: Message, state: FSMContext):
    user_id = message.from_user.id
    text = message.text or message.caption or ""
    content_type = "text"
    file_id = None

    if message.photo:
        file_id = message.photo[-1].file_id
        content_type = "photo"
    elif message.document:
        file_id = message.document.file_id
        content_type = "document"
    elif message.video:
        file_id = message.video.file_id
        content_type = "video"

    # открытый тикет или новый
    ticket = await db_execute(
        "SELECT * FROM support_tickets WHERE user_id=? AND status='open' ORDER BY ticket_id DESC LIMIT 1",
        (user_id,), fetch="one"
    )
    if not ticket:
        ticket_id = await db_execute(
            "INSERT INTO support_tickets (user_id, status, created_at, updated_at) VALUES (?, 'open', ?, ?)",
            (user_id, now_iso(), now_iso()),
        )
    else:
        ticket_id = ticket["ticket_id"]
        await db_execute(
            "UPDATE support_tickets SET updated_at=? WHERE ticket_id=?",
            (now_iso(), ticket_id),
        )

    await db_execute(
        "INSERT INTO support_messages (ticket_id, sender_id, is_admin, content_type, text, media_file_id, created_at) "
        "VALUES (?, ?, 0, ?, ?, ?, ?)",
        (ticket_id, user_id, content_type, text, file_id, now_iso()),
    )
    await log_action("support_message", user_id, f"ticket_id={ticket_id}")

    await state.clear()
    await message.answer(
        "Обращение принято. Администратор ответит в ближайшее время.",
        reply_markup=main_menu_kb(),
    )

    # notify admin
    try:
        await bot.send_message(
            ADMIN_ID,
            f"💬 Новое обращение в поддержку\n"
            f"Тикет: #{ticket_id}\n"
            f"От: <code>{user_id}</code> @{message.from_user.username or '—'}\n"
            f"Текст: {text[:300] or '(медиа)'}",
            reply_markup=InlineKeyboardBuilder()
            .button(text="Ответить", callback_data=f"admin:support_reply:{ticket_id}")
            .button(text="Открыть тикеты", callback_data="admin:support:open:0")
            .adjust(1)
            .as_markup(),
        )
        if file_id:
            if content_type == "photo":
                await bot.send_photo(ADMIN_ID, file_id)
            elif content_type == "document":
                await bot.send_document(ADMIN_ID, file_id)
            elif content_type == "video":
                await bot.send_video(ADMIN_ID, file_id)
    except Exception as e:
        logger.exception("Support notify failed: %s", e)


# ============================================================================
# 12. ADMIN PANEL
# ============================================================================

@router.message(Command("apanel"))
async def cmd_apanel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("⛔")
        return
    if admin_session_active(message.from_user.id):
        touch_admin_session(message.from_user.id)
        await message.answer("Админ-панель:", reply_markup=admin_main_kb())
        return

    await state.set_state(AdminStates.waiting_secret)
    await message.answer("Введи секретный код:", reply_markup=cancel_kb("admin:cancel"))


@router.message(StateFilter(AdminStates.waiting_secret))
async def process_secret(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await state.clear()
        return
    # удаляем сообщение с кодом
    try:
        await message.delete()
    except Exception:
        pass

    if message.text and message.text.strip() == SECRET_CODE:
        touch_admin_session(message.from_user.id)
        await state.clear()
        await message.answer("✅ Доступ разрешён.\nАдмин-панель:", reply_markup=admin_main_kb())
        await log_action("admin_login", message.from_user.id)
    else:
        await state.clear()
        await message.answer("❌ Неверный код.")
        await log_action("admin_login_failed", message.from_user.id)


@router.callback_query(F.data == "admin:cancel")
async def cb_admin_cancel(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Отменено")
    if admin_session_active(callback.from_user.id):
        await callback.message.answer("Админ-панель:", reply_markup=admin_main_kb())


@router.callback_query(F.data == "admin:logout")
async def cb_admin_logout(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("⛔", show_alert=True)
        return
    admin_sessions.pop(callback.from_user.id, None)
    await callback.answer("Выход")
    await callback.message.edit_text("Сессия завершена.")


def require_admin(handler):
    """Декоратор-проверка админа + сессии."""
    async def wrapper(callback: CallbackQuery, *args, **kwargs):
        if not is_admin(callback.from_user.id) or not admin_session_active(callback.from_user.id):
            await callback.answer("⛔ Сессия истекла или нет доступа. /apanel", show_alert=True)
            return
        touch_admin_session(callback.from_user.id)
        return await handler(callback, *args, **kwargs)
    return wrapper


@router.callback_query(F.data.startswith("admin:posts:"))
@require_admin
async def admin_posts(callback: CallbackQuery):
    page = int(callback.data.split(":")[2])
    per_page = 10
    posts = await db_execute(
        "SELECT p.*, u.username FROM posts p LEFT JOIN users u ON p.user_id=u.user_id "
        "ORDER BY p.post_id DESC LIMIT ? OFFSET ?",
        (per_page, page * per_page), fetch="all"
    )
    if not posts:
        await callback.answer("Пусто", show_alert=True)
        return

    lines = []
    for p in posts:
        lines.append(
            f"#{p['post_id']} | <code>{p['user_id']}</code> @{p['username'] or '—'} | "
            f"{p['content_type']} | {p['status']} | {p['created_at'][:16]}\n"
            f"{(p['text'] or '')[:80]}"
        )
    text = "<b>Последние публикации</b>\n\n" + "\n\n".join(lines)

    builder = InlineKeyboardBuilder()
    if page > 0:
        builder.button(text="⬅️", callback_data=f"admin:posts:{page-1}")
    builder.button(text="➡️", callback_data=f"admin:posts:{page+1}")
    builder.button(text="◀️ Меню", callback_data="admin:menu")
    builder.adjust(2, 1)

    await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:moderation:"))
@require_admin
async def admin_moderation(callback: CallbackQuery):
    page = int(callback.data.split(":")[2])
    posts = await db_execute(
        "SELECT * FROM posts WHERE status='pending' ORDER BY post_id DESC LIMIT 10 OFFSET ?",
        (page * 10,), fetch="all"
    )
    if not posts:
        await callback.message.edit_text(
            "Нет сообщений на модерации.",
            reply_markup=InlineKeyboardBuilder().button(text="◀️ Меню", callback_data="admin:menu").as_markup(),
        )
        await callback.answer()
        return

    text = "<b>На модерации</b>\n\n"
    builder = InlineKeyboardBuilder()
    for p in posts:
        text += f"#{p['post_id']} | {p['content_type']} | {p['created_at'][:16]}\n{(p['text'] or '')[:60]}\n\n"
        builder.button(text=f"Открыть #{p['post_id']}", callback_data=f"admin:view_post:{p['post_id']}")
    builder.button(text="◀️ Меню", callback_data="admin:menu")
    builder.adjust(1)
    await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:view_post:"))
@require_admin
async def admin_view_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[2])
    post = await db_execute("SELECT * FROM posts WHERE post_id=?", (post_id,), fetch="one")
    if not post:
        await callback.answer("Не найден", show_alert=True)
        return
    # просто пересылаем админу через notify (повторно)
    await notify_admin_new_post(post_id)
    await callback.answer("Отправлено")


@router.callback_query(F.data == "admin:users")
@require_admin
async def admin_users(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_user_search)
    await callback.message.answer(
        "Введи Telegram ID пользователя для поиска:",
        reply_markup=cancel_kb("admin:cancel"),
    )
    await callback.answer()


@router.message(StateFilter(AdminStates.waiting_user_search))
async def admin_user_search(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not admin_session_active(message.from_user.id):
        await state.clear()
        return
    try:
        uid = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой ID.")
        return

    user = await db_execute("SELECT * FROM users WHERE user_id=?", (uid,), fetch="one")
    if not user:
        await message.answer("Пользователь не найден.")
        await state.clear()
        return

    posts_cnt = await db_execute(
        "SELECT COUNT(*) as cnt FROM posts WHERE user_id=?", (uid,), fetch="one"
    )
    text = (
        f"<b>Пользователь</b>\n"
        f"ID: <code>{user['user_id']}</code>\n"
        f"Username: @{user['username'] or '—'}\n"
        f"Имя: {user['first_name'] or '—'}\n"
        f"Регистрация: {user['registration_date'][:19]}\n"
        f"Забанен: {'да' if user['is_banned'] else 'нет'}\n"
        f"Причина бана: {user['ban_reason'] or '—'}\n"
        f"Публикаций: {posts_cnt['cnt'] if posts_cnt else 0}"
    )
    builder = InlineKeyboardBuilder()
    if user["is_banned"]:
        builder.button(text="✅ Разбанить", callback_data=f"admin:unban:{uid}")
    else:
        builder.button(text="🚫 Забанить", callback_data=f"admin:ban:{uid}")
    builder.button(text="◀️ Меню", callback_data="admin:menu")
    builder.adjust(1)
    await message.answer(text, reply_markup=builder.as_markup())
    await state.clear()


@router.callback_query(F.data.startswith("admin:ban:"))
@require_admin
async def admin_ban_start(callback: CallbackQuery, state: FSMContext):
    uid = int(callback.data.split(":")[2])
    await state.set_state(AdminStates.waiting_ban_reason)
    await state.update_data(ban_user_id=uid)
    await callback.message.answer(
        f"Причина блокировки <code>{uid}</code>:",
        reply_markup=cancel_kb("admin:cancel"),
    )
    await callback.answer()


@router.message(StateFilter(AdminStates.waiting_ban_reason))
async def admin_ban_reason(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not admin_session_active(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    uid = data.get("ban_user_id")
    reason = (message.text or "без указания").strip()[:500]
    await db_execute(
        "UPDATE users SET is_banned=1, ban_reason=? WHERE user_id=?",
        (reason, uid),
    )
    await log_action("user_banned", message.from_user.id, f"user={uid} reason={reason}")
    await state.clear()
    await message.answer(f"Пользователь <code>{uid}</code> заблокирован.")
    if admin_session_active(message.from_user.id):
        await message.answer("Админ-панель:", reply_markup=admin_main_kb())


@router.callback_query(F.data.startswith("admin:unban:"))
@require_admin
async def admin_unban(callback: CallbackQuery):
    uid = int(callback.data.split(":")[2])
    await db_execute(
        "UPDATE users SET is_banned=0, ban_reason=NULL WHERE user_id=?", (uid,)
    )
    await log_action("user_unbanned", callback.from_user.id, f"user={uid}")
    await callback.answer("Разбанен")
    await callback.message.edit_text(f"Пользователь <code>{uid}</code> разблокирован.")


@router.callback_query(F.data.startswith("admin:banned:"))
@require_admin
async def admin_banned_list(callback: CallbackQuery):
    page = int(callback.data.split(":")[2])
    users = await db_execute(
        "SELECT * FROM users WHERE is_banned=1 ORDER BY user_id DESC LIMIT 10 OFFSET ?",
        (page * 10,), fetch="all"
    )
    if not users:
        text = "Нет заблокированных."
    else:
        lines = [
            f"<code>{u['user_id']}</code> @{u['username'] or '—'} — {u['ban_reason'] or '—'}"
            for u in users
        ]
        text = "<b>Заблокированные</b>\n\n" + "\n".join(lines)

    builder = InlineKeyboardBuilder()
    if page > 0:
        builder.button(text="⬅️", callback_data=f"admin:banned:{page-1}")
    builder.button(text="➡️", callback_data=f"admin:banned:{page+1}")
    builder.button(text="◀️ Меню", callback_data="admin:menu")
    builder.adjust(2, 1)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:reports:"))
@require_admin
async def admin_reports(callback: CallbackQuery):
    page = int(callback.data.split(":")[2])
    reports = await db_execute(
        "SELECT r.*, p.status as post_status FROM reports r "
        "LEFT JOIN posts p ON r.post_id=p.post_id "
        "WHERE r.status='open' ORDER BY r.report_id DESC LIMIT 10 OFFSET ?",
        (page * 10,), fetch="all"
    )
    if not reports:
        text = "Открытых жалоб нет."
    else:
        lines = []
        for r in reports:
            lines.append(
                f"#{r['report_id']} пост {r['post_id']} | {r['reason']} | "
                f"от <code>{r['user_id']}</code> | {r['created_at'][:16]}"
            )
        text = "<b>Жалобы</b>\n\n" + "\n".join(lines)

    builder = InlineKeyboardBuilder()
    for r in (reports or [])[:5]:
        builder.button(
            text=f"Жалоба #{r['report_id']}",
            callback_data=f"admin:report_view:{r['report_id']}",
        )
    if page > 0:
        builder.button(text="⬅️", callback_data=f"admin:reports:{page-1}")
    builder.button(text="➡️", callback_data=f"admin:reports:{page+1}")
    builder.button(text="◀️ Меню", callback_data="admin:menu")
    builder.adjust(1)
    await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:report_view:"))
@require_admin
async def admin_report_view(callback: CallbackQuery):
    report_id = int(callback.data.split(":")[2])
    report = await db_execute("SELECT * FROM reports WHERE report_id=?", (report_id,), fetch="one")
    if not report:
        await callback.answer("Не найдена", show_alert=True)
        return
    post = await db_execute("SELECT * FROM posts WHERE post_id=?", (report["post_id"],), fetch="one")

    text = (
        f"<b>Жалоба #{report_id}</b>\n"
        f"Пост: {report['post_id']} (статус {post['status'] if post else '?'})\n"
        f"Причина: {report['reason']}\n"
        f"От: <code>{report['user_id']}</code>\n"
        f"Дата: {report['created_at'][:19]}"
    )
    builder = InlineKeyboardBuilder()
    builder.button(text="🗑 Удалить пост из канала", callback_data=f"admin:delete_post:{report['post_id']}")
    builder.button(text="✅ Отклонить жалобу", callback_data=f"admin:report_reject:{report_id}")
    builder.button(text="🚫 Бан автора поста", callback_data=f"admin:ban:{post['user_id'] if post else 0}")
    builder.button(text="◀️ Назад", callback_data="admin:reports:0")
    builder.adjust(1)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:report_reject:"))
@require_admin
async def admin_report_reject(callback: CallbackQuery):
    report_id = int(callback.data.split(":")[2])
    await db_execute("UPDATE reports SET status='rejected' WHERE report_id=?", (report_id,))
    await callback.answer("Жалоба отклонена")
    await callback.message.edit_text("Жалоба отклонена.")


@router.callback_query(F.data.startswith("admin:delete_post:"))
@require_admin
async def admin_delete_post(callback: CallbackQuery):
    post_id = int(callback.data.split(":")[2])
    post = await db_execute("SELECT * FROM posts WHERE post_id=?", (post_id,), fetch="one")
    if post and post["channel_message_id"]:
        try:
            await bot.delete_message(CHANNEL_ID, post["channel_message_id"])
        except Exception as e:
            logger.warning("Cannot delete channel message: %s", e)
    await db_execute("UPDATE posts SET status='rejected' WHERE post_id=?", (post_id,))
    await log_action("post_deleted_from_channel", callback.from_user.id, f"post_id={post_id}")
    await callback.answer("Удалено")
    await callback.message.edit_text(f"Пост #{post_id} удалён из канала и отклонён.")


@router.callback_query(F.data.startswith("admin:support:"))
@require_admin
async def admin_support_list(callback: CallbackQuery):
    parts = callback.data.split(":")
    status = parts[2]  # open / closed
    page = int(parts[3])
    tickets = await db_execute(
        "SELECT t.*, u.username FROM support_tickets t "
        "LEFT JOIN users u ON t.user_id=u.user_id "
        "WHERE t.status=? ORDER BY t.updated_at DESC LIMIT 10 OFFSET ?",
        (status, page * 10), fetch="all"
    )
    text = f"<b>Тикеты ({status})</b>\n\n"
    builder = InlineKeyboardBuilder()
    if not tickets:
        text += "Пусто."
    else:
        for t in tickets:
            text += f"#{t['ticket_id']} | <code>{t['user_id']}</code> @{t['username'] or '—'} | {t['updated_at'][:16]}\n"
            builder.button(text=f"Тикет #{t['ticket_id']}", callback_data=f"admin:ticket:{t['ticket_id']}")
    builder.button(text="Открытые", callback_data="admin:support:open:0")
    builder.button(text="Закрытые", callback_data="admin:support:closed:0")
    builder.button(text="◀️ Меню", callback_data="admin:menu")
    builder.adjust(1)
    await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:ticket:"))
@require_admin
async def admin_ticket_view(callback: CallbackQuery):
    ticket_id = int(callback.data.split(":")[2])
    ticket = await db_execute(
        "SELECT * FROM support_tickets WHERE ticket_id=?", (ticket_id,), fetch="one"
    )
    if not ticket:
        await callback.answer("Не найден", show_alert=True)
        return
    messages = await db_execute(
        "SELECT * FROM support_messages WHERE ticket_id=? ORDER BY created_at",
        (ticket_id,), fetch="all"
    )
    lines = []
    for m in messages:
        who = "Админ" if m["is_admin"] else f"User <code>{m['sender_id']}</code>"
        lines.append(f"{who} · {m['created_at'][:16]}\n{m['text'] or '(медиа)'}")
    text = f"<b>Тикет #{ticket_id}</b> ({ticket['status']})\n\n" + "\n\n".join(lines[-15:])

    builder = InlineKeyboardBuilder()
    if ticket["status"] == "open":
        builder.button(text="✍️ Ответить", callback_data=f"admin:support_reply:{ticket_id}")
        builder.button(text="🔒 Закрыть", callback_data=f"admin:support_close:{ticket_id}")
    builder.button(text="◀️ Назад", callback_data="admin:support:open:0")
    builder.adjust(1)
    await callback.message.edit_text(text[:4000], reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data.startswith("admin:support_reply:"))
@require_admin
async def admin_support_reply_start(callback: CallbackQuery, state: FSMContext):
    ticket_id = int(callback.data.split(":")[2])
    await state.set_state(AdminStates.waiting_support_reply)
    await state.update_data(reply_ticket_id=ticket_id)
    await callback.message.answer(
        f"Ответ на тикет #{ticket_id}:",
        reply_markup=cancel_kb("admin:cancel"),
    )
    await callback.answer()


@router.message(StateFilter(AdminStates.waiting_support_reply))
async def admin_support_reply(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not admin_session_active(message.from_user.id):
        await state.clear()
        return
    data = await state.get_data()
    ticket_id = data.get("reply_ticket_id")
    ticket = await db_execute(
        "SELECT * FROM support_tickets WHERE ticket_id=?", (ticket_id,), fetch="one"
    )
    if not ticket:
        await state.clear()
        await message.answer("Тикет не найден.")
        return

    text = message.text or message.caption or ""
    file_id = None
    content_type = "text"
    if message.photo:
        file_id = message.photo[-1].file_id
        content_type = "photo"
    elif message.document:
        file_id = message.document.file_id
        content_type = "document"

    await db_execute(
        "INSERT INTO support_messages (ticket_id, sender_id, is_admin, content_type, text, media_file_id, created_at) "
        "VALUES (?, ?, 1, ?, ?, ?, ?)",
        (ticket_id, message.from_user.id, content_type, text, file_id, now_iso()),
    )
    await db_execute(
        "UPDATE support_tickets SET updated_at=? WHERE ticket_id=?",
        (now_iso(), ticket_id),
    )
    await log_action("support_reply", message.from_user.id, f"ticket_id={ticket_id}")

    # отправка пользователю
    try:
        if content_type == "photo" and file_id:
            await bot.send_photo(ticket["user_id"], file_id, caption=f"Ответ поддержки:\n{text}")
        elif content_type == "document" and file_id:
            await bot.send_document(ticket["user_id"], file_id, caption=f"Ответ поддержки:\n{text}")
        else:
            await bot.send_message(ticket["user_id"], f"Ответ поддержки:\n\n{text}")
    except Exception as e:
        await message.answer(f"Не удалось отправить пользователю: {e}")

    await state.clear()
    await message.answer("Ответ отправлен.")
    await message.answer("Админ-панель:", reply_markup=admin_main_kb())


@router.callback_query(F.data.startswith("admin:support_close:"))
@require_admin
async def admin_support_close(callback: CallbackQuery):
    ticket_id = int(callback.data.split(":")[2])
    await db_execute(
        "UPDATE support_tickets SET status='closed', updated_at=? WHERE ticket_id=?",
        (now_iso(), ticket_id),
    )
    await callback.answer("Закрыт")
    await callback.message.edit_text(f"Тикет #{ticket_id} закрыт.")


@router.callback_query(F.data.startswith("admin:logs:"))
@require_admin
async def admin_logs(callback: CallbackQuery):
    page = int(callback.data.split(":")[2])
    logs = await db_execute(
        "SELECT * FROM logs ORDER BY log_id DESC LIMIT 20 OFFSET ?",
        (page * 20,), fetch="all"
    )
    if not logs:
        text = "Логов нет."
    else:
        lines = [
            f"{l['created_at'][:19]} | {l['action']} | uid={l['user_id']} | {l['details'] or ''}"
            for l in logs
        ]
        text = "<b>Логи</b>\n\n<code>" + "\n".join(lines)[:3500] + "</code>"

    builder = InlineKeyboardBuilder()
    if page > 0:
        builder.button(text="⬅️", callback_data=f"admin:logs:{page-1}")
    builder.button(text="➡️", callback_data=f"admin:logs:{page+1}")
    builder.button(text="◀️ Меню", callback_data="admin:menu")
    builder.adjust(2, 1)
    await callback.message.edit_text(text, reply_markup=builder.as_markup())
    await callback.answer()


@router.callback_query(F.data == "admin:broadcast")
@require_admin
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    await state.set_state(AdminStates.waiting_broadcast)
    await callback.message.answer(
        "Отправь сообщение для рассылки (текст / фото / видео с подписью).\n"
        "После этого будет запрошено подтверждение.",
        reply_markup=cancel_kb("admin:cancel"),
    )
    await callback.answer()


@router.message(StateFilter(AdminStates.waiting_broadcast))
async def admin_broadcast_receive(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id) or not admin_session_active(message.from_user.id):
        await state.clear()
        return

    data = {
        "text": message.text or message.caption or "",
        "content_type": "text",
        "file_id": None,
    }
    if message.photo:
        data["content_type"] = "photo"
        data["file_id"] = message.photo[-1].file_id
    elif message.video:
        data["content_type"] = "video"
        data["file_id"] = message.video.file_id
    elif message.animation:
        data["content_type"] = "animation"
        data["file_id"] = message.animation.file_id

    await state.update_data(broadcast=data)
    await state.set_state(AdminStates.waiting_broadcast_confirm)

    users_cnt = await db_execute("SELECT COUNT(*) as cnt FROM users WHERE is_banned=0", fetch="one")
    await message.answer(
        f"Получателей (не забаненных): {users_cnt['cnt'] if users_cnt else 0}\n\n"
        f"Текст: {data['text'][:200] or '(медиа)'}\n\n"
        "Подтвердить рассылку?",
        reply_markup=InlineKeyboardBuilder()
        .button(text="✅ Да, отправить", callback_data="admin:broadcast_go")
        .button(text="❌ Отмена", callback_data="admin:cancel")
        .adjust(1)
        .as_markup(),
    )


@router.callback_query(F.data == "admin:broadcast_go")
@require_admin
async def admin_broadcast_go(callback: CallbackQuery, state: FSMContext):
    data = await state.get_data()
    broadcast = data.get("broadcast")
    if not broadcast:
        await callback.answer("Нет данных", show_alert=True)
        await state.clear()
        return

    await state.clear()
    await callback.message.edit_text("Рассылка начата...")

    users = await db_execute(
        "SELECT user_id FROM users WHERE is_banned=0", fetch="all"
    )
    total = len(users)
    success = 0
    failed = 0
    blocked = 0

    for u in users:
        try:
            if broadcast["content_type"] == "photo" and broadcast["file_id"]:
                await bot.send_photo(u["user_id"], broadcast["file_id"], caption=broadcast["text"] or None)
            elif broadcast["content_type"] == "video" and broadcast["file_id"]:
                await bot.send_video(u["user_id"], broadcast["file_id"], caption=broadcast["text"] or None)
            elif broadcast["content_type"] == "animation" and broadcast["file_id"]:
                await bot.send_animation(u["user_id"], broadcast["file_id"], caption=broadcast["text"] or None)
            else:
                await bot.send_message(u["user_id"], broadcast["text"] or "—")
            success += 1
        except TelegramForbiddenError:
            blocked += 1
            failed += 1
        except TelegramRetryAfter as e:
            await asyncio.sleep(e.retry_after + 1)
            try:
                await bot.send_message(u["user_id"], broadcast["text"] or "—")
                success += 1
            except Exception:
                failed += 1
        except Exception:
            failed += 1
        await asyncio.sleep(ANTISPAM["broadcast_delay_seconds"])

    await log_action(
        "broadcast",
        callback.from_user.id,
        f"total={total} success={success} failed={failed} blocked={blocked}",
    )
    await callback.message.answer(
        f"<b>Рассылка завершена</b>\n"
        f"Всего: {total}\n"
        f"Успешно: {success}\n"
        f"Ошибки: {failed}\n"
        f"Заблокировали бота: {blocked}",
        reply_markup=admin_main_kb(),
    )


@router.callback_query(F.data == "admin:stats")
@require_admin
async def admin_stats(callback: CallbackQuery):
    now = datetime.utcnow()
    day_ago = (now - timedelta(days=1)).isoformat()
    week_ago = (now - timedelta(days=7)).isoformat()

    def q(sql, params=()):
        return db_execute(sql, params, fetch="one")

    total_users = (await q("SELECT COUNT(*) as c FROM users"))["c"]
    banned = (await q("SELECT COUNT(*) as c FROM users WHERE is_banned=1"))["c"]
    total_posts = (await q("SELECT COUNT(*) as c FROM posts"))["c"]
    published = (await q("SELECT COUNT(*) as c FROM posts WHERE status='published'"))["c"]
    rejected = (await q("SELECT COUNT(*) as c FROM posts WHERE status='rejected'"))["c"]
    pending = (await q("SELECT COUNT(*) as c FROM posts WHERE status='pending'"))["c"]
    comments = (await q("SELECT COUNT(*) as c FROM comments WHERE is_deleted=0"))["c"]
    reports = (await q("SELECT COUNT(*) as c FROM reports"))["c"]
    tickets = (await q("SELECT COUNT(*) as c FROM support_tickets"))["c"]

    posts_24 = (await q("SELECT COUNT(*) as c FROM posts WHERE created_at > ?", (day_ago,)))["c"]
    posts_7 = (await q("SELECT COUNT(*) as c FROM posts WHERE created_at > ?", (week_ago,)))["c"]
    users_24 = (await q("SELECT COUNT(*) as c FROM users WHERE registration_date > ?", (day_ago,)))["c"]

    text = (
        "<b>📊 Статистика</b>\n\n"
        f"Пользователей всего: <b>{total_users}</b>\n"
        f"Заблокировано: <b>{banned}</b>\n"
        f"Новых за 24ч: <b>{users_24}</b>\n\n"
        f"Публикаций всего: <b>{total_posts}</b>\n"
        f"  • опубликовано: {published}\n"
        f"  • отклонено: {rejected}\n"
        f"  • на модерации: {pending}\n"
        f"  • за 24ч: {posts_24}\n"
        f"  • за 7 дней: {posts_7}\n\n"
        f"Комментариев: <b>{comments}</b>\n"
        f"Жалоб: <b>{reports}</b>\n"
        f"Обращений в поддержку: <b>{tickets}</b>"
    )
    await callback.message.edit_text(
        text,
        reply_markup=InlineKeyboardBuilder().button(text="◀️ Меню", callback_data="admin:menu").as_markup(),
    )
    await callback.answer()


@router.callback_query(F.data == "admin:menu")
@require_admin
async def admin_menu(callback: CallbackQuery):
    await callback.message.edit_text("Админ-панель:", reply_markup=admin_main_kb())
    await callback.answer()


@router.callback_query(F.data == "noop")
async def cb_noop(callback: CallbackQuery):
    await callback.answer()


@router.callback_query(F.data == "cancel")
async def cb_cancel_generic(callback: CallbackQuery, state: FSMContext):
    await state.clear()
    await callback.answer("Отменено")
    await callback.message.answer("Главное меню:", reply_markup=main_menu_kb())


# ============================================================================
# 13. ERROR HANDLING & STARTUP
# ============================================================================

@router.errors()
async def errors_handler(event, exception):
    logger.exception("Unhandled error: %s", exception)
    await log_action("error", None, str(exception)[:500])
    return True


async def main():
    if BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("=" * 60)
        print("ВНИМАНИЕ: укажи BOT_TOKEN и CHANNEL_ID в начале файла bot.py")
        print("=" * 60)
        return

    await init_db()
    logger.info("Bot starting...")
    await dp.start_polling(bot)


if __name__ == "__main__":
    asyncio.run(main())
