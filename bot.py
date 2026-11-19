#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Подслушано — анонимный Telegram-бот с премодерацией.
Вся логика в одном файле bot.py (aiogram 3 + aiosqlite + TOTP).
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import shutil
import sys
import traceback
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Union

import aiosqlite
import pyotp
import qrcode
from aiogram import Bot, Dispatcher, F, Router
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ChatMemberStatus, ChatType, ContentType, ParseMode
from aiogram.exceptions import (
    TelegramAPIError,
    TelegramBadRequest,
    TelegramForbiddenError,
    TelegramNetworkError,
    TelegramRetryAfter,
)
from aiogram.filters import Command, CommandObject, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    BufferedInputFile,
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    InputMediaPhoto,
    InputMediaVideo,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
    ReplyKeyboardRemove,
    FSInputFile,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder
from dotenv import load_dotenv
from PIL import Image

# ---------------------------------------------------------------------------
# Конфигурация и пути
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
ENV_PATH = BASE_DIR / ".env"
DB_PATH = BASE_DIR / "bot.db"
BACKUPS_DIR = BASE_DIR / "backups"

load_dotenv(ENV_PATH)

BOT_TOKEN = os.getenv("BOT_TOKEN", "").strip()
ADMIN_IDS_RAW = os.getenv("ADMIN_IDS", "").strip()
CHANNEL_ID_RAW = os.getenv("CHANNEL_ID", "").strip()
ANTISPAM_SECONDS = int(os.getenv("ANTISPAM_SECONDS", "60") or 60)
MAX_ALBUM_SIZE = int(os.getenv("MAX_ALBUM_SIZE", "10") or 10)

ADMIN_IDS: set[int] = set()
for part in ADMIN_IDS_RAW.replace(" ", "").split(","):
    if part.isdigit():
        ADMIN_IDS.add(int(part))

CHANNEL_ID: Optional[int] = None
if CHANNEL_ID_RAW:
    try:
        CHANNEL_ID = int(CHANNEL_ID_RAW)
    except ValueError:
        CHANNEL_ID = None

# ---------------------------------------------------------------------------
# Логирование
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("podslushano")

# ---------------------------------------------------------------------------
# FSM States
# ---------------------------------------------------------------------------


class PostStates(StatesGroup):
    waiting_content = State()


class SupportStates(StatesGroup):
    waiting_message = State()


class AdminStates(StatesGroup):
    waiting_totp = State()
    waiting_setup_confirm = State()
    waiting_broadcast = State()
    waiting_ban_id = State()
    waiting_unban_id = State()
    waiting_reply = State()
    waiting_reject_reason = State()


# ---------------------------------------------------------------------------
# База данных
# ---------------------------------------------------------------------------


class Database:
    def __init__(self, path: Path = DB_PATH):
        self.path = path
        self._lock = asyncio.Lock()

    async def connect(self) -> aiosqlite.Connection:
        conn = await aiosqlite.connect(self.path)
        conn.row_factory = aiosqlite.Row
        await conn.execute("PRAGMA journal_mode=WAL;")
        await conn.execute("PRAGMA foreign_keys=ON;")
        return conn

    @asynccontextmanager
    async def session(self):
        async with self._lock:
            conn = await self.connect()
            try:
                yield conn
                await conn.commit()
            except Exception:
                await conn.rollback()
                raise
            finally:
                await conn.close()

    async def init(self) -> None:
        async with self.session() as conn:
            await conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS users (
                    user_id     INTEGER PRIMARY KEY,
                    username    TEXT,
                    full_name   TEXT,
                    is_banned   INTEGER DEFAULT 0,
                    is_admin    INTEGER DEFAULT 0,
                    posts_count INTEGER DEFAULT 0,
                    last_post_at TEXT,
                    created_at  TEXT NOT NULL,
                    updated_at  TEXT
                );

                CREATE TABLE IF NOT EXISTS posts (
                    id              INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id         INTEGER NOT NULL,
                    content_type    TEXT NOT NULL,  -- text, photo, video, album
                    text            TEXT,
                    media_json      TEXT,           -- JSON list of {type, file_id}
                    status          TEXT DEFAULT 'pending',  -- pending, approved, rejected
                    reject_reason   TEXT,
                    channel_msg_ids TEXT,           -- JSON list of message_ids in channel
                    moderated_by    INTEGER,
                    moderated_at    TEXT,
                    created_at      TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS support_messages (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id     INTEGER NOT NULL,
                    message     TEXT NOT NULL,
                    is_from_admin INTEGER DEFAULT 0,
                    admin_id    INTEGER,
                    is_read     INTEGER DEFAULT 0,
                    created_at  TEXT NOT NULL,
                    FOREIGN KEY (user_id) REFERENCES users(user_id)
                );

                CREATE TABLE IF NOT EXISTS admin_logs (
                    id          INTEGER PRIMARY KEY AUTOINCREMENT,
                    admin_id    INTEGER NOT NULL,
                    action      TEXT NOT NULL,
                    details     TEXT,
                    created_at  TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS settings (
                    key         TEXT PRIMARY KEY,
                    value       TEXT NOT NULL
                );

                CREATE INDEX IF NOT EXISTS idx_posts_status ON posts(status);
                CREATE INDEX IF NOT EXISTS idx_posts_user ON posts(user_id);
                CREATE INDEX IF NOT EXISTS idx_users_banned ON users(is_banned);
                CREATE INDEX IF NOT EXISTS idx_support_user ON support_messages(user_id);
                """
            )
        logger.info("База данных инициализирована: %s", self.path)

    async def get_setting(self, key: str, default: Optional[str] = None) -> Optional[str]:
        async with self.session() as conn:
            cur = await conn.execute("SELECT value FROM settings WHERE key = ?", (key,))
            row = await cur.fetchone()
            return row["value"] if row else default

    async def set_setting(self, key: str, value: str) -> None:
        async with self.session() as conn:
            await conn.execute(
                "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                (key, value),
            )

    async def ensure_user(self, user_id: int, username: Optional[str], full_name: str) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self.session() as conn:
            cur = await conn.execute("SELECT user_id FROM users WHERE user_id = ?", (user_id,))
            if await cur.fetchone():
                await conn.execute(
                    "UPDATE users SET username = ?, full_name = ?, updated_at = ? WHERE user_id = ?",
                    (username, full_name, now, user_id),
                )
            else:
                is_admin = 1 if user_id in ADMIN_IDS else 0
                await conn.execute(
                    """
                    INSERT INTO users (user_id, username, full_name, is_admin, created_at, updated_at)
                    VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (user_id, username, full_name, is_admin, now, now),
                )

    async def is_banned(self, user_id: int) -> bool:
        async with self.session() as conn:
            cur = await conn.execute("SELECT is_banned FROM users WHERE user_id = ?", (user_id,))
            row = await cur.fetchone()
            return bool(row and row["is_banned"])

    async def set_ban(self, user_id: int, banned: bool) -> None:
        async with self.session() as conn:
            await conn.execute(
                "UPDATE users SET is_banned = ?, updated_at = ? WHERE user_id = ?",
                (1 if banned else 0, datetime.now(timezone.utc).isoformat(), user_id),
            )

    async def can_post(self, user_id: int) -> tuple[bool, str]:
        if await self.is_banned(user_id):
            return False, "Вы заблокированы и не можете отправлять публикации."
        async with self.session() as conn:
            cur = await conn.execute(
                "SELECT last_post_at FROM users WHERE user_id = ?", (user_id,)
            )
            row = await cur.fetchone()
            if row and row["last_post_at"]:
                try:
                    last = datetime.fromisoformat(row["last_post_at"])
                    delta = datetime.now(timezone.utc) - last
                    if delta.total_seconds() < ANTISPAM_SECONDS:
                        wait = int(ANTISPAM_SECONDS - delta.total_seconds()) + 1
                        return False, f"Антиспам: подождите ещё {wait} сек. перед следующей публикацией."
                except Exception:
                    pass
        return True, ""

    async def update_last_post(self, user_id: int) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self.session() as conn:
            await conn.execute(
                "UPDATE users SET last_post_at = ?, posts_count = posts_count + 1, updated_at = ? WHERE user_id = ?",
                (now, now, user_id),
            )

    async def create_post(
        self,
        user_id: int,
        content_type: str,
        text: Optional[str],
        media: Optional[List[Dict[str, str]]],
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        media_json = json.dumps(media or [], ensure_ascii=False)
        async with self.session() as conn:
            cur = await conn.execute(
                """
                INSERT INTO posts (user_id, content_type, text, media_json, status, created_at)
                VALUES (?, ?, ?, ?, 'pending', ?)
                """,
                (user_id, content_type, text, media_json, now),
            )
            return cur.lastrowid

    async def get_post(self, post_id: int) -> Optional[Dict[str, Any]]:
        async with self.session() as conn:
            cur = await conn.execute("SELECT * FROM posts WHERE id = ?", (post_id,))
            row = await cur.fetchone()
            return dict(row) if row else None

    async def get_pending_posts(self, limit: int = 20) -> List[Dict[str, Any]]:
        async with self.session() as conn:
            cur = await conn.execute(
                "SELECT * FROM posts WHERE status = 'pending' ORDER BY created_at ASC LIMIT ?",
                (limit,),
            )
            rows = await cur.fetchall()
            return [dict(r) for r in rows]

    async def set_post_status(
        self,
        post_id: int,
        status: str,
        admin_id: Optional[int] = None,
        reason: Optional[str] = None,
        channel_msg_ids: Optional[List[int]] = None,
    ) -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self.session() as conn:
            await conn.execute(
                """
                UPDATE posts
                SET status = ?, moderated_by = ?, moderated_at = ?,
                    reject_reason = ?, channel_msg_ids = ?
                WHERE id = ?
                """,
                (
                    status,
                    admin_id,
                    now,
                    reason,
                    json.dumps(channel_msg_ids or []),
                    post_id,
                ),
            )

    async def add_support_message(
        self, user_id: int, message: str, is_from_admin: bool = False, admin_id: Optional[int] = None
    ) -> int:
        now = datetime.now(timezone.utc).isoformat()
        async with self.session() as conn:
            cur = await conn.execute(
                """
                INSERT INTO support_messages (user_id, message, is_from_admin, admin_id, created_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (user_id, message, 1 if is_from_admin else 0, admin_id, now),
            )
            return cur.lastrowid

    async def get_unread_support(self, limit: int = 30) -> List[Dict[str, Any]]:
        async with self.session() as conn:
            cur = await conn.execute(
                """
                SELECT sm.*, u.username, u.full_name
                FROM support_messages sm
                JOIN users u ON u.user_id = sm.user_id
                WHERE sm.is_from_admin = 0 AND sm.is_read = 0
                ORDER BY sm.created_at ASC
                LIMIT ?
                """,
                (limit,),
            )
            return [dict(r) for r in await cur.fetchall()]

    async def mark_support_read(self, msg_id: int) -> None:
        async with self.session() as conn:
            await conn.execute(
                "UPDATE support_messages SET is_read = 1 WHERE id = ?", (msg_id,)
            )

    async def log_admin(self, admin_id: int, action: str, details: str = "") -> None:
        now = datetime.now(timezone.utc).isoformat()
        async with self.session() as conn:
            await conn.execute(
                "INSERT INTO admin_logs (admin_id, action, details, created_at) VALUES (?, ?, ?, ?)",
                (admin_id, action, details, now),
            )

    async def get_admin_logs(self, limit: int = 30) -> List[Dict[str, Any]]:
        async with self.session() as conn:
            cur = await conn.execute(
                "SELECT * FROM admin_logs ORDER BY created_at DESC LIMIT ?", (limit,)
            )
            return [dict(r) for r in await cur.fetchall()]

    async def get_stats(self) -> Dict[str, Any]:
        async with self.session() as conn:
            users = (await (await conn.execute("SELECT COUNT(*) AS c FROM users")).fetchone())["c"]
            banned = (
                await (await conn.execute("SELECT COUNT(*) AS c FROM users WHERE is_banned = 1")).fetchone()
            )["c"]
            pending = (
                await (await conn.execute("SELECT COUNT(*) AS c FROM posts WHERE status = 'pending'")).fetchone()
            )["c"]
            approved = (
                await (await conn.execute("SELECT COUNT(*) AS c FROM posts WHERE status = 'approved'")).fetchone()
            )["c"]
            rejected = (
                await (await conn.execute("SELECT COUNT(*) AS c FROM posts WHERE status = 'rejected'")).fetchone()
            )["c"]
            support = (
                await (
                    await conn.execute(
                        "SELECT COUNT(*) AS c FROM support_messages WHERE is_from_admin = 0 AND is_read = 0"
                    )
                ).fetchone()
            )["c"]
            return {
                "users": users,
                "banned": banned,
                "pending": pending,
                "approved": approved,
                "rejected": rejected,
                "support_unread": support,
            }

    async def get_all_user_ids(self) -> List[int]:
        async with self.session() as conn:
            cur = await conn.execute("SELECT user_id FROM users WHERE is_banned = 0")
            return [r["user_id"] for r in await cur.fetchall()]


db = Database()

# ---------------------------------------------------------------------------
# Вспомогательные функции
# ---------------------------------------------------------------------------


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def is_admin(user_id: int) -> bool:
    return user_id in ADMIN_IDS


def main_menu_kb() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="📝 Отправить публикацию"))
    builder.row(KeyboardButton(text="💬 Написать в поддержку"))
    builder.row(KeyboardButton(text="ℹ️ Помощь"))
    return builder.as_markup(resize_keyboard=True)


def admin_panel_kb() -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="📋 Ожидающие посты", callback_data="admin:pending"),
        InlineKeyboardButton(text="📊 Статистика", callback_data="admin:stats"),
    )
    builder.row(
        InlineKeyboardButton(text="💬 Поддержка", callback_data="admin:support"),
        InlineKeyboardButton(text="📢 Рассылка", callback_data="admin:broadcast"),
    )
    builder.row(
        InlineKeyboardButton(text="🚫 Заблокировать", callback_data="admin:ban"),
        InlineKeyboardButton(text="✅ Разблокировать", callback_data="admin:unban"),
    )
    builder.row(
        InlineKeyboardButton(text="📜 Журнал", callback_data="admin:logs"),
        InlineKeyboardButton(text="💾 Бэкап БД", callback_data="admin:backup"),
    )
    builder.row(InlineKeyboardButton(text="🔄 Проверить права канала", callback_data="admin:check_channel"))
    return builder.as_markup()


def post_moderation_kb(post_id: int) -> InlineKeyboardMarkup:
    builder = InlineKeyboardBuilder()
    builder.row(
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"mod:approve:{post_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"mod:reject:{post_id}"),
    )
    return builder.as_markup()


def cancel_kb() -> ReplyKeyboardMarkup:
    builder = ReplyKeyboardBuilder()
    builder.row(KeyboardButton(text="❌ Отмена"))
    return builder.as_markup(resize_keyboard=True)


async def safe_send(bot: Bot, chat_id: int, text: str, **kwargs) -> Optional[Message]:
    try:
        return await bot.send_message(chat_id, text, **kwargs)
    except TelegramForbiddenError:
        logger.warning("Пользователь %s заблокировал бота", chat_id)
    except TelegramRetryAfter as e:
        logger.warning("Flood control, sleep %s", e.retry_after)
        await asyncio.sleep(e.retry_after)
        try:
            return await bot.send_message(chat_id, text, **kwargs)
        except Exception as ex:
            logger.error("Retry failed: %s", ex)
    except TelegramNetworkError as e:
        logger.error("Network error: %s", e)
    except TelegramAPIError as e:
        logger.error("API error: %s", e)
    return None


async def publish_to_channel(
    bot: Bot, post: Dict[str, Any]
) -> List[int]:
    """Публикует пост в канал и возвращает список message_id."""
    if not CHANNEL_ID:
        raise RuntimeError("CHANNEL_ID не задан")

    text = post.get("text") or ""
    media_raw = post.get("media_json") or "[]"
    try:
        media_list: List[Dict[str, str]] = json.loads(media_raw)
    except Exception:
        media_list = []

    content_type = post.get("content_type", "text")
    msg_ids: List[int] = []

    if content_type == "text" or not media_list:
        msg = await bot.send_message(
            CHANNEL_ID,
            text or "—",
            parse_mode=ParseMode.HTML,
            disable_web_page_preview=True,
        )
        msg_ids.append(msg.message_id)
        return msg_ids

    if content_type in ("photo", "video") and len(media_list) == 1:
        item = media_list[0]
        if item["type"] == "photo":
            msg = await bot.send_photo(
                CHANNEL_ID,
                item["file_id"],
                caption=text or None,
                parse_mode=ParseMode.HTML if text else None,
            )
        else:
            msg = await bot.send_video(
                CHANNEL_ID,
                item["file_id"],
                caption=text or None,
                parse_mode=ParseMode.HTML if text else None,
            )
        msg_ids.append(msg.message_id)
        return msg_ids

    # Альбом
    media_group = []
    for i, item in enumerate(media_list[:MAX_ALBUM_SIZE]):
        caption = text if i == 0 and text else None
        if item["type"] == "photo":
            media_group.append(
                InputMediaPhoto(
                    media=item["file_id"],
                    caption=caption,
                    parse_mode=ParseMode.HTML if caption else None,
                )
            )
        elif item["type"] == "video":
            media_group.append(
                InputMediaVideo(
                    media=item["file_id"],
                    caption=caption,
                    parse_mode=ParseMode.HTML if caption else None,
                )
            )
    if media_group:
        messages = await bot.send_media_group(CHANNEL_ID, media=media_group)
        msg_ids = [m.message_id for m in messages]
    return msg_ids


async def backup_db() -> Path:
    BACKUPS_DIR.mkdir(exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    dest = BACKUPS_DIR / f"bot_{stamp}.db"
    shutil.copy2(DB_PATH, dest)
    # Чистим старые (оставляем последние 20)
    backups = sorted(BACKUPS_DIR.glob("bot_*.db"), key=lambda p: p.stat().st_mtime, reverse=True)
    for old in backups[20:]:
        try:
            old.unlink()
        except Exception:
            pass
    return dest


async def check_bot_rights(bot: Bot) -> str:
    if not CHANNEL_ID:
        return "CHANNEL_ID не задан в .env"
    try:
        member = await bot.get_chat_member(CHANNEL_ID, bot.id)
        if member.status not in (ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR):
            return "Бот не является администратором канала"
        # Проверяем право на публикацию
        if isinstance(member, type(member)) and hasattr(member, "can_post_messages"):
            if not getattr(member, "can_post_messages", True):
                return "У бота нет права «Публикация сообщений»"
        chat = await bot.get_chat(CHANNEL_ID)
        return f"OK — канал «{chat.title}», права в порядке"
    except TelegramBadRequest as e:
        return f"Ошибка доступа к каналу: {e}"
    except Exception as e:
        return f"Не удалось проверить: {e}"


# ---------------------------------------------------------------------------
# Middleware: антиспам + бан + логирование
# ---------------------------------------------------------------------------


class AccessMiddleware:
    def __init__(self, db: Database):
        self.db = db

    async def __call__(self, handler, event, data):
        user = None
        if isinstance(event, Message) and event.from_user:
            user = event.from_user
        elif isinstance(event, CallbackQuery) and event.from_user:
            user = event.from_user

        if user:
            await self.db.ensure_user(
                user.id,
                user.username,
                user.full_name or str(user.id),
            )
            if await self.db.is_banned(user.id) and not is_admin(user.id):
                if isinstance(event, Message):
                    await event.answer("🚫 Вы заблокированы.")
                elif isinstance(event, CallbackQuery):
                    await event.answer("Вы заблокированы.", show_alert=True)
                return
        return await handler(event, data)


# ---------------------------------------------------------------------------
# Сборщик альбомов (простой in-memory)
# ---------------------------------------------------------------------------

_album_cache: Dict[str, List[Message]] = {}
_album_lock = asyncio.Lock()


async def collect_album(message: Message, delay: float = 0.8) -> List[Message]:
    """Собирает медиагруппу. Возвращает полный список сообщений альбома."""
    if not message.media_group_id:
        return [message]

    key = f"{message.chat.id}:{message.media_group_id}"
    async with _album_lock:
        if key not in _album_cache:
            _album_cache[key] = []
        _album_cache[key].append(message)

    await asyncio.sleep(delay)

    async with _album_lock:
        album = _album_cache.pop(key, [message])
    # Сортируем по message_id
    album.sort(key=lambda m: m.message_id)
    return album


# ---------------------------------------------------------------------------
# Роутеры
# ---------------------------------------------------------------------------

user_router = Router(name="user")
admin_router = Router(name="admin")


# ======================== USER HANDLERS ========================


@user_router.message(Command("start"))
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    text = (
        "👋 Добро пожаловать в <b>Подслушано</b>!\n\n"
        "Здесь вы можете анонимно отправить историю, фото или видео.\n"
        "Все публикации проходят премодерацию.\n\n"
        "Выберите действие в меню ниже."
    )
    await message.answer(text, reply_markup=main_menu_kb(), parse_mode=ParseMode.HTML)


@user_router.message(F.text == "ℹ️ Помощь")
@user_router.message(Command("help"))
async def cmd_help(message: Message):
    text = (
        "<b>Как пользоваться:</b>\n\n"
        "1. Нажмите «📝 Отправить публикацию»\n"
        "2. Пришлите текст, фото, видео или альбом (до 10 медиа)\n"
        "3. Дождитесь модерации — после одобрения пост появится в канале\n\n"
        "• Антиспам: не чаще одного поста в минуту (настраивается)\n"
        "• Поддержка: кнопка «💬 Написать в поддержку»\n\n"
        "Администраторам: /apanel"
    )
    await message.answer(text, parse_mode=ParseMode.HTML, reply_markup=main_menu_kb())


@user_router.message(F.text == "📝 Отправить публикацию")
async def start_post(message: Message, state: FSMContext):
    ok, reason = await db.can_post(message.from_user.id)
    if not ok:
        await message.answer(reason, reply_markup=main_menu_kb())
        return
    await state.set_state(PostStates.waiting_content)
    await message.answer(
        "Отправьте текст, фото, видео или альбом.\n"
        "Можно с подписью. Для отмены — «❌ Отмена».",
        reply_markup=cancel_kb(),
    )


@user_router.message(F.text == "❌ Отмена")
async def cancel_any(message: Message, state: FSMContext):
    await state.clear()
    await message.answer("Отменено.", reply_markup=main_menu_kb())


@user_router.message(PostStates.waiting_content, F.media_group_id)
async def handle_album_post(message: Message, state: FSMContext, bot: Bot):
    album = await collect_album(message)
    if not album:
        return
    # Берём только первое сообщение для обработки (остальные уже в album)
    if message.message_id != album[0].message_id:
        return  # пропускаем дубли

    ok, reason = await db.can_post(message.from_user.id)
    if not ok:
        await state.clear()
        await message.answer(reason, reply_markup=main_menu_kb())
        return

    media: List[Dict[str, str]] = []
    caption = None
    for m in album:
        if m.caption and not caption:
            caption = m.caption
        if m.photo:
            media.append({"type": "photo", "file_id": m.photo[-1].file_id})
        elif m.video:
            media.append({"type": "video", "file_id": m.video.file_id})
        # другие типы игнорируем для простоты

    if not media:
        await message.answer("Не удалось распознать медиа в альбоме.", reply_markup=cancel_kb())
        return

    post_id = await db.create_post(
        message.from_user.id,
        "album",
        caption,
        media,
    )
    await db.update_last_post(message.from_user.id)
    await state.clear()

    await message.answer(
        f"✅ Альбом принят на модерацию (#{post_id}).\nОжидайте решения администратора.",
        reply_markup=main_menu_kb(),
    )
    # Уведомляем админов
    await notify_admins_new_post(bot, post_id)


@user_router.message(PostStates.waiting_content, F.photo)
async def handle_photo_post(message: Message, state: FSMContext, bot: Bot):
    if message.media_group_id:
        return  # обработает album handler
    ok, reason = await db.can_post(message.from_user.id)
    if not ok:
        await state.clear()
        await message.answer(reason, reply_markup=main_menu_kb())
        return

    media = [{"type": "photo", "file_id": message.photo[-1].file_id}]
    post_id = await db.create_post(
        message.from_user.id,
        "photo",
        message.caption,
        media,
    )
    await db.update_last_post(message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Фото принято на модерацию (#{post_id}).",
        reply_markup=main_menu_kb(),
    )
    await notify_admins_new_post(bot, post_id)


@user_router.message(PostStates.waiting_content, F.video)
async def handle_video_post(message: Message, state: FSMContext, bot: Bot):
    if message.media_group_id:
        return
    ok, reason = await db.can_post(message.from_user.id)
    if not ok:
        await state.clear()
        await message.answer(reason, reply_markup=main_menu_kb())
        return

    media = [{"type": "video", "file_id": message.video.file_id}]
    post_id = await db.create_post(
        message.from_user.id,
        "video",
        message.caption,
        media,
    )
    await db.update_last_post(message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Видео принято на модерацию (#{post_id}).",
        reply_markup=main_menu_kb(),
    )
    await notify_admins_new_post(bot, post_id)


@user_router.message(PostStates.waiting_content, F.text)
async def handle_text_post(message: Message, state: FSMContext, bot: Bot):
    if message.text == "❌ Отмена":
        return  # уже обработано
    text = (message.text or "").strip()
    if len(text) < 5:
        await message.answer("Слишком короткий текст. Минимум 5 символов.", reply_markup=cancel_kb())
        return
    if len(text) > 4000:
        await message.answer("Слишком длинный текст (макс. 4000 символов).", reply_markup=cancel_kb())
        return

    ok, reason = await db.can_post(message.from_user.id)
    if not ok:
        await state.clear()
        await message.answer(reason, reply_markup=main_menu_kb())
        return

    post_id = await db.create_post(message.from_user.id, "text", text, None)
    await db.update_last_post(message.from_user.id)
    await state.clear()
    await message.answer(
        f"✅ Текст принят на модерацию (#{post_id}).",
        reply_markup=main_menu_kb(),
    )
    await notify_admins_new_post(bot, post_id)


@user_router.message(PostStates.waiting_content)
async def handle_unsupported_post(message: Message, state: FSMContext):
    await message.answer(
        "Поддерживаются только текст, фото, видео и альбомы.\nПопробуйте ещё раз или «❌ Отмена».",
        reply_markup=cancel_kb(),
    )


@user_router.message(F.text == "💬 Написать в поддержку")
async def start_support(message: Message, state: FSMContext):
    await state.set_state(SupportStates.waiting_message)
    await message.answer(
        "Напишите ваше сообщение поддержке.\nДля отмены — «❌ Отмена».",
        reply_markup=cancel_kb(),
    )


@user_router.message(SupportStates.waiting_message, F.text)
async def handle_support_message(message: Message, state: FSMContext, bot: Bot):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено.", reply_markup=main_menu_kb())
        return
    text = (message.text or "").strip()
    if len(text) < 3:
        await message.answer("Сообщение слишком короткое.")
        return
    await db.add_support_message(message.from_user.id, text)
    await state.clear()
    await message.answer(
        "✅ Сообщение отправлено в поддержку. Мы ответим вам в личные сообщения.",
        reply_markup=main_menu_kb(),
    )
    # Уведомляем админов
    for admin_id in ADMIN_IDS:
        await safe_send(
            bot,
            admin_id,
            f"💬 Новое сообщение в поддержку от {message.from_user.full_name} "
            f"(@{message.from_user.username or '—'}, id={message.from_user.id}):\n\n{text}",
        )


# ======================== ADMIN HANDLERS ========================


async def notify_admins_new_post(bot: Bot, post_id: int):
    post = await db.get_post(post_id)
    if not post:
        return
    preview = (post.get("text") or "")[:200]
    media_info = ""
    try:
        media = json.loads(post.get("media_json") or "[]")
        if media:
            media_info = f"\nМедиа: {len(media)} файл(ов) ({post['content_type']})"
    except Exception:
        pass
    text = (
        f"🆕 Новый пост #{post_id} на модерации\n"
        f"От: id={post['user_id']}\n"
        f"Тип: {post['content_type']}{media_info}\n"
        f"Текст: {preview or '—'}"
    )
    for admin_id in ADMIN_IDS:
        await safe_send(bot, admin_id, text, reply_markup=post_moderation_kb(post_id))


@admin_router.message(Command("apanel"))
async def cmd_apanel(message: Message, state: FSMContext):
    if not is_admin(message.from_user.id):
        await message.answer("Доступ запрещён.")
        return

    secret = await db.get_setting("totp_secret")
    if not secret:
        # Первоначальная настройка 2FA
        secret = pyotp.random_base32()
        await db.set_setting("totp_secret_pending", secret)
        totp = pyotp.TOTP(secret)
        uri = totp.provisioning_uri(
            name=f"admin_{message.from_user.id}",
            issuer_name="PodslushanoBot",
        )
        # Генерируем QR
        qr = qrcode.QRCode(version=1, box_size=6, border=2)
        qr.add_data(uri)
        qr.make(fit=True)
        img = qr.make_image(fill_color="black", back_color="white")
        from io import BytesIO
        buf = BytesIO()
        img.save(buf, format="PNG")
        buf.seek(0)

        await message.answer_photo(
            BufferedInputFile(buf.read(), filename="totp_qr.png"),
            caption=(
                "🔐 <b>Первоначальная настройка Google Authenticator</b>\n\n"
                "1. Отсканируйте QR-код в приложении Google Authenticator / Authy / и т.п.\n"
                "2. Или введите секрет вручную:\n"
                f"<code>{secret}</code>\n\n"
                "После добавления введите текущий 6-значный код из приложения."
            ),
            parse_mode=ParseMode.HTML,
        )
        await state.set_state(AdminStates.waiting_setup_confirm)
        await state.update_data(pending_secret=secret)
        return

    # Уже настроено — требуем код
    await state.set_state(AdminStates.waiting_totp)
    await message.answer(
        "🔐 Введите 6-значный код из Google Authenticator:",
        reply_markup=cancel_kb(),
    )


@admin_router.message(AdminStates.waiting_setup_confirm, F.text)
async def setup_totp_confirm(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Настройка отменена.", reply_markup=main_menu_kb())
        return
    data = await state.get_data()
    secret = data.get("pending_secret")
    if not secret:
        await state.clear()
        await message.answer("Сессия истекла. Начните заново /apanel")
        return
    code = (message.text or "").strip().replace(" ", "")
    totp = pyotp.TOTP(secret)
    if not totp.verify(code, valid_window=1):
        await message.answer("Неверный код. Попробуйте ещё раз.")
        return
    await db.set_setting("totp_secret", secret)
    await db.set_setting("totp_secret_pending", "")
    await db.log_admin(message.from_user.id, "totp_setup", "2FA успешно настроена")
    await state.clear()
    await message.answer(
        "✅ 2FA успешно настроена!\n\nАдмин-панель:",
        reply_markup=admin_panel_kb(),
    )


@admin_router.message(AdminStates.waiting_totp, F.text)
async def verify_totp(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено.", reply_markup=main_menu_kb())
        return
    secret = await db.get_setting("totp_secret")
    if not secret:
        await state.clear()
        await message.answer("2FA не настроена. Используйте /apanel")
        return
    code = (message.text or "").strip().replace(" ", "")
    totp = pyotp.TOTP(secret)
    if not totp.verify(code, valid_window=1):
        await message.answer("Неверный код. Попробуйте ещё раз или /apanel")
        return
    await state.clear()
    await db.log_admin(message.from_user.id, "login", "Успешный вход в панель")
    await message.answer(
        "✅ Доступ разрешён.\n\nАдмин-панель:",
        reply_markup=admin_panel_kb(),
    )


@admin_router.callback_query(F.data == "admin:pending")
async def admin_pending(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    posts = await db.get_pending_posts(15)
    if not posts:
        await callback.message.answer("Нет ожидающих постов.")
        await callback.answer()
        return
    await callback.answer()
    for post in posts:
        await send_post_preview(bot, callback.message.chat.id, post)


async def send_post_preview(bot: Bot, chat_id: int, post: Dict[str, Any]):
    post_id = post["id"]
    text = post.get("text") or ""
    media_raw = post.get("media_json") or "[]"
    try:
        media = json.loads(media_raw)
    except Exception:
        media = []
    header = (
        f"📋 Пост <b>#{post_id}</b>\n"
        f"От: <code>{post['user_id']}</code>\n"
        f"Тип: {post['content_type']}\n"
        f"Статус: {post['status']}\n"
        f"Создан: {post['created_at'][:19]}\n\n"
    )
    if not media:
        await bot.send_message(
            chat_id,
            header + (text or "—"),
            parse_mode=ParseMode.HTML,
            reply_markup=post_moderation_kb(post_id),
        )
        return

    # Отправляем медиа + caption
    if len(media) == 1:
        item = media[0]
        caption = header + (text or "")
        if len(caption) > 1024:
            caption = caption[:1020] + "…"
        if item["type"] == "photo":
            await bot.send_photo(
                chat_id,
                item["file_id"],
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=post_moderation_kb(post_id),
            )
        else:
            await bot.send_video(
                chat_id,
                item["file_id"],
                caption=caption,
                parse_mode=ParseMode.HTML,
                reply_markup=post_moderation_kb(post_id),
            )
    else:
        # Альбом — сначала медиа, потом кнопки
        media_group = []
        for i, item in enumerate(media[:10]):
            cap = (header + (text or "")) if i == 0 else None
            if cap and len(cap) > 1024:
                cap = cap[:1020] + "…"
            if item["type"] == "photo":
                media_group.append(InputMediaPhoto(media=item["file_id"], caption=cap, parse_mode=ParseMode.HTML if cap else None))
            else:
                media_group.append(InputMediaVideo(media=item["file_id"], caption=cap, parse_mode=ParseMode.HTML if cap else None))
        await bot.send_media_group(chat_id, media=media_group)
        await bot.send_message(
            chat_id,
            f"Кнопки для поста #{post_id}:",
            reply_markup=post_moderation_kb(post_id),
        )


@admin_router.callback_query(F.data.startswith("mod:approve:"))
async def mod_approve(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    post_id = int(callback.data.split(":")[-1])
    post = await db.get_post(post_id)
    if not post:
        await callback.answer("Пост не найден", show_alert=True)
        return
    if post["status"] != "pending":
        await callback.answer(f"Уже обработан: {post['status']}", show_alert=True)
        return

    try:
        msg_ids = await publish_to_channel(bot, post)
    except Exception as e:
        logger.exception("Ошибка публикации")
        await callback.answer(f"Ошибка публикации: {e}", show_alert=True)
        return

    await db.set_post_status(
        post_id, "approved", admin_id=callback.from_user.id, channel_msg_ids=msg_ids
    )
    await db.log_admin(
        callback.from_user.id,
        "approve",
        f"post_id={post_id}, channel_msgs={msg_ids}",
    )
    await callback.answer("Одобрено и опубликовано ✅")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass
    # Уведомляем автора
    await safe_send(
        bot,
        post["user_id"],
        f"✅ Ваша публикация #{post_id} одобрена и опубликована в канале!",
    )


@admin_router.callback_query(F.data.startswith("mod:reject:"))
async def mod_reject_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    post_id = int(callback.data.split(":")[-1])
    post = await db.get_post(post_id)
    if not post or post["status"] != "pending":
        await callback.answer("Пост уже обработан или не найден", show_alert=True)
        return
    await state.set_state(AdminStates.waiting_reject_reason)
    await state.update_data(reject_post_id=post_id)
    await callback.message.answer(
        f"Укажите причину отклонения поста #{post_id} (или «-» без причины):",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_reject_reason, F.text)
async def mod_reject_finish(message: Message, state: FSMContext, bot: Bot):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено.", reply_markup=main_menu_kb())
        return
    data = await state.get_data()
    post_id = data.get("reject_post_id")
    if not post_id:
        await state.clear()
        return
    reason = (message.text or "").strip()
    if reason == "-":
        reason = None
    post = await db.get_post(post_id)
    if not post or post["status"] != "pending":
        await state.clear()
        await message.answer("Пост уже обработан.")
        return
    await db.set_post_status(
        post_id, "rejected", admin_id=message.from_user.id, reason=reason
    )
    await db.log_admin(
        message.from_user.id, "reject", f"post_id={post_id}, reason={reason}"
    )
    await state.clear()
    await message.answer(f"Пост #{post_id} отклонён.", reply_markup=admin_panel_kb())
    notify = f"❌ Ваша публикация #{post_id} отклонена."
    if reason:
        notify += f"\nПричина: {reason}"
    await safe_send(bot, post["user_id"], notify)


@admin_router.callback_query(F.data == "admin:stats")
async def admin_stats(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    stats = await db.get_stats()
    text = (
        "📊 <b>Статистика</b>\n\n"
        f"👥 Пользователей: {stats['users']}\n"
        f"🚫 Заблокировано: {stats['banned']}\n"
        f"⏳ Ожидают модерации: {stats['pending']}\n"
        f"✅ Одобрено: {stats['approved']}\n"
        f"❌ Отклонено: {stats['rejected']}\n"
        f"💬 Непрочитанных в поддержке: {stats['support_unread']}"
    )
    await callback.message.answer(text, parse_mode=ParseMode.HTML)
    await callback.answer()


@admin_router.callback_query(F.data == "admin:support")
async def admin_support(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        await callback.answer("Нет доступа", show_alert=True)
        return
    msgs = await db.get_unread_support(20)
    if not msgs:
        await callback.message.answer("Нет непрочитанных сообщений поддержки.")
        await callback.answer()
        return
    await callback.answer()
    for m in msgs:
        text = (
            f"💬 #{m['id']} от {m.get('full_name') or '—'} "
            f"(@{m.get('username') or '—'}, <code>{m['user_id']}</code>)\n"
            f"{m['created_at'][:19]}\n\n"
            f"{m['message']}"
        )
        kb = InlineKeyboardBuilder()
        kb.row(
            InlineKeyboardButton(
                text="✍️ Ответить",
                callback_data=f"support:reply:{m['user_id']}:{m['id']}",
            )
        )
        kb.row(
            InlineKeyboardButton(
                text="✓ Прочитано",
                callback_data=f"support:read:{m['id']}",
            )
        )
        await bot.send_message(
            callback.message.chat.id,
            text,
            parse_mode=ParseMode.HTML,
            reply_markup=kb.as_markup(),
        )


@admin_router.callback_query(F.data.startswith("support:read:"))
async def support_mark_read(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    msg_id = int(callback.data.split(":")[-1])
    await db.mark_support_read(msg_id)
    await callback.answer("Отмечено прочитанным")
    try:
        await callback.message.edit_reply_markup(reply_markup=None)
    except Exception:
        pass


@admin_router.callback_query(F.data.startswith("support:reply:"))
async def support_reply_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    parts = callback.data.split(":")
    user_id = int(parts[2])
    msg_id = int(parts[3])
    await state.set_state(AdminStates.waiting_reply)
    await state.update_data(reply_user_id=user_id, reply_msg_id=msg_id)
    await callback.message.answer(
        f"Введите ответ пользователю {user_id}:",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_reply, F.text)
async def support_reply_finish(message: Message, state: FSMContext, bot: Bot):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено.", reply_markup=admin_panel_kb())
        return
    data = await state.get_data()
    user_id = data.get("reply_user_id")
    msg_id = data.get("reply_msg_id")
    if not user_id:
        await state.clear()
        return
    text = message.text.strip()
    await db.add_support_message(user_id, text, is_from_admin=True, admin_id=message.from_user.id)
    if msg_id:
        await db.mark_support_read(msg_id)
    await db.log_admin(message.from_user.id, "support_reply", f"to={user_id}")
    await state.clear()
    sent = await safe_send(
        bot,
        user_id,
        f"💬 <b>Ответ поддержки:</b>\n\n{text}",
        parse_mode=ParseMode.HTML,
    )
    if sent:
        await message.answer("Ответ отправлен.", reply_markup=admin_panel_kb())
    else:
        await message.answer(
            "Не удалось доставить (пользователь мог заблокировать бота).",
            reply_markup=admin_panel_kb(),
        )


@admin_router.callback_query(F.data == "admin:broadcast")
async def admin_broadcast_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_broadcast)
    await callback.message.answer(
        "Отправьте сообщение для рассылки (текст / фото / видео).\n"
        "Или «❌ Отмена».",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_broadcast)
async def admin_broadcast_send(message: Message, state: FSMContext, bot: Bot):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Рассылка отменена.", reply_markup=admin_panel_kb())
        return
    await state.clear()
    users = await db.get_all_user_ids()
    if not users:
        await message.answer("Нет пользователей для рассылки.")
        return
    await message.answer(f"Начинаю рассылку на {len(users)} пользователей…")
    success = 0
    fail = 0
    for uid in users:
        try:
            if message.photo:
                await bot.send_photo(
                    uid,
                    message.photo[-1].file_id,
                    caption=message.caption,
                )
            elif message.video:
                await bot.send_video(
                    uid,
                    message.video.file_id,
                    caption=message.caption,
                )
            else:
                await bot.send_message(uid, message.text or message.caption or "—")
            success += 1
        except Exception:
            fail += 1
        await asyncio.sleep(0.05)  # лёгкий троттлинг
    await db.log_admin(
        message.from_user.id,
        "broadcast",
        f"success={success}, fail={fail}, total={len(users)}",
    )
    await message.answer(
        f"Рассылка завершена.\n✅ Успешно: {success}\n❌ Ошибок: {fail}",
        reply_markup=admin_panel_kb(),
    )


@admin_router.callback_query(F.data == "admin:ban")
async def admin_ban_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_ban_id)
    await callback.message.answer(
        "Введите Telegram ID пользователя для блокировки:",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_ban_id, F.text)
async def admin_ban_finish(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено.", reply_markup=admin_panel_kb())
        return
    try:
        uid = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой ID.")
        return
    if uid in ADMIN_IDS:
        await message.answer("Нельзя заблокировать администратора.")
        await state.clear()
        return
    await db.set_ban(uid, True)
    await db.log_admin(message.from_user.id, "ban", f"user_id={uid}")
    await state.clear()
    await message.answer(f"Пользователь {uid} заблокирован.", reply_markup=admin_panel_kb())


@admin_router.callback_query(F.data == "admin:unban")
async def admin_unban_start(callback: CallbackQuery, state: FSMContext):
    if not is_admin(callback.from_user.id):
        return
    await state.set_state(AdminStates.waiting_unban_id)
    await callback.message.answer(
        "Введите Telegram ID пользователя для разблокировки:",
        reply_markup=cancel_kb(),
    )
    await callback.answer()


@admin_router.message(AdminStates.waiting_unban_id, F.text)
async def admin_unban_finish(message: Message, state: FSMContext):
    if message.text == "❌ Отмена":
        await state.clear()
        await message.answer("Отменено.", reply_markup=admin_panel_kb())
        return
    try:
        uid = int(message.text.strip())
    except ValueError:
        await message.answer("Нужен числовой ID.")
        return
    await db.set_ban(uid, False)
    await db.log_admin(message.from_user.id, "unban", f"user_id={uid}")
    await state.clear()
    await message.answer(f"Пользователь {uid} разблокирован.", reply_markup=admin_panel_kb())


@admin_router.callback_query(F.data == "admin:logs")
async def admin_logs(callback: CallbackQuery):
    if not is_admin(callback.from_user.id):
        return
    logs = await db.get_admin_logs(25)
    if not logs:
        await callback.message.answer("Журнал пуст.")
        await callback.answer()
        return
    lines = []
    for log in logs:
        lines.append(
            f"{log['created_at'][:19]} | {log['admin_id']} | {log['action']} | {log.get('details') or ''}"
        )
    text = "📜 <b>Журнал действий</b>\n\n<pre>" + "\n".join(lines) + "</pre>"
    if len(text) > 4000:
        text = text[:3990] + "…</pre>"
    await callback.message.answer(text, parse_mode=ParseMode.HTML)
    await callback.answer()


@admin_router.callback_query(F.data == "admin:backup")
async def admin_backup(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        return
    try:
        path = await backup_db()
        await callback.message.answer_document(
            FSInputFile(path),
            caption=f"Бэкап создан: {path.name}",
        )
        await db.log_admin(callback.from_user.id, "backup", str(path))
    except Exception as e:
        await callback.message.answer(f"Ошибка бэкапа: {e}")
    await callback.answer()


@admin_router.callback_query(F.data == "admin:check_channel")
async def admin_check_channel(callback: CallbackQuery, bot: Bot):
    if not is_admin(callback.from_user.id):
        return
    result = await check_bot_rights(bot)
    await callback.message.answer(f"Проверка прав канала:\n{result}")
    await callback.answer()


# ---------------------------------------------------------------------------
# Главная функция
# ---------------------------------------------------------------------------


async def on_startup(bot: Bot):
    logger.info("Запуск бота…")
    await db.init()
    try:
        await backup_db()
        logger.info("Стартовый бэкап создан")
    except Exception as e:
        logger.warning("Не удалось сделать стартовый бэкап: %s", e)

    me = await bot.get_me()
    logger.info("Бот: @%s (id=%s)", me.username, me.id)

    rights = await check_bot_rights(bot)
    logger.info("Права в канале: %s", rights)

    if not ADMIN_IDS:
        logger.warning("ADMIN_IDS пуст! Админ-панель будет недоступна.")
    if not CHANNEL_ID:
        logger.warning("CHANNEL_ID не задан — публикация невозможна.")


async def on_shutdown(bot: Bot):
    logger.info("Остановка бота…")
    try:
        await backup_db()
    except Exception:
        pass


async def main():
    if not BOT_TOKEN:
        logger.error("BOT_TOKEN не задан в .env")
        sys.exit(1)

    bot = Bot(
        token=BOT_TOKEN,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML),
    )
    storage = MemoryStorage()
    dp = Dispatcher(storage=storage)

    # Middleware
    access_mw = AccessMiddleware(db)
    dp.message.middleware(access_mw)
    dp.callback_query.middleware(access_mw)

    dp.include_router(admin_router)
    dp.include_router(user_router)

    dp.startup.register(on_startup)
    dp.shutdown.register(on_shutdown)

    # Глобальный обработчик ошибок
    @dp.errors()
    async def errors_handler(event, exception):
        logger.error("Unhandled error: %s\n%s", exception, traceback.format_exc())
        return True

    logger.info("Polling started")
    await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("Остановлено пользователем")
    except Exception:
        logger.exception("Критическая ошибка")
        sys.exit(1)
