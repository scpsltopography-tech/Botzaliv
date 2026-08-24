"""
Бот для автопостинга контента в канал — всё в одном файле.

Нужно только:
    pip install -r requirements.txt
    заполнить .env (см. .env.example)
    python bot.py

Что делает — см. README.md (логика та же, просто весь код в одном месте,
без импортов между файлами, чтобы было проще деплоить/редактировать).
"""

import asyncio
import json
import logging
import os
from typing import Any, Dict, List, Optional

import asyncpg
from dotenv import load_dotenv
from aiohttp import web, ClientSession, ClientTimeout
from aiogram import BaseMiddleware, Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode, ChatAction, ContentType
from aiogram.filters import Command, CommandStart, StateFilter
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.base import BaseStorage, StorageKey, StateType
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.exceptions import TelegramBadRequest

# =============================================================================
# КОНФИГ (было config.py)
# =============================================================================
load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "")
ADMIN_IDS = {int(x) for x in os.getenv("ADMIN_IDS", "").split(",") if x.strip()}
OWNER_ID = int(os.getenv("OWNER_ID", "0"))
CHANNEL_ID = int(os.getenv("CHANNEL_ID", "0"))
DATABASE_URL = os.getenv("DATABASE_URL", "")
DISCLAIMER = "Канал ничего не одобряет и не пропагандирует"
FORWARD_DEBOUNCE_SECONDS = 1.5
BROADCAST_DELAY_SECONDS = 0.05

# ID, которых бот при первом запуске сам добавит в админы (через запятую).
# После первого запуска можно очистить — админы уже будут в БД.
BOOTSTRAP_ADMIN_IDS = {
    int(x) for x in os.getenv("BOOTSTRAP_ADMIN_IDS", "1964233800").split(",") if x.strip()
}

logging.basicConfig(level=logging.INFO)
log = logging.getLogger("media-bot")


# =============================================================================
# FSM-ХРАНИЛИЩЕ НА POSTGRES (было storage_pg.py)
# =============================================================================
def _key_str(key: StorageKey) -> str:
    # Специально БЕЗ user_id: состояние /batch общее на весь чат, чтобы
    # контент могли добавлять разные админы, а не только тот, кто запустил /batch.
    return f"{key.bot_id}:{key.chat_id}:{key.thread_id}:{key.destiny}"


class PostgresStorage(BaseStorage):
    """FSM-хранилище на Postgres, чтобы состояние /batch переживало рестарты бота."""

    def __init__(self, dsn: str):
        self._dsn = dsn
        self._pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        if self._pool is None:
            self._pool = await asyncpg.create_pool(self._dsn)
            async with self._pool.acquire() as conn:
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS fsm_storage (
                        key   TEXT PRIMARY KEY,
                        state TEXT,
                        data  JSONB NOT NULL DEFAULT '{}'::jsonb
                    )
                    """
                )

    async def set_state(self, key: StorageKey, state: StateType = None) -> None:
        await self.connect()
        state_str = state.state if isinstance(state, State) else state
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fsm_storage (key, state, data)
                VALUES ($1, $2, '{}'::jsonb)
                ON CONFLICT (key) DO UPDATE SET state = EXCLUDED.state
                """,
                _key_str(key), state_str,
            )

    async def get_state(self, key: StorageKey) -> Optional[str]:
        await self.connect()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT state FROM fsm_storage WHERE key = $1", _key_str(key))
            return row["state"] if row else None

    async def set_data(self, key: StorageKey, data: Dict[str, Any]) -> None:
        await self.connect()
        async with self._pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO fsm_storage (key, state, data)
                VALUES ($1, NULL, $2::jsonb)
                ON CONFLICT (key) DO UPDATE SET data = EXCLUDED.data
                """,
                _key_str(key), json.dumps(data),
            )

    async def get_data(self, key: StorageKey) -> Dict[str, Any]:
        await self.connect()
        async with self._pool.acquire() as conn:
            row = await conn.fetchrow("SELECT data FROM fsm_storage WHERE key = $1", _key_str(key))
            if not row or row["data"] is None:
                return {}
            raw = row["data"]
            return json.loads(raw) if isinstance(raw, str) else dict(raw)

    async def close(self) -> None:
        if self._pool:
            await self._pool.close()
            self._pool = None


# =============================================================================
# БАЗА ДАННЫХ: посты, пользователи, админы, баны, предложка (было posts_db.py)
# =============================================================================
_pool: Optional[asyncpg.Pool] = None


async def db_init(dsn: str):
    global _pool
    if _pool is not None:
        return
    _pool = await asyncpg.create_pool(dsn)
    async with _pool.acquire() as conn:
        await conn.execute("CREATE TABLE IF NOT EXISTS posts (id SERIAL PRIMARY KEY)")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS title TEXT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS hashtag TEXT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS author TEXT")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS items JSONB NOT NULL DEFAULT '[]'::jsonb")
        await conn.execute("ALTER TABLE posts ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ NOT NULL DEFAULT now()")

        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                first_seen TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS admins (
                user_id BIGINT PRIMARY KEY,
                username TEXT,
                added_by BIGINT,
                added_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS bans (
                user_id BIGINT PRIMARY KEY,
                reason TEXT,
                banned_by BIGINT,
                banned_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )
        await conn.execute(
            """
            CREATE TABLE IF NOT EXISTS submissions (
                id SERIAL PRIMARY KEY,
                user_id BIGINT NOT NULL,
                username TEXT,
                items JSONB NOT NULL DEFAULT '[]'::jsonb,
                status TEXT NOT NULL DEFAULT 'pending',
                reviewed_by BIGINT,
                created_at TIMESTAMPTZ NOT NULL DEFAULT now()
            )
            """
        )


async def db_close():
    global _pool
    if _pool:
        await _pool.close()
        _pool = None


# ---------- posts ----------
async def create_post(title: str, hashtag: str, author: str, items: List[Dict[str, Any]]) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO posts (title, hashtag, author, items) VALUES ($1, $2, $3, $4::jsonb) RETURNING id",
            title, hashtag, author, json.dumps(items),
        )
        return row["id"]


async def get_post(post_id: int) -> Optional[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT title, hashtag, author, items FROM posts WHERE id = $1", post_id)
        if not row:
            return None
        items = row["items"]
        items = json.loads(items) if isinstance(items, str) else items
        return {"title": row["title"], "hashtag": row["hashtag"], "author": row["author"], "items": items}


# ---------- users (для broadcast) ----------
async def touch_user(user_id: int, username: Optional[str]) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO users (user_id, username) VALUES ($1, $2)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
            """,
            user_id, username,
        )


async def get_all_user_ids() -> List[int]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id FROM users")
        return [r["user_id"] for r in rows]


# ---------- admins ----------
async def add_admin(user_id: int, username: Optional[str], added_by: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO admins (user_id, username, added_by) VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE SET username = EXCLUDED.username
            """,
            user_id, username, added_by,
        )


async def remove_admin(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        result = await conn.execute("DELETE FROM admins WHERE user_id = $1", user_id)
        return result.endswith("1")


async def list_admins() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, username, added_at FROM admins ORDER BY added_at")
        return [dict(r) for r in rows]


async def is_db_admin(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM admins WHERE user_id = $1", user_id)
        return row is not None


# ---------- баны ----------
async def ban_user(user_id: int, banned_by: int, reason: Optional[str]) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO bans (user_id, reason, banned_by) VALUES ($1, $2, $3)
            ON CONFLICT (user_id) DO UPDATE SET reason = EXCLUDED.reason, banned_by = EXCLUDED.banned_by, banned_at = now()
            """,
            user_id, reason, banned_by,
        )


async def unban_user(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        result = await conn.execute("DELETE FROM bans WHERE user_id = $1", user_id)
        return result.endswith("1")


async def is_banned(user_id: int) -> bool:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow("SELECT 1 FROM bans WHERE user_id = $1", user_id)
        return row is not None


async def list_bans() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch("SELECT user_id, reason, banned_at FROM bans ORDER BY banned_at DESC")
        return [dict(r) for r in rows]


# ---------- предложка ----------
async def create_submission(user_id: int, username: Optional[str], items: List[Dict[str, Any]]) -> int:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "INSERT INTO submissions (user_id, username, items) VALUES ($1, $2, $3::jsonb) RETURNING id",
            user_id, username, json.dumps(items),
        )
        return row["id"]


async def get_submission(sub_id: int) -> Optional[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        row = await conn.fetchrow(
            "SELECT id, user_id, username, items, status FROM submissions WHERE id = $1", sub_id
        )
        if not row:
            return None
        items = row["items"]
        items = json.loads(items) if isinstance(items, str) else items
        return {"id": row["id"], "user_id": row["user_id"], "username": row["username"],
                "items": items, "status": row["status"]}


async def set_submission_status(sub_id: int, status: str, reviewed_by: int) -> None:
    async with _pool.acquire() as conn:
        await conn.execute(
            "UPDATE submissions SET status = $1, reviewed_by = $2 WHERE id = $3",
            status, reviewed_by, sub_id,
        )


async def list_pending_submissions() -> List[Dict[str, Any]]:
    async with _pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, user_id, username, created_at FROM submissions WHERE status = 'pending' ORDER BY created_at"
        )
        return [dict(r) for r in rows]


# =============================================================================
# БОТ (было bot.py)
# =============================================================================
router = Router()
BOT_USERNAME = ""

ACCEPTED_TYPES = {
    ContentType.VIDEO, ContentType.PHOTO, ContentType.DOCUMENT, ContentType.AUDIO,
    ContentType.VOICE, ContentType.ANIMATION, ContentType.VIDEO_NOTE, ContentType.TEXT,
}


class Batch(StatesGroup):
    collecting = State()
    waiting_title = State()
    waiting_cover = State()
    waiting_hashtag = State()


class Submit(StatesGroup):
    collecting = State()


BATCH_STATES = {Batch.collecting.state, Batch.waiting_title.state, Batch.waiting_cover.state, Batch.waiting_hashtag.state}


# ---------- права доступа ----------
async def is_admin(user_id: int) -> bool:
    if user_id == OWNER_ID or user_id in ADMIN_IDS:
        return True
    return await is_db_admin(user_id)


async def is_owner(user_id: int) -> bool:
    return user_id == OWNER_ID


async def get_admin_ids() -> List[int]:
    ids = set(ADMIN_IDS)
    ids.add(OWNER_ID)
    for row in await list_admins():
        ids.add(row["user_id"])
    ids.discard(0)
    return list(ids)


# ---------- клавиатуры ----------
def stop_keyboard(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🛑 Это всё, завершить ({count} шт.)", callback_data="stop_batch")
    ]])


def submit_stop_keyboard(count: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=f"🛑 Отправить на проверку ({count} шт.)", callback_data="stop_submit")
    ]])


def submission_review_keyboard(sub_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text="✅ Одобрить", callback_data=f"sub_appr:{sub_id}"),
        InlineKeyboardButton(text="❌ Отклонить", callback_data=f"sub_rej:{sub_id}"),
    ]])


async def safe_delete(bot: Bot, chat_id: int, message_id: int):
    try:
        await bot.delete_message(chat_id, message_id)
    except TelegramBadRequest as e:
        log.warning("Не смог удалить сообщение %s: %s", message_id, e)


def item_from_message(message: Message) -> Optional[dict]:
    ctype = message.content_type
    msg_id = message.message_id
    if ctype == ContentType.TEXT:
        return {"type": "text", "text": message.text, "msg_id": msg_id}
    if ctype == ContentType.VIDEO:
        return {"type": "video", "file_id": message.video.file_id, "msg_id": msg_id}
    if ctype == ContentType.PHOTO:
        return {"type": "photo", "file_id": message.photo[-1].file_id, "msg_id": msg_id}
    if ctype == ContentType.DOCUMENT:
        return {"type": "document", "file_id": message.document.file_id, "msg_id": msg_id}
    if ctype == ContentType.AUDIO:
        return {"type": "audio", "file_id": message.audio.file_id, "msg_id": msg_id}
    if ctype == ContentType.VOICE:
        return {"type": "voice", "file_id": message.voice.file_id, "msg_id": msg_id}
    if ctype == ContentType.ANIMATION:
        return {"type": "animation", "file_id": message.animation.file_id, "msg_id": msg_id}
    if ctype == ContentType.VIDEO_NOTE:
        return {"type": "video_note", "file_id": message.video_note.file_id, "msg_id": msg_id}
    return None


async def send_item(bot: Bot, chat_id: int, item: dict):
    t = item["type"]
    if t == "text":
        await bot.send_message(chat_id, item["text"])
    elif t == "video":
        await bot.send_video(chat_id, item["file_id"])
    elif t == "photo":
        await bot.send_photo(chat_id, item["file_id"])
    elif t == "document":
        await bot.send_document(chat_id, item["file_id"])
    elif t == "audio":
        await bot.send_audio(chat_id, item["file_id"])
    elif t == "voice":
        await bot.send_voice(chat_id, item["file_id"])
    elif t == "animation":
        await bot.send_animation(chat_id, item["file_id"])
    elif t == "video_note":
        await bot.send_video_note(chat_id, item["file_id"])


# ---------- лок на чат, чтобы не терять файлы при параллельной обработке ----------
_batch_locks: Dict[int, asyncio.Lock] = {}


def get_batch_lock(chat_id: int) -> asyncio.Lock:
    lock = _batch_locks.get(chat_id)
    if lock is None:
        lock = asyncio.Lock()
        _batch_locks[chat_id] = lock
    return lock


# ---------- пачечная пересылка контента владельцу в ЛС (только /batch) ----------
_pending_forward: Dict[int, List[int]] = {}
_forward_tasks: Dict[int, asyncio.Task] = {}


def schedule_forward(bot: Bot, chat_id: int, message_id: int):
    if not OWNER_ID:
        return
    _pending_forward.setdefault(chat_id, []).append(message_id)
    old_task = _forward_tasks.get(chat_id)
    if old_task and not old_task.done():
        old_task.cancel()
    _forward_tasks[chat_id] = asyncio.create_task(_flush_forward(bot, chat_id))


async def _flush_forward(bot: Bot, chat_id: int):
    try:
        await asyncio.sleep(FORWARD_DEBOUNCE_SECONDS)
    except asyncio.CancelledError:
        return
    ids = sorted(set(_pending_forward.pop(chat_id, [])))
    if not ids:
        return
    try:
        await bot.forward_messages(chat_id=OWNER_ID, from_chat_id=chat_id, message_ids=ids)
    except TelegramBadRequest as e:
        log.warning("Не смог переслать пачку владельцу: %s", e)


# ---------- учёт пользователей (для /broadcast) ----------
class TouchUserMiddleware(BaseMiddleware):
    async def __call__(self, handler, event: Message, data: dict):
        if event.from_user and not event.from_user.is_bot:
            try:
                await touch_user(event.from_user.id, event.from_user.username)
            except Exception as e:
                log.warning("Не смог записать пользователя %s: %s", event.from_user.id, e)
        return await handler(event, data)


# ---------- /start и раздача контента ----------
@router.message(CommandStart())
async def cmd_start(message: Message):
    if await is_banned(message.from_user.id):
        return

    args = message.text.split(maxsplit=1)
    payload = args[1] if len(args) > 1 else None

    if not payload or not payload.startswith("p_"):
        await message.answer(
            "Привет! Открой ссылку из поста в канале, чтобы получить контент.\n"
            "Хочешь предложить свой контент для канала — напиши /submit."
        )
        return

    try:
        post_id = int(payload[2:])
    except ValueError:
        await message.answer("Некорректная ссылка.")
        return

    post = await get_post(post_id)
    if not post:
        await message.answer("Контент не найден (возможно, был удалён).")
        return

    await message.answer(f"📦 «{post['title']}»")
    for item in post["items"]:
        try:
            await send_item(message.bot, message.chat.id, item)
        except TelegramBadRequest as e:
            log.warning("Ошибка при отправке контента пользователю: %s", e)
            await message.answer("⚠️ Часть контента не удалось отправить.")


@router.message(Command("cancel"))
async def cmd_cancel(message: Message, state: FSMContext):
    current = await state.get_state()
    if current is None:
        return
    if current in BATCH_STATES and not await is_admin(message.from_user.id):
        return
    await state.clear()
    await message.answer("Отменено. Всё сброшено.")


# ============ /batch — приём контента админом, публикация в канал ============
@router.message(Command("batch"))
async def cmd_batch(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        await message.answer("Эта команда только для админов.")
        return
    await state.clear()
    await state.set_state(Batch.collecting)
    await state.update_data(media=[], status_msg_id=None, submission_id=None, author_name=None)
    await message.answer(
        "📥 Приём начат. Кидай любой контент — видео, фото, файлы, аудио, текст.\n"
        "Когда закончишь — жми кнопку под последним файлом, либо пришли /done."
    )


@router.message(Command("done"))
async def cmd_done(message: Message, state: FSMContext):
    current = await state.get_state()
    if current == Batch.collecting.state:
        if not await is_admin(message.from_user.id):
            return
        await finish_collecting(message.bot, message.chat.id, message.from_user, state)
    elif current == Submit.collecting.state:
        await finish_submission(message.bot, message.chat.id, message.from_user, state)


@router.message(StateFilter(Batch.collecting), F.content_type.in_(ACCEPTED_TYPES))
async def collect_media(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    if message.content_type == ContentType.TEXT and message.text.startswith("/"):
        return

    item = item_from_message(message)
    if item is None:
        return

    lock = get_batch_lock(message.chat.id)
    async with lock:
        data = await state.get_data()
        media = data.get("media", [])
        media.append(item)

        old_status_id = data.get("status_msg_id")
        if old_status_id:
            await safe_delete(message.bot, message.chat.id, old_status_id)

        status = await message.answer(
            f"Загружено файлов: {len(media)}. Это всё?",
            reply_markup=stop_keyboard(len(media)),
        )
        await state.update_data(media=media, status_msg_id=status.message_id)

    if message.from_user.id != OWNER_ID:
        schedule_forward(message.bot, message.chat.id, message.message_id)


@router.callback_query(F.data == "stop_batch", StateFilter(Batch.collecting))
async def cb_stop_batch(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer()
        return
    await callback.answer()
    await finish_collecting(callback.bot, callback.message.chat.id, callback.from_user, state)


async def finish_collecting(bot: Bot, chat_id: int, from_user, state: FSMContext):
    data = await state.get_data()
    media = data.get("media", [])

    if not media:
        await bot.send_message(chat_id, "Файлов не было, отмена.")
        await state.clear()
        return

    if data.get("status_msg_id"):
        await safe_delete(bot, chat_id, data["status_msg_id"])
    for item in media:
        await safe_delete(bot, chat_id, item["msg_id"])

    await state.update_data(admin_id=from_user.id, admin_name=from_user.full_name)
    await state.set_state(Batch.waiting_title)
    await bot.send_message(
        chat_id,
        f"✅ Принято файлов: {len(media)}.\nТеперь пришли *название* поста.",
        parse_mode=ParseMode.MARKDOWN,
    )


@router.message(StateFilter(Batch.waiting_title), F.text)
async def get_title(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(title=message.text.strip())
    await state.set_state(Batch.waiting_cover)
    await message.answer("🖼 Теперь пришли обложку (фото).")


@router.message(StateFilter(Batch.waiting_cover), F.photo)
async def get_cover(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    await state.update_data(cover_file_id=message.photo[-1].file_id)
    await state.set_state(Batch.waiting_hashtag)
    await message.answer("#️⃣ Теперь пришли хэштег (например: #видео).")


@router.message(StateFilter(Batch.waiting_hashtag), F.text)
async def get_hashtag(message: Message, state: FSMContext):
    if not await is_admin(message.from_user.id):
        return
    hashtag = message.text.strip()
    if not hashtag.startswith("#"):
        hashtag = "#" + hashtag
    await state.update_data(hashtag=hashtag)
    await publish(message, state)


async def publish(message: Message, state: FSMContext):
    bot = message.bot
    data = await state.get_data()
    media = data["media"]
    title = data["title"]
    cover_file_id = data["cover_file_id"]
    hashtag = data["hashtag"]
    admin_name = data.get("author_name") or data.get("admin_name", "admin")
    submission_id = data.get("submission_id")

    await bot.send_chat_action(message.chat.id, ChatAction.TYPING)

    post_id = await create_post(title=title, hashtag=hashtag, author=admin_name, items=media)
    deep_link = f"https://t.me/{BOT_USERNAME}?start=p_{post_id}"

    caption = (
        f"<b>{title}</b>\n\n"
        f'<a href="{deep_link}">видео здесь</a>\n\n'
        f"{DISCLAIMER}\n\n"
        f"{hashtag}\n\n"
        f"Автор: {admin_name}"
    )

    try:
        await bot.send_photo(CHANNEL_ID, cover_file_id, caption=caption, parse_mode=ParseMode.HTML)
    except TelegramBadRequest as e:
        await message.answer(f"❌ Не смог опубликовать пост в канал: {e}")
        await state.clear()
        return

    if submission_id:
        await set_submission_status(submission_id, "approved", message.from_user.id)
        sub = await get_submission(submission_id)
        if sub:
            try:
                await bot.send_message(sub["user_id"], "🎉 Ваш пост одобрен и опубликован в канале!")
            except TelegramBadRequest:
                pass

    await message.answer(f"✅ Готово! Опубликовано в канал.\nСсылка на контент: {deep_link}")
    await state.clear()


# ============ /submit — предложка ============
@router.message(Command("submit"))
async def cmd_submit(message: Message, state: FSMContext):
    if message.chat.type != "private":
        await message.answer("Пришли /submit мне в личные сообщения.")
        return
    if await is_banned(message.from_user.id):
        await message.answer("Вам запрещено пользоваться ботом.")
        return
    await state.clear()
    await state.set_state(Submit.collecting)
    await state.update_data(media=[], status_msg_id=None)
    await message.answer(
        "📥 Пришли контент, который хочешь предложить для канала — видео, фото, файлы, аудио, текст.\n"
        "Когда закончишь — жми кнопку, либо пришли /done."
    )


@router.message(StateFilter(Submit.collecting), F.content_type.in_(ACCEPTED_TYPES))
async def collect_submission(message: Message, state: FSMContext):
    if await is_banned(message.from_user.id):
        await state.clear()
        return
    if message.content_type == ContentType.TEXT and message.text.startswith("/"):
        return

    item = item_from_message(message)
    if item is None:
        return

    lock = get_batch_lock(message.chat.id)
    async with lock:
        data = await state.get_data()
        media = data.get("media", [])
        media.append(item)

        old_status_id = data.get("status_msg_id")
        if old_status_id:
            await safe_delete(message.bot, message.chat.id, old_status_id)

        status = await message.answer(
            f"Загружено файлов: {len(media)}. Отправить на проверку?",
            reply_markup=submit_stop_keyboard(len(media)),
        )
        await state.update_data(media=media, status_msg_id=status.message_id)


@router.callback_query(F.data == "stop_submit", StateFilter(Submit.collecting))
async def cb_stop_submit(callback: CallbackQuery, state: FSMContext):
    await callback.answer()
    await finish_submission(callback.bot, callback.message.chat.id, callback.from_user, state)


async def finish_submission(bot: Bot, chat_id: int, from_user, state: FSMContext):
    data = await state.get_data()
    media = data.get("media", [])
    await state.clear()

    if not media:
        await bot.send_message(chat_id, "Файлов не было, отмена.")
        return

    sub_id = await create_submission(from_user.id, from_user.username, media)
    await bot.send_message(chat_id, "✅ Отправлено на проверку. Спасибо! Как только админ решит — напишу вам.")

    text = (
        f"📨 Новая заявка на публикацию #{sub_id}\n"
        f"От: {from_user.full_name} (@{from_user.username or '—'}, id {from_user.id})\n"
        f"Файлов: {len(media)}"
    )
    for admin_id in await get_admin_ids():
        try:
            await bot.send_message(admin_id, text, reply_markup=submission_review_keyboard(sub_id))
        except TelegramBadRequest:
            pass


@router.callback_query(F.data.startswith("sub_appr:"))
async def cb_submission_approve(callback: CallbackQuery, state: FSMContext):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Только для админов.", show_alert=True)
        return

    sub_id = int(callback.data.split(":")[1])
    sub = await get_submission(sub_id)
    if not sub or sub["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)

    author_name = f"@{sub['username']}" if sub["username"] else f"id{sub['user_id']}"

    await state.clear()
    await state.set_state(Batch.waiting_title)
    await state.update_data(
        media=sub["items"],
        admin_id=callback.from_user.id,
        admin_name=callback.from_user.full_name,
        author_name=author_name,
        submission_id=sub_id,
    )
    await callback.message.answer(f"Заявка #{sub_id} одобрена. Пришли *название* поста.", parse_mode=ParseMode.MARKDOWN)


@router.callback_query(F.data.startswith("sub_rej:"))
async def cb_submission_reject(callback: CallbackQuery):
    if not await is_admin(callback.from_user.id):
        await callback.answer("Только для админов.", show_alert=True)
        return

    sub_id = int(callback.data.split(":")[1])
    sub = await get_submission(sub_id)
    if not sub or sub["status"] != "pending":
        await callback.answer("Заявка уже обработана.", show_alert=True)
        return

    await set_submission_status(sub_id, "rejected", callback.from_user.id)
    await callback.answer("Отклонено.")
    await callback.message.edit_reply_markup(reply_markup=None)
    try:
        await callback.bot.send_message(sub["user_id"], "😕 Ваш пост отклонён администратором.")
    except TelegramBadRequest:
        pass


@router.message(Command("pending"))
async def cmd_pending(message: Message):
    if not await is_admin(message.from_user.id):
        return
    rows = await list_pending_submissions()
    if not rows:
        await message.answer("Заявок в ожидании нет.")
        return
    lines = [f"#{r['id']} — id{r['user_id']} (@{r['username'] or '—'})" for r in rows]
    await message.answer("Заявки в ожидании:\n" + "\n".join(lines))


# ============ управление админами — только владелец ============
@router.message(Command("add"))
async def cmd_add_admin(message: Message):
    if not await is_owner(message.from_user.id):
        await message.answer("Добавлять админов может только владелец бота.")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /add <telegram_id>")
        return
    new_id = int(args[1].strip())
    await add_admin(new_id, None, message.from_user.id)
    await message.answer(f"✅ id{new_id} добавлен в админы.")
    try:
        await message.bot.send_message(new_id, "Вас назначили админом бота.")
    except TelegramBadRequest:
        pass


@router.message(Command("deladm"))
async def cmd_del_admin(message: Message):
    if not await is_owner(message.from_user.id):
        await message.answer("Убирать админов может только владелец бота.")
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /deladm <telegram_id>")
        return
    del_id = int(args[1].strip())
    ok = await remove_admin(del_id)
    await message.answer(
        f"✅ id{del_id} убран из админов." if ok
        else "Такого динамического админа нет в базе (возможно, задан статично через ADMIN_IDS в .env)."
    )


@router.message(Command("listadm"))
async def cmd_list_admins(message: Message):
    if not await is_admin(message.from_user.id):
        return
    lines = [f"👑 Владелец: id{OWNER_ID}"]
    if ADMIN_IDS:
        lines.append("Статичные (.env): " + ", ".join(f"id{i}" for i in ADMIN_IDS))
    db_admins = await list_admins()
    if db_admins:
        lines.append("Динамические (/add): " + ", ".join(f"id{r['user_id']}" for r in db_admins))
    await message.answer("\n".join(lines))


# ============ баны — доступны любому админу ============
@router.message(Command("ban"))
async def cmd_ban(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=2)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /ban <telegram_id> [причина]")
        return
    target_id = int(args[1].strip())
    if target_id == OWNER_ID or await is_admin(target_id):
        await message.answer("Нельзя забанить админа или владельца.")
        return
    reason = args[2].strip() if len(args) > 2 else None
    await ban_user(target_id, message.from_user.id, reason)
    await message.answer(f"🚫 id{target_id} забанен." + (f" Причина: {reason}" if reason else ""))


@router.message(Command("unban"))
async def cmd_unban(message: Message):
    if not await is_admin(message.from_user.id):
        return
    args = message.text.split(maxsplit=1)
    if len(args) < 2 or not args[1].strip().lstrip("-").isdigit():
        await message.answer("Использование: /unban <telegram_id>")
        return
    target_id = int(args[1].strip())
    ok = await unban_user(target_id)
    await message.answer(f"✅ id{target_id} разбанен." if ok else "Этот id не был забанен.")


@router.message(Command("banlist"))
async def cmd_banlist(message: Message):
    if not await is_admin(message.from_user.id):
        return
    rows = await list_bans()
    if not rows:
        await message.answer("Забаненных нет.")
        return
    lines = [f"id{r['user_id']}" + (f" — {r['reason']}" if r["reason"] else "") for r in rows]
    await message.answer("Забанены:\n" + "\n".join(lines))


# ============ /broadcast ============
@router.message(Command("broadcast"))
async def cmd_broadcast(message: Message):
    if not await is_admin(message.from_user.id):
        return

    source = message.reply_to_message
    args = message.text.split(maxsplit=1)
    text = args[1].strip() if len(args) > 1 else None

    if not source and not text:
        await message.answer(
            "Использование:\n"
            "/broadcast <текст> — разослать текст\n"
            "или ответь командой /broadcast на сообщение (фото/видео/файл с подписью) — разошлю его копией."
        )
        return

    user_ids = await get_all_user_ids()
    banned = {r["user_id"] for r in await list_bans()}
    targets = [uid for uid in user_ids if uid not in banned]

    status = await message.answer(f"⏳ Рассылка на {len(targets)} чел...")
    sent, failed = 0, 0
    for uid in targets:
        try:
            if source:
                await message.bot.copy_message(uid, source.chat.id, source.message_id)
            else:
                await message.bot.send_message(uid, text)
            sent += 1
        except TelegramBadRequest:
            failed += 1
        await asyncio.sleep(BROADCAST_DELAY_SECONDS)

    await status.edit_text(f"✅ Рассылка завершена. Доставлено: {sent}, не удалось: {failed}.")


# ============ health-check сервер (для Render free / Railway) ============
async def start_health_server():
    app = web.Application()
    app.router.add_get("/", lambda request: web.Response(text="OK"))
    runner = web.AppRunner(app)
    await runner.setup()
    port = int(os.environ.get("PORT", 8080))
    site = web.TCPSite(runner, "0.0.0.0", port)
    await site.start()
    log.info("Health-check сервер слушает порт %s", port)


async def self_ping_loop():
    """Актуально только для Render free — на Railway RENDER_EXTERNAL_URL не задан, цикл просто не запустится."""
    url = os.environ.get("RENDER_EXTERNAL_URL")
    if not url:
        log.info("RENDER_EXTERNAL_URL не задан — самопинг выключен")
        return
    async with ClientSession(timeout=ClientTimeout(total=15)) as session:
        while True:
            await asyncio.sleep(600)
            try:
                async with session.get(url) as resp:
                    log.info("Самопинг: %s -> %s", url, resp.status)
            except Exception as e:
                log.warning("Самопинг не удался: %s", e)


async def bootstrap_admins():
    for admin_id in BOOTSTRAP_ADMIN_IDS:
        if admin_id and admin_id != OWNER_ID:
            await add_admin(admin_id, None, added_by=OWNER_ID or admin_id)
            log.info("Bootstrap: id%s добавлен в админы", admin_id)


async def main():
    global BOT_USERNAME

    if not BOT_TOKEN:
        raise SystemExit("Укажи BOT_TOKEN в .env")
    if not CHANNEL_ID:
        raise SystemExit("Укажи CHANNEL_ID в .env")
    if not DATABASE_URL:
        raise SystemExit("Укажи DATABASE_URL (строка подключения к Neon) в .env")

    storage = PostgresStorage(DATABASE_URL)
    await storage.connect()
    await db_init(DATABASE_URL)
    await bootstrap_admins()

    bot = Bot(token=BOT_TOKEN)
    me = await bot.get_me()
    BOT_USERNAME = me.username

    dp = Dispatcher(storage=storage)
    dp.message.outer_middleware(TouchUserMiddleware())
    dp.include_router(router)

    try:
        await start_health_server()
        asyncio.create_task(self_ping_loop())
        await bot.delete_webhook(drop_pending_updates=True)
        await dp.start_polling(bot)
    finally:
        await storage.close()
        await db_close()


if __name__ == "__main__":
    asyncio.run(main())
