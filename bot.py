import asyncio
import logging
import os
import sqlite3
import time
from datetime import datetime, date, timedelta

from dotenv import load_dotenv
from aiogram import BaseMiddleware, Bot, Dispatcher, F, Router
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    ReplyKeyboardRemove,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    TelegramObject,
)
from typing import Any, Awaitable, Callable, Dict

# Google Calendar API
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pickle

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN", "PASTE_YOUR_BOT_TOKEN_HERE")
ADMIN_CHAT_IDS = [
    int(x) for x in os.getenv("ADMIN_CHAT_IDS", "0").split(",") if x.strip()
]

COMPANY_NAME = "GlobexTrans"
DB_PATH = os.path.join(os.path.dirname(__file__), "globextrans.db")

# Google Calendar
CALENDAR_SCOPES = ['https://www.googleapis.com/auth/calendar']
CREDENTIALS_FILE = os.path.join(os.path.dirname(__file__), "credentials.json")
TOKEN_FILE = os.path.join(os.path.dirname(__file__), "token.pickle")
CALENDAR_ID = os.getenv("CALENDAR_ID", "primary")

FIXED_DEPARTURE_DATES = {
    "ukraine": [date(2026, 8, 21), date(2026, 9, 4), date(2026, 9, 18), date(2026, 10, 2), date(2026, 10, 16)],
    "germany": [date(2026, 8, 23), date(2026, 9, 6), date(2026, 9, 20), date(2026, 10, 4), date(2026, 10, 18)],
}

COUNTRY_LABELS = {"ukraine": "🇺🇦 Україна", "germany": "🇩🇪 Німеччина"}

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO")
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)
router = Router()

MAX_TEXT_LENGTH = 300
THROTTLE_INTERVAL = 1.0


class ThrottlingMiddleware(BaseMiddleware):
    def __init__(self):
        self._last_seen: Dict[int, float] = {}

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        user = data.get("event_from_user")
        if user is not None:
            now = time.monotonic()
            last = self._last_seen.get(user.id, 0.0)
            if now - last < THROTTLE_INTERVAL:
                return
            self._last_seen[user.id] = now
        return await handler(event, data)


# ------------------------------------------------------------------
# GOOGLE CALENDAR
# ------------------------------------------------------------------

def get_google_calendar_service():
    """Отримуємо авторизований сервіс Google Calendar."""
    creds = None

    if os.path.exists(TOKEN_FILE):
        with open(TOKEN_FILE, 'rb') as token:
            creds = pickle.load(token)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            from google.auth.transport.requests import Request
            creds.refresh(Request())
        else:
            if not os.path.exists(CREDENTIALS_FILE):
                raise FileNotFoundError(
                    f"❌ Файл {CREDENTIALS_FILE} не знайдено. "
                    "Завантажте його з Google Cloud Console."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, CALENDAR_SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)
            logger.info("💾 Токен Google Calendar збережено")

    return build('calendar', 'v3', credentials=creds)


def create_calendar_event(title: str, description: str, event_date: str):
    """Створюємо подію в Google Calendar на дату виїзду."""
    try:
        service = get_google_calendar_service()

        event_start = datetime.fromisoformat(event_date)
        event_end = event_start + timedelta(hours=2)

        event = {
            'summary': title,
            'description': description,
            'start': {
                'dateTime': event_start.isoformat(),
                'timeZone': 'Europe/Kiev',
            },
            'end': {
                'dateTime': event_end.isoformat(),
                'timeZone': 'Europe/Kiev',
            },
        }

        result = service.events().insert(calendarId=CALENDAR_ID, body=event).execute()
        logger.info(f"📅 Подія створена в календарі: {result.get('id')}")
        return True
    except Exception as e:
        logger.error(f"❌ Помилка при створенні подієї в календарі: {e}")
        return False


async def create_calendar_event_async(title: str, description: str, event_date: str, message):
    """Асинхронна обгортка для Google Calendar (запускається в фоні)."""
    created = await asyncio.to_thread(create_calendar_event, title, description, event_date)
    if created:
        try:
            await message.answer("📅 Подія додана в Google Calendar!")
        except Exception as e:
            logger.warning(f"⚠️ Не вдалось надіслати повідомлення про календар: {e}")


# ------------------------------------------------------------------
# БАЗА ДАНИХ
# ------------------------------------------------------------------

def db_connect():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db():
    conn = db_connect()
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            username TEXT,
            req_type TEXT NOT NULL,
            direction TEXT,
            location TEXT,
            address TEXT,
            phone TEXT,
            dimensions TEXT,
            weight TEXT,
            destination TEXT,
            departure_date TEXT,
            status TEXT DEFAULT 'new',
            created_at TEXT NOT NULL
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON requests(user_id)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON requests(status)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_created_at ON requests(created_at)")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_user_created ON requests(user_id, created_at)")

    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS status_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER NOT NULL,
            old_status TEXT,
            new_status TEXT,
            changed_by TEXT,
            changed_at TEXT NOT NULL,
            FOREIGN KEY(request_id) REFERENCES requests(id)
        )
        """
    )
    conn.execute("CREATE INDEX IF NOT EXISTS idx_request_id ON status_log(request_id)")

    conn.commit()
    conn.close()
    logger.info("✅ База даних ініціалізована")


def save_request(data: dict) -> int:
    conn = db_connect()
    cur = conn.execute(
        """
        INSERT INTO requests
            (user_id, username, req_type, direction, location, address, phone,
             dimensions, weight, destination, departure_date, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'new', ?)
        """,
        (
            data.get("user_id"),
            data.get("username"),
            data.get("req_type"),
            data.get("direction"),
            data.get("location"),
            data.get("address"),
            data.get("phone"),
            data.get("dimensions"),
            data.get("weight"),
            data.get("destination"),
            data.get("departure_date"),
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    req_id = cur.lastrowid
    conn.close()
    return req_id


def save_not_found_request(data: dict) -> int:
    conn = db_connect()
    cur = conn.execute(
        """
        INSERT INTO requests
            (user_id, username, req_type, direction, location, phone, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?, 'new', ?)
        """,
        (
            data.get("user_id"),
            data.get("username"),
            data.get("req_type"),
            data.get("direction"),
            data.get("location"),
            data.get("phone"),
            datetime.now().isoformat(timespec="seconds"),
        ),
    )
    conn.commit()
    req_id = cur.lastrowid
    conn.close()
    return req_id


def get_user_requests(user_id: int):
    conn = db_connect()
    rows = conn.execute(
        "SELECT * FROM requests WHERE user_id = ? ORDER BY id DESC LIMIT 10",
        (user_id,),
    ).fetchall()
    conn.close()
    return rows


def get_request(req_id: int):
    conn = db_connect()
    row = conn.execute("SELECT * FROM requests WHERE id = ?", (req_id,)).fetchone()
    conn.close()
    return row


def set_status(req_id: int, status: str, changed_by: str = "admin"):
    try:
        conn = db_connect()
        old_req = conn.execute("SELECT status FROM requests WHERE id = ?", (req_id,)).fetchone()
        old_status = old_req["status"] if old_req else None

        conn.execute("UPDATE requests SET status = ? WHERE id = ?", (status, req_id))

        conn.execute(
            """
            INSERT INTO status_log (request_id, old_status, new_status, changed_by, changed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (req_id, old_status, status, changed_by, datetime.now().isoformat(timespec="seconds"))
        )
        conn.commit()
        conn.close()
        logger.info(f"📝 Заявка #{req_id}: статус змінено {old_status} → {status}")
    except Exception as e:
        logger.error(f"❌ Помилка при оновленні статусу заявки #{req_id}: {e}")
        raise


# ------------------------------------------------------------------
# СТАНИ (FSM)
# ------------------------------------------------------------------

class RequestForm(StatesGroup):
    # Загальні
    direction = State()

    # Пасажирські
    location_choice = State()
    custom_city = State()
    address = State()
    phone = State()
    has_baggage = State()
    dimensions = State()
    weight = State()
    destination = State()
    departure_date = State()

    # Посилки
    parcel_city_choice = State()
    parcel_phone = State()

    # "Не має міста"
    not_found_city = State()
    not_found_phone = State()


# ------------------------------------------------------------------
# КЛАВІАТУРИ
# ------------------------------------------------------------------

def type_choice_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚖 Пасажирські перевезення")],
            [KeyboardButton(text="📦 Посилка")],
            [KeyboardButton(text="📋 Мої заявки")],
        ],
        resize_keyboard=True,
    )


def phone_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="📱 Надіслати мій номер", request_contact=True)]],
        resize_keyboard=True,
        one_time_keyboard=True,
    )


def cancel_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text="❌ Скасувати")]],
        resize_keyboard=True,
    )


def baggage_choice_kb() -> ReplyKeyboardMarkup:
    """Вибір чи буде багаж"""
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Так, буде багаж")],
            [KeyboardButton(text="❌ Ні, без багажу")],
        ],
        resize_keyboard=True,
    )


def parcel_city_kb() -> InlineKeyboardMarkup:
    """Вибір міста для посилок"""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏙️ Івано-Франківськ", callback_data="parcel_city:if")],
            [InlineKeyboardButton(text="📍 Інші міста", callback_data="parcel_city:other")],
        ]
    )


def admin_status_kb(req_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(text="✅ Прийнято в роботу", callback_data=f"st:{req_id}:in_progress"),
                InlineKeyboardButton(text="🏁 Виконано", callback_data=f"st:{req_id}:done"),
            ]
        ]
    )


def direction_choice_kb() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🇺🇦 Україна → Німеччина")],
            [KeyboardButton(text="🇩🇪 Німеччина → Україна")],
        ],
        resize_keyboard=True,
    )


def ukraine_cities_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏙️ Львів", callback_data="city:lviv")],
            [InlineKeyboardButton(text="🏙️ Івано-Франківськ", callback_data="city:ivano_frankivsk")],
            [InlineKeyboardButton(text="🏘️ Львівська область", callback_data="city:lviv_region")],
            [InlineKeyboardButton(text="🏘️ Івано-Франківська область", callback_data="city:ivano_frankivsk_region")],
            [InlineKeyboardButton(text="❌ Не має вашого міста", callback_data="city:not_found")],
        ]
    )


def get_available_dates(country: str) -> list:
    today = date.today()
    all_dates = FIXED_DEPARTURE_DATES.get(country, [])
    return [d for d in all_dates if d >= today]


def date_choice_kb(direction: str) -> InlineKeyboardMarkup:
    buttons = []
    row = []
    for d in get_available_dates(direction):
        label = d.strftime("%d.%m.%Y")
        row.append(InlineKeyboardButton(text=label, callback_data=f"date:{d.isoformat()}"))
        if len(row) == 2:
            buttons.append(row)
            row = []
    if row:
        buttons.append(row)
    return InlineKeyboardMarkup(inline_keyboard=buttons)


# ------------------------------------------------------------------
# ЗАГАЛЬНІ КОМАНДИ
# ------------------------------------------------------------------

@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
    logger.info(f"👤 Користувач {message.from_user.id} (@{message.from_user.username}) розпочав бота")
    await message.answer(
        f"Вас вітає <b>{COMPANY_NAME}</b>! 🚐\n\n"
        "Оберіть, будь ласка, потрібну послугу нижче 👇",
        reply_markup=type_choice_kb(),
        parse_mode="HTML",
    )


@router.message(Command("cancel"))
@router.message(F.text == "❌ Скасувати")
async def cmd_cancel(message: Message, state: FSMContext):
    await state.clear()
    logger.info(f"⛔ Користувач {message.from_user.id} скасував заявку")
    await message.answer("Заявку скасовано. Повертаю до головного меню.", reply_markup=type_choice_kb())


@router.message(Command("all"))
async def cmd_all_requests(message: Message):
    user_id = message.from_user.id
    if user_id not in ADMIN_CHAT_IDS:
        await message.answer("❌ У вас немає доступу до цієї команди.")
        logger.warning(f"⚠️ Користувач {user_id} спробував використати /all без дозволу")
        return

    try:
        conn = db_connect()
        rows = conn.execute(
            "SELECT * FROM requests ORDER BY created_at DESC LIMIT 50"
        ).fetchall()
        conn.close()

        if not rows:
            await message.answer("📭 Заявок немає.")
            return

        status_labels = {"new": "🆕 нова", "in_progress": "⏳ в роботі", "done": "✅ виконано"}
        lines = [f"<b>Останні 50 заявок:</b>\n"]

        for r in rows:
            kind = "🚖 Пасажири" if r["req_type"] == "passenger" else "📦 Посилка"
            lines.append(
                f"#{r['id']} — {kind} | @{r['username'] or r['user_id']}\n"
                f"  {r['location'] or 'N/A'}, {r['address'] or 'N/A'} → {r['destination']}\n"
                f"  Статус: {status_labels.get(r['status'], r['status'])} | {r['created_at']}\n"
            )

        await message.answer("\n".join(lines), parse_mode="HTML")
        logger.info(f"📊 Адмін {user_id} переглянув всі заявки")
    except Exception as e:
        logger.error(f"❌ Помилка при отриманні заявок: {e}")
        await message.answer("❌ Помилка при отриманні даних.")


@router.message(F.text == "📋 Мої заявки")
async def my_requests(message: Message):
    rows = get_user_requests(message.from_user.id)
    if not rows:
        await message.answer("У вас поки немає заявок.")
        return
    status_labels = {"new": "🆕 нова", "in_progress": "⏳ в роботі", "done": "✅ виконано"}
    lines = []
    for r in rows:
        kind = "🚖 Пасажири" if r["req_type"] == "passenger" else "📦 Посилка"
        lines.append(
            f"#{r['id']} — {kind}\n"
            f"{r['location'] or 'N/A'}, {r['address'] or 'N/A'} → {r['destination']}\n"
            f"Дата: {r['departure_date'] or '—'}\n"
            f"Статус: {status_labels.get(r['status'], r['status'])}\n"
        )
    await message.answer("\n".join(lines))


# ------------------------------------------------------------------
# ФОРМА ПАСАЖИРСЬКИХ ПЕРЕВЕЗЕНЬ
# ------------------------------------------------------------------

async def is_too_long(message: Message) -> bool:
    text = message.text or ""
    if len(text) > MAX_TEXT_LENGTH:
        await message.answer(
            f"Текст занадто довгий (макс. {MAX_TEXT_LENGTH} символів). Спробуйте коротше."
        )
        return True
    return False


@router.message(F.text.in_({"🚖 Пасажирські перевезення", "📦 Посилка"}))
async def start_form(message: Message, state: FSMContext):
    req_type = "passenger" if "Пасажирські" in message.text else "parcel"
    await state.update_data(req_type=req_type, user_id=message.from_user.id, username=message.from_user.username)

    if req_type == "passenger":
        # Пасажирські перевезення
        await state.set_state(RequestForm.direction)
        await message.answer(
            "🚗 Обберіть напрямок подорожі:",
            reply_markup=direction_choice_kb(),
        )
    else:
        # Посилки
        await state.set_state(RequestForm.parcel_city_choice)
        await message.answer(
            "📦 Звідки ви хочете відправити посилку?",
            reply_markup=parcel_city_kb(),
        )


@router.message(RequestForm.direction)
async def form_direction(message: Message, state: FSMContext):
    if "Україна" in message.text:
        direction = "ukraine"
    elif "Німеччина" in message.text:
        direction = "germany"
    else:
        await message.answer("❌ Обберіть з кнопок!")
        return

    await state.update_data(direction=direction)
    await state.set_state(RequestForm.location_choice)
    await message.answer(
        "📍 Обберіть місто або область:",
        reply_markup=ukraine_cities_kb(),
    )


@router.callback_query(RequestForm.location_choice, F.data.startswith("city:"))
async def form_location_choice(callback: CallbackQuery, state: FSMContext):
    city_choice = callback.data.split(":", 1)[1]

    if city_choice == "not_found":
        await state.set_state(RequestForm.not_found_city)
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.answer()
        await callback.message.answer(
            "📍 Не має вашого міста\n\nВкажіть своє місто/село:",
            reply_markup=cancel_kb(),
        )
    else:
        city_labels = {
            "lviv": "Львів",
            "ivano_frankivsk": "Івано-Франківськ",
            "lviv_region": "Львівська область",
            "ivano_frankivsk_region": "Івано-Франківська область",
        }

        await state.update_data(location=city_labels.get(city_choice, city_choice))

        if "region" in city_choice:
            await state.set_state(RequestForm.custom_city)
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            await callback.message.answer(
                "🏘️ Вкажіть місто або село в області:",
                reply_markup=cancel_kb(),
            )
        else:
            await state.set_state(RequestForm.address)
            await callback.message.edit_reply_markup(reply_markup=None)
            await callback.answer()
            await callback.message.answer(
                "🏠 Введіть адресу (вулиця, номер дому):",
                reply_markup=cancel_kb(),
            )


@router.callback_query(RequestForm.parcel_city_choice, F.data.startswith("parcel_city:"))
async def form_parcel_city_choice(callback: CallbackQuery, state: FSMContext):
    """Обробка вибору міста для посилок"""
    city_choice = callback.data.split(":", 1)[1]

    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer()

    if city_choice == "if":
        # Івано-Франківськ
        await state.update_data(location="Івано-Франківськ")
        await state.set_state(RequestForm.parcel_phone)
        await callback.message.answer(
            "📱 Ваш номер телефону?",
            reply_markup=phone_kb(),
        )
    else:
        # Інші міста
        info_text = (
            "📍 <b>Інші міста</b>\n\n"
            "<i>Всі передачі з України відправляються на адресу Нової Пошти, потім доставляються в Німеччину.</i>\n\n"
            "<b>Адреса Нової Пошти:</b>\n"
            "м. Івано-Франківськ, НП#1\n"
            "Шишкін Павло\n"
            "☎️ 0972107800\n\n"
            "<b>Інструкції з пакування:</b>\n"
            "✅ ОБОВ'ЯЗКОВО всі посилки пакуються в сумки або чемодани\n"
            "✅ ОБОВ'ЯЗКОВО в посилці має бути листочок з точним адресом і номером телефону отримувача\n\n"
            "<b>Заборонені речи в передачах:</b>\n"
            "❌ Сигарети\n"
            "❌ Алкоголь\n"
            "❌ Мясне\n"
            "❌ Лікарство (в обмеженій кількості, по 1-2 біостера кожного виду лікарства)"
        )
        await callback.message.answer(info_text, parse_mode="HTML")

        # Завершуємо заявку для "Інші міста"
        await state.update_data(location="Інші міста")
        data = await state.get_data()
        req_id = save_request(data)
        await state.clear()

        await callback.message.answer(
            f"✅ Дякуємо!\n\n"
            f"Вашу заявку №{req_id} прийнято. "
            f"Команда GlobexTrans звʼяжеться з вами найближчим часом.",
            reply_markup=type_choice_kb(),
        )
        logger.info(f"📦 Заявка посилки від іншого міста від користувача {callback.from_user.id}: #{req_id}")

        # Повідомляємо адмінів
        kind = "📦 Посилка"
        text = (
            f"<b>Нова заявка</b> №{req_id} — {kind}\n\n"
            f"Місто: Інші міста\n"
            f"Користувач: @{callback.from_user.username or callback.from_user.id}"
        )
        await notify_admins(callback.bot, text, req_id)


@router.message(RequestForm.parcel_phone, F.contact)
async def form_parcel_phone_contact(message: Message, state: FSMContext, bot: Bot):
    """Обробка телефону для посилок (Івано-Франківськ)"""
    await state.update_data(phone=message.contact.phone_number)
    data = await state.get_data()
    req_id = save_request(data)
    await state.clear()

    await message.answer(
        f"✅ Дякуємо!\n\n"
        f"Вашу заявку №{req_id} прийнято. "
        f"Команда GlobexTrans звʼяжеться з вами найближчим часом.",
        reply_markup=type_choice_kb(),
    )
    logger.info(f"📦 Заявка посилки від користувача {message.from_user.id}: #{req_id}")

    # Повідомляємо адмінів
    kind = "📦 Посилка"
    text = (
        f"<b>Нова заявка</b> №{req_id} — {kind}\n\n"
        f"Місто: {data.get('location')}\n"
        f"Телефон: {data.get('phone')}\n"
        f"Користувач: @{message.from_user.username or message.from_user.id}"
    )
    await notify_admins(bot, text, req_id)


@router.message(RequestForm.parcel_phone)
async def form_parcel_phone_text(message: Message, state: FSMContext, bot: Bot):
    """Текстовий вввід телефону для посилок"""
    if await is_too_long(message):
        return

    await state.update_data(phone=message.text)
    data = await state.get_data()
    req_id = save_request(data)
    await state.clear()

    await message.answer(
        f"✅ Дякуємо!\n\n"
        f"Вашу заявку №{req_id} прийнято. "
        f"Команда GlobexTrans звʼяжеться з вами найближчим часом.",
        reply_markup=type_choice_kb(),
    )
    logger.info(f"📦 Заявка посилки від користувача {message.from_user.id}: #{req_id}")

    # Повідомляємо адмінів
    kind = "📦 Посилка"
    text = (
        f"<b>Нова заявка</b> №{req_id} — {kind}\n\n"
        f"Місто: {data.get('location')}\n"
        f"Телефон: {data.get('phone')}\n"
        f"Користувач: @{message.from_user.username or message.from_user.id}"
    )
    await notify_admins(bot, text, req_id)


async def form_custom_city(message: Message, state: FSMContext):
    if await is_too_long(message):
        return

    custom_city = message.text
    data = await state.get_data()
    region = data.get("location", "")

    full_location = f"{custom_city}, {region}"
    await state.update_data(location=full_location)

    await state.set_state(RequestForm.address)
    await message.answer(
        "🏠 Введіть адресу (вулиця, номер дому):",
        reply_markup=cancel_kb(),
    )


@router.message(RequestForm.address)
async def form_address(message: Message, state: FSMContext):
    if await is_too_long(message):
        return

    await state.update_data(address=message.text)
    await state.set_state(RequestForm.phone)
    await message.answer(
        "📱 Контактний номер телефону?",
        reply_markup=phone_kb(),
    )


@router.message(RequestForm.phone, F.contact)
async def form_phone_contact(message: Message, state: FSMContext):
    await state.update_data(phone=message.contact.phone_number)
    await state.set_state(RequestForm.has_baggage)
    await message.answer(
        "🎒 Чи у вас буде багаж?",
        reply_markup=baggage_choice_kb(),
    )


@router.message(RequestForm.phone)
async def form_phone_text(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(phone=message.text)
    await state.set_state(RequestForm.has_baggage)
    await message.answer(
        "🎒 Чи у вас буде багаж?",
        reply_markup=baggage_choice_kb(),
    )


@router.message(RequestForm.has_baggage)
async def form_has_baggage(message: Message, state: FSMContext):
    if "Так" in message.text or "так" in message.text:
        # Буде багаж — запитуємо кількість
        await state.update_data(has_baggage=True)
        await state.set_state(RequestForm.dimensions)
        await message.answer(
            "🛄 Скільки вас буде з багажем? (кількість осіб/багажних одиниць):",
            reply_markup=ReplyKeyboardRemove(),
        )
    elif "Ні" in message.text or "ні" in message.text or "без" in message.text:
        # Без багажу — пропускаємо на destination
        await state.update_data(has_baggage=False, dimensions=None, weight=None)
        await state.set_state(RequestForm.destination)
        await message.answer(
            "🎯 Куди їдете? (місто/адреса):",
            reply_markup=ReplyKeyboardRemove(),
        )
    else:
        await message.answer("❌ Обберіть з кнопок!")


@router.message(RequestForm.dimensions)
async def form_dimensions(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(dimensions=message.text)
    await state.set_state(RequestForm.weight)
    await message.answer("⚖️ Орієнтовна вага (кг)?")


@router.message(RequestForm.weight)
async def form_weight(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(weight=message.text)
    await state.set_state(RequestForm.destination)
    await message.answer("🎯 Куди їдете? (місто/адреса):")


@router.message(RequestForm.destination)
async def form_destination(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(destination=message.text)
    await state.set_state(RequestForm.departure_date)

    data = await state.get_data()
    direction = data.get("direction", "ukraine")

    await message.answer(
        "📅 Оберіть дату виїзду:",
        reply_markup=date_choice_kb(direction),
    )


@router.callback_query(RequestForm.departure_date, F.data.startswith("date:"))
async def form_departure_date(callback: CallbackQuery, state: FSMContext, bot: Bot):
    chosen_date = callback.data.split(":", 1)[1]
    await state.update_data(departure_date=chosen_date)
    data = await state.get_data()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.answer()
    await finalize_request(
        chat_id=callback.from_user.id,
        username=callback.from_user.username,
        bot=bot,
        state=state,
        data=data,
    )


# ------------------------------------------------------------------
# ФОРМА "НЕ ЗНАЙШЛИ МІСТА"
# ------------------------------------------------------------------

@router.message(RequestForm.not_found_city)
async def form_not_found_city(message: Message, state: FSMContext):
    if await is_too_long(message):
        return

    await state.update_data(location=message.text)
    await state.set_state(RequestForm.not_found_phone)
    await message.answer(
        "📱 Ваш номер телефону:",
        reply_markup=phone_kb(),
    )


@router.message(RequestForm.not_found_phone, F.contact)
async def form_not_found_phone_contact(message: Message, state: FSMContext):
    await state.update_data(phone=message.contact.phone_number)
    data = await state.get_data()

    save_not_found_request(data)

    await state.clear()
    await message.answer(
        "✅ Спасибі!\n\n"
        "Команда GlobexTrans звяжеться з вами найближчим часом "
        "для уточнення можливості поїздки.",
        reply_markup=type_choice_kb(),
    )
    logger.info(f"📝 Заявка 'не має міста' від користувача {message.from_user.id}: {data.get('location')}")


@router.message(RequestForm.not_found_phone)
async def form_not_found_phone_text(message: Message, state: FSMContext):
    if await is_too_long(message):
        return

    await state.update_data(phone=message.text)
    data = await state.get_data()

    save_not_found_request(data)

    await state.clear()
    await message.answer(
        "✅ Спасибі!\n\n"
        "Команда GlobexTrans звяжеться з вами найближчим часом "
        "для уточнення можливості поїздки.",
        reply_markup=type_choice_kb(),
    )
    logger.info(f"📝 Заявка 'не має міста' від користувача {message.from_user.id}: {data.get('location')}")


# ------------------------------------------------------------------
# ФІНАЛІЗАЦІЯ ЗАЯВКИ
# ------------------------------------------------------------------

async def finalize_request(chat_id: int, username: str, bot: Bot, state: FSMContext, data: dict):
    req_id = save_request(data)
    await state.clear()
    await bot.send_message(
        chat_id,
        f"✅ Дякуємо, що скористались нашим сервісом, {COMPANY_NAME}!\n"
        f"Вашу заявку №{req_id} прийнято. Наш менеджер звʼяжеться з вами найближчим часом.",
        reply_markup=type_choice_kb(),
    )

    kind = "🚖 Пасажирські перевезення" if data.get("req_type") == "passenger" else "📦 Посилка"
    direction_label = "🇺🇦 Україна → Німеччина" if data.get("direction") == "ukraine" else "🇩🇪 Німеччина → Україна"

    text = (
        f"<b>Нова заявка</b> №{req_id} — {kind}\n\n"
        f"Напрямок: {direction_label}\n"
        f"Місто/село: {data.get('location')}\n"
        f"Адреса: {data.get('address')}\n"
        f"Телефон: {data.get('phone')}\n"
        f"Розміри: {data.get('dimensions')}\n"
        f"Вага: {data.get('weight')} кг\n"
        f"Пункт призначення: {data.get('destination')}\n"
        f"Дата виїзду: {data.get('departure_date')}\n"
        f"Користувач: @{username or chat_id}"
    )
    await notify_admins(bot, text, req_id)


# ------------------------------------------------------------------
# СПОВІЩЕННЯ АДМІНІВ + ОБРОБКА СТАТУСІВ
# ------------------------------------------------------------------

async def notify_admins(bot: Bot, text: str, req_id: int):
    if not ADMIN_CHAT_IDS or ADMIN_CHAT_IDS == [0]:
        logger.warning(f"⚠️ ADMIN_CHAT_IDS не налаштовано — заявка #{req_id} збережена, але сповіщення не надіслано.")
        return

    for chat_id in ADMIN_CHAT_IDS:
        try:
            logger.info(f"📬 Надсилаємо заявку #{req_id} адміну {chat_id}")
            await bot.send_message(chat_id, text, parse_mode="HTML", reply_markup=admin_status_kb(req_id))
        except Exception as e:
            logger.error(f"❌ Не вдалось надіслати повідомлення адміну {chat_id}: {e}")


@router.callback_query(F.data.startswith("st:"))
async def change_status(callback: CallbackQuery, bot: Bot):
    try:
        _, req_id, status = callback.data.split(":")
        req_id = int(req_id)
        set_status(req_id, status, changed_by=f"admin_{callback.from_user.id}")

        labels = {"in_progress": "⏳ в роботі", "done": "✅ виконано"}
        status_text = labels.get(status, status)

        await callback.answer(f"✅ Статус заявки №{req_id} оновлено: {status_text}")
        await callback.message.edit_reply_markup(reply_markup=None)
        await callback.message.answer(f"📝 Статус заявки №{req_id}: {status_text}")

        req = get_request(req_id)
        if req is not None:
            client_texts = {
                "in_progress": f"⏳ Вашу заявку №{req_id} прийнято в роботу. Менеджер {COMPANY_NAME} може звʼязатись з вами для уточнення деталей.",
                "done": f"✅ Вашу заявку №{req_id} виконано. Дякуємо, що скористались {COMPANY_NAME}!",
            }
            text = client_texts.get(status)
            if text:
                try:
                    await bot.send_message(req["user_id"], text)
                    logger.info(f"📨 Клієнт {req['user_id']} повідомлений про статус заявки #{req_id}")
                except Exception as e:
                    logger.error(f"❌ Не вдалось повідомити клієнта {req['user_id']} про статус заявки №{req_id}: {e}")

            # НОВЕ: Якщо статус "done" — створюємо подію в Google Calendar (в фоні, без блокування)
            if status == "done" and req["departure_date"]:
                title = f"Заявка №{req_id} — {req['location'] or 'N/A'} → {req['destination'] or 'N/A'}"
                description = (
                    f"Тип: {'🚖 Пасажирські перевезення' if req['req_type'] == 'passenger' else '📦 Посилка'}\n"
                    f"Місто: {req['location']}\n"
                    f"Адреса: {req['address']}\n"
                    f"Телефон: {req['phone']}\n"
                    f"Розміри: {req['dimensions']}\n"
                    f"Вага: {req['weight']} кг\n"
                    f"Пункт призначення: {req['destination']}\n"
                    f"Користувач: @{req['username'] or req['user_id']}"
                )
                # Запускаємо в фоні без блокування
                asyncio.create_task(
                    create_calendar_event_async(title, description, req["departure_date"], callback.message)
                )

    except ValueError as e:
        logger.error(f"❌ Помилка парсингу callback_data: {e}")
        await callback.answer("❌ Помилка обробки запиту.")
    except Exception as e:
        logger.error(f"❌ Помилка при зміні статусу: {e}")
        await callback.answer("❌ Помилка при оновленні статусу.")


# ------------------------------------------------------------------
# ЗАПУСК
# ------------------------------------------------------------------

async def main():
    if BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
        raise SystemExit(
            "❌ ПОМИЛКА: Вкажіть токен бота у змінній середовища BOT_TOKEN або у файлі .env (див. README.md)."
        )
    if not ADMIN_CHAT_IDS or ADMIN_CHAT_IDS == [0]:
        logger.warning("⚠️ ADMIN_CHAT_IDS не налаштовано. Заявки будуть зберігатися, але адміни їх не отримуватимуть.")

    logger.info("🚀 Запускаємо GlobexTrans Bot...")
    init_db()
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())
    dp.message.middleware(ThrottlingMiddleware())
    dp.include_router(router)

    try:
        await bot.delete_webhook(drop_pending_updates=True)
        logger.info("✅ Бот запустився. Чекаємо повідомлень...")
        await dp.start_polling(bot)
    except Exception as e:
        logger.error(f"❌ КРИТИЧНА ПОМИЛКА: {e}")
        raise
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
