import asyncio
import logging
import os
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
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
    TelegramObject,
)
from typing import Any, Awaitable, Callable, Dict
import aiosqlite

# Google Calendar API
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
import pickle

load_dotenv()

BOT_TOKEN = os.getenv("BOT_TOKEN")
if not BOT_TOKEN or BOT_TOKEN == "PASTE_YOUR_BOT_TOKEN_HERE":
    raise SystemExit("❌ ПОМИЛКА: Вкажіть токен бота у змінній середовища BOT_TOKEN.")

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
                    f"❌ Файл {CREDENTIALS_FILE} не знайдено."
                )
            flow = InstalledAppFlow.from_client_secrets_file(
                CREDENTIALS_FILE, CALENDAR_SCOPES)
            creds = flow.run_local_server(port=0)

        with open(TOKEN_FILE, 'wb') as token:
            pickle.dump(creds, token)

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
        logger.error(f"❌ Помилка при створенні події в календарі: {e}")
        return False


async def create_calendar_event_async(title: str, description: str, event_date: str, message: Message):
    """Асинхронна обгортка для Google Calendar."""
    created = await asyncio.to_thread(create_calendar_event, title, description, event_date)
    if created:
        try:
            await message.answer("📅 Подія додана в Google Calendar!")
        except Exception as e:
            logger.warning(f"⚠️ Не вдалось надіслати повідомлення про календар: {e}")


# ------------------------------------------------------------------
# БАЗА ДАНИХ (AIOSQLITE)
# ------------------------------------------------------------------

async def init_db():
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
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
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_user_id ON requests(user_id)")
        await conn.execute("CREATE INDEX IF NOT EXISTS idx_status ON requests(status)")
        await conn.execute(
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
        await conn.commit()
    logger.info("✅ База даних ініціалізована (aiosqlite)")


async def save_request(data: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute(
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
        await conn.commit()
        return cur.lastrowid


async def save_not_found_request(data: dict) -> int:
    async with aiosqlite.connect(DB_PATH) as conn:
        cur = await conn.execute(
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
        await conn.commit()
        return cur.lastrowid


async def get_user_requests(user_id: int):
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute(
            "SELECT * FROM requests WHERE user_id = ? ORDER BY id DESC LIMIT 10",
            (user_id,),
        ) as cursor:
            return await cursor.fetchall()


async def set_status(req_id: int, status: str, changed_by: str = "admin"):
    async with aiosqlite.connect(DB_PATH) as conn:
        conn.row_factory = aiosqlite.Row
        async with conn.execute("SELECT status FROM requests WHERE id = ?", (req_id,)) as cursor:
            old_req = await cursor.fetchone()
            old_status = old_req["status"] if old_req else None

        await conn.execute("UPDATE requests SET status = ? WHERE id = ?", (status, req_id))
        await conn.execute(
            """
            INSERT INTO status_log (request_id, old_status, new_status, changed_by, changed_at)
            VALUES (?, ?, ?, ?, ?)
            """,
            (req_id, old_status, status, changed_by, datetime.now().isoformat(timespec="seconds"))
        )
        await conn.commit()


# ------------------------------------------------------------------
# FSM СТАНИ
# ------------------------------------------------------------------

class RequestForm(StatesGroup):
    direction = State()
    postal_code = State()
    city = State()
    street = State()
    house_number = State()
    phone = State()
    has_baggage = State()
    baggage_quantity = State()
    weight = State()
    destination = State()
    departure_date = State()

    parcel_country_choice = State()
    parcel_city_choice = State()
    parcel_phone = State()

    parcel_de_postal_code = State()
    parcel_de_city = State()
    parcel_de_street = State()
    parcel_de_house_number = State()
    parcel_de_quantity = State()
    parcel_de_weight = State()

    not_found_city = State()
    not_found_phone = State()


# ------------------------------------------------------------------
# КЛАВІАТУРИ ТА ДИНАМІЧНІ ДАТИ
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
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="✅ Так, буде багаж")],
            [KeyboardButton(text="❌ Ні, без багажу")],
        ],
        resize_keyboard=True,
    )


def parcel_city_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🏙️ Івано-Франківськ", callback_data="parcel_city:if")],
            [InlineKeyboardButton(text="📍 Інші міста", callback_data="parcel_city:other")],
        ]
    )


def parcel_country_kb() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="🇺🇦 Україна", callback_data="parcel_country:ukraine")],
            [InlineKeyboardButton(text="🇩🇪 Німеччина", callback_data="parcel_country:germany")],
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


def get_available_dates(country: str) -> list:
    """Динамічна генерація наступних 5 виїздів від сьогодення."""
    today = date.today()
    offset = 0 if country == "ukraine" else 2
    dates = []
    
    current = today + timedelta(days=(4 - today.weekday() + offset) % 7)
    for _ in range(5):
        if current >= today:
            dates.append(current)
        current += timedelta(days=14)
    return dates


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
# ХЕНДЛЕРИ
# ------------------------------------------------------------------

async def is_too_long(message: Message) -> bool:
    text = message.text or ""
    if len(text) > MAX_TEXT_LENGTH:
        await message.answer(
            f"Текст занадто довгий (макс. {MAX_TEXT_LENGTH} символів). Спробуйте коротше."
        )
        return True
    return False


@router.message(CommandStart())
async def cmd_start(message: Message, state: FSMContext):
    await state.clear()
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
    await message.answer("Заявку скасовано. Повертаю до головного меню.", reply_markup=type_choice_kb())


@router.message(F.text == "📋 Мої заявки")
async def my_requests(message: Message):
    rows = await get_user_requests(message.from_user.id)
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
    await message.answer("\n\n".join(lines))


@router.message(F.text.in_({"🚖 Пасажирські перевезення", "📦 Посилка"}))
async def start_form(message: Message, state: FSMContext):
    req_type = "passenger" if "Пасажирські" in message.text else "parcel"
    await state.update_data(req_type=req_type, user_id=message.from_user.id, username=message.from_user.username)

    if req_type == "passenger":
        await state.set_state(RequestForm.direction)
        await message.answer("🚗 Обберіть напрямок подорожі:", reply_markup=direction_choice_kb())
    else:
        await state.set_state(RequestForm.parcel_country_choice)
        await message.answer("📦 Звідки ви хочете відправити посилку?", reply_markup=parcel_country_kb())


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
    await state.set_state(RequestForm.postal_code)
    await message.answer("📮 Введіть поштовий код:", reply_markup=cancel_kb())


@router.message(RequestForm.postal_code)
async def form_postal_code(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(location=message.text)
    await state.set_state(RequestForm.city)
    await message.answer("🏙️ Введіть місто/село:", reply_markup=cancel_kb())


@router.message(RequestForm.city)
async def form_city(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    data = await state.get_data()
    await state.update_data(location=f"{data.get('location', '')} {message.text}")
    await state.set_state(RequestForm.street)
    await message.answer("🛣️ Введіть вулицю:", reply_markup=cancel_kb())


@router.message(RequestForm.street)
async def form_street(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    data = await state.get_data()
    await state.update_data(location=f"{data.get('location', '')}, {message.text}")
    await state.set_state(RequestForm.house_number)
    await message.answer("🏠 Введіть номер будинку:", reply_markup=cancel_kb())


@router.message(RequestForm.house_number)
async def form_house_number(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    data = await state.get_data()
    await state.update_data(location=f"{data.get('location', '')} {message.text}")
    await state.set_state(RequestForm.phone)
    await message.answer("📱 Контактний номер телефону?", reply_markup=phone_kb())


@router.message(RequestForm.phone)
async def form_phone(message: Message, state: FSMContext):
    phone = message.contact.phone_number if message.contact else message.text
    await state.update_data(phone=phone)
    await state.set_state(RequestForm.has_baggage)
    await message.answer("🎒 Чи буде з вами багаж?", reply_markup=baggage_choice_kb())


@router.message(RequestForm.has_baggage)
async def form_has_baggage(message: Message, state: FSMContext):
    if "Так" in message.text:
        await state.update_data(has_baggage=True)
        await state.set_state(RequestForm.baggage_quantity)
        await message.answer("📦 Скільки одиниць багажу?", reply_markup=cancel_kb())
    elif "Ні" in message.text:
        await state.update_data(has_baggage=False)
        await state.set_state(RequestForm.destination)
        await message.answer("🏁 Пункт призначення:", reply_markup=cancel_kb())
    else:
        await message.answer("❌ Обберіть з кнопок!")


@router.message(RequestForm.baggage_quantity)
async def form_baggage_quantity(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(dimensions=message.text)
    await state.set_state(RequestForm.weight)
    await message.answer("⚖️ Вага багажу (кг):", reply_markup=cancel_kb())


@router.message(RequestForm.weight)
async def form_weight(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(weight=message.text)
    await state.set_state(RequestForm.destination)
    await message.answer("🏁 Пункт призначення:", reply_markup=cancel_kb())


@router.message(RequestForm.destination)
async def form_destination(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(destination=message.text)
    data = await state.get_data()
    direction = data.get("direction", "ukraine")
    await state.set_state(RequestForm.departure_date)
    await message.answer("📅 Виберіть дату відправлення:", reply_markup=date_choice_kb(direction))


@router.callback_query(F.data.startswith("date:"))
async def form_departure_date(query: CallbackQuery, state: FSMContext, bot: Bot):
    date_str = query.data.split(":", 1)[1]
    await state.update_data(departure_date=date_str)
    await finalize_request(query.message, state, query.from_user, bot)
    await query.answer()


async def finalize_request(message: Message, state: FSMContext, user, bot: Bot):
    data = await state.get_data()
    req_id = await save_request(data)
    direction_label = COUNTRY_LABELS.get(data.get("direction"), data.get("direction"))

    summary = (
        f"✅ Заявка #{req_id} зареєстрована!\n\n"
        f"<b>Напрямок:</b> {direction_label}\n"
        f"<b>Адреса:</b> {data.get('location', 'N/A')}\n"
        f"<b>Пункт призначення:</b> {data.get('destination', 'N/A')}\n"
        f"<b>Дата:</b> {data.get('departure_date', 'N/A')}\n"
        f"<b>Багаж:</b> {'Так' if data.get('has_baggage') else 'Ні'}"
    )

    await message.answer(summary, parse_mode="HTML", reply_markup=type_choice_kb())

    admin_msg = (
        f"🚖 <b>Нова заявка на пасажирські перевезення #{req_id}</b>\n\n"
        f"<b>Користувач:</b> @{data.get('username', 'невідомо')} ({user.id})\n"
        f"<b>Напрямок:</b> {direction_label}\n"
        f"<b>Адреса:</b> {data.get('location', 'N/A')}\n"
        f"<b>Пункт призначення:</b> {data.get('destination', 'N/A')}\n"
        f"<b>Дата:</b> {data.get('departure_date', 'N/A')}\n"
        f"<b>Телефон:</b> {data.get('phone', 'N/A')}"
    )

    for admin_id in ADMIN_CHAT_IDS:
        try:
            await bot.send_message(admin_id, admin_msg, parse_mode="HTML", reply_markup=admin_status_kb(req_id))
        except Exception as e:
            logger.error(f"❌ Не вдалось надіслати повідомлення адміну {admin_id}: {e}")

    await state.clear()


# ------------------------------------------------------------------
# ПОСИЛКИ
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("parcel_country:"))
async def parcel_country_choice(query: CallbackQuery, state: FSMContext):
    country = query.data.split(":", 1)[1]
    await state.update_data(parcel_country=country)
    await state.set_state(RequestForm.parcel_city_choice)
    await query.message.edit_text("🏙️ Виберіть місто:", reply_markup=parcel_city_kb())
    await query.answer()


@router.callback_query(F.data.startswith("parcel_city:"))
async def parcel_city_choice(query: CallbackQuery, state: FSMContext):
    city = query.data.split(":", 1)[1]
    if city == "other":
        await state.set_state(RequestForm.not_found_city)
        await query.message.edit_text("🏙️ Введіть назву вашого міста:")
    else:
        await state.update_data(parcel_city=city)
        await state.set_state(RequestForm.parcel_phone)
        await query.message.edit_text("📱 Контактний номер:", reply_markup=phone_kb())
    await query.answer()


@router.message(RequestForm.not_found_city)
async def not_found_city(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(parcel_city=message.text)
    await state.set_state(RequestForm.parcel_phone)
    await message.answer("📱 Контактний номер:", reply_markup=phone_kb())


@router.message(RequestForm.parcel_phone)
async def parcel_phone_input(message: Message, state: FSMContext, bot: Bot):
    phone = message.contact.phone_number if message.contact else message.text
    data = await state.get_data()
    country = data.get("parcel_country", "ukraine")
    await state.update_data(phone=phone)

    if country == "germany":
        await state.set_state(RequestForm.parcel_de_postal_code)
        await message.answer("📮 Поштовий код у Німеччині:", reply_markup=cancel_kb())
    else:
        req_id = await save_not_found_request({
            "user_id": message.from_user.id,
            "username": message.from_user.username,
            "req_type": "parcel",
            "direction": country,
            "location": data.get("parcel_city", "unknown"),
            "phone": phone,
        })

        await message.answer(f"✅ Заявка #{req_id} зареєстрована!", reply_markup=type_choice_kb())

        for admin_id in ADMIN_CHAT_IDS:
            try:
                await bot.send_message(
                    admin_id,
                    f"📦 <b>Нова заявка на посилку #{req_id}</b>\n\n"
                    f"<b>Користувач:</b> @{message.from_user.username} ({message.from_user.id})\n"
                    f"<b>Місто:</b> {data.get('parcel_city', 'N/A')}\n"
                    f"<b>Телефон:</b> {phone}",
                    parse_mode="HTML",
                    reply_markup=admin_status_kb(req_id)
                )
            except Exception as e:
                logger.error(f"❌ Помилка надсилання адміну: {e}")

        await state.clear()


@router.message(RequestForm.parcel_de_postal_code)
async def parcel_de_postal_code(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(parcel_de_postal_code=message.text)
    await state.set_state(RequestForm.parcel_de_city)
    await message.answer("🏙️ Місто:", reply_markup=cancel_kb())


@router.message(RequestForm.parcel_de_city)
async def parcel_de_city(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(parcel_de_city=message.text)
    await state.set_state(RequestForm.parcel_de_street)
    await message.answer("🛣️ Вулиця:", reply_markup=cancel_kb())


@router.message(RequestForm.parcel_de_street)
async def parcel_de_street(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(parcel_de_street=message.text)
    await state.set_state(RequestForm.parcel_de_house_number)
    await message.answer("🏠 Номер дому:", reply_markup=cancel_kb())


@router.message(RequestForm.parcel_de_house_number)
async def parcel_de_house_number(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(parcel_de_house_number=message.text)
    await state.set_state(RequestForm.parcel_de_quantity)
    await message.answer("📦 Кількість посилок:", reply_markup=cancel_kb())


@router.message(RequestForm.parcel_de_quantity)
async def parcel_de_quantity(message: Message, state: FSMContext):
    if await is_too_long(message):
        return
    await state.update_data(parcel_de_quantity=message.text)
    await state.set_state(RequestForm.parcel_de_weight)
    await message.answer("⚖️ Загальна вага (кг):", reply_markup=cancel_kb())


@router.message(RequestForm.parcel_de_weight)
async def parcel_de_weight(message: Message, state: FSMContext, bot: Bot):
    if await is_too_long(message):
        return
    await state.update_data(parcel_de_weight=message.text)
    data = await state.get_data()

    address = f"{data.get('parcel_de_postal_code', '')}, {data.get('parcel_de_city', '')}, {data.get('parcel_de_street', '')}, {data.get('parcel_de_house_number', '')}"

    req_id = await save_request({
        "user_id": message.from_user.id,
        "username": message.from_user.username,
        "req_type": "parcel",
        "direction": "germany",
        "location": data.get("parcel_de_city", "N/A"),
        "address": address,
        "phone": data.get("phone", "N/A"),
        "dimensions": data.get("parcel_de_quantity", "N/A"),
        "weight": data.get("parcel_de_weight", "N/A"),
        "destination": "Україна",
        "departure_date": None,
    })

    await message.answer(f"✅ Заявка #{req_id} на посилку з Німеччини зареєстрована!", reply_markup=type_choice_kb())

    for admin_id in ADMIN_CHAT_IDS:
        try:
            await bot.send_message(
                admin_id,
                f"📦 <b>Нова заявка на посилку з Німеччини #{req_id}</b>\n\n"
                f"<b>Користувач:</b> @{message.from_user.username} ({message.from_user.id})\n"
                f"<b>Адреса в Німеччині:</b> {address}\n"
                f"<b>Кількість:</b> {data.get('parcel_de_quantity', 'N/A')}\n"
                f"<b>Вага:</b> {data.get('parcel_de_weight', 'N/A')} кг\n"
                f"<b>Телефон:</b> {data.get('phone', 'N/A')}",
                parse_mode="HTML",
                reply_markup=admin_status_kb(req_id)
            )
        except Exception as e:
            logger.error(f"❌ Помилка надсилання адміну: {e}")

    await state.clear()


# ------------------------------------------------------------------
# СТАТУСИ ТА АДМІН-КОМАНДИ
# ------------------------------------------------------------------

@router.callback_query(F.data.startswith("st:"))
async def admin_set_status(query: CallbackQuery):
    if query.from_user.id not in ADMIN_CHAT_IDS:
        await query.answer("❌ Доступ заборонено", show_alert=True)
        return

    parts = query.data.split(":")
    req_id = int(parts[1])
    new_status = parts[2]

    try:
        await set_status(req_id, new_status, changed_by=f"admin_{query.from_user.id}")
        await query.answer(f"✅ Статус заявки #{req_id} змінено на '{new_status}'", show_alert=True)
        await query.message.edit_reply_markup(reply_markup=None)
    except Exception as e:
        logger.error(f"❌ Помилка: {e}")
        await query.answer("❌ Помилка при оновленні статусу", show_alert=True)


# ------------------------------------------------------------------
# ЗАПУСК
# ------------------------------------------------------------------

async def main():
    await init_db()
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)
    dp.update.middleware(ThrottlingMiddleware())

    bot = Bot(token=BOT_TOKEN)
    try:
        logger.info("🚀 Бот запущено!")
        await dp.start_polling(bot, allowed_updates=dp.resolve_used_update_types())
    finally:
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())