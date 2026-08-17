import logging
import asyncio
import sqlite3
from datetime import datetime, timedelta
from aiogram import F, Router, types
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from aiogram.filters.state import StateFilter
from aiogram.utils.keyboard import InlineKeyboardBuilder
from google.auth.transport.requests import Request
from google.oauth2.service_account import Credentials
from google.auth.exceptions import RefreshError
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
import json

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

router = Router()

DB_NAME = "globextrans.db"
GOOGLE_CREDENTIALS_FILE = "service_account.json"
CALENDAR_ID = "primary"

FIXED_DEPARTURE_DATES = {
    "ukraine": [
        "2024-11-25",
        "2024-12-02",
        "2024-12-09",
        "2024-12-16",
    ],
    "germany": [
        "2024-11-20",
        "2024-11-27",
        "2024-12-04",
        "2024-12-11",
    ]
}

def get_db_connection():
    conn = sqlite3.connect(DB_NAME)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            username TEXT,
            direction TEXT,
            location TEXT,
            phone TEXT,
            has_baggage BOOLEAN,
            baggage_quantity INTEGER,
            weight REAL,
            destination TEXT,
            departure_date TEXT,
            status TEXT DEFAULT 'pending',
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS status_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            request_id INTEGER,
            status TEXT,
            changed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (request_id) REFERENCES requests(id)
        )
    ''')
    conn.commit()
    conn.close()

def get_google_calendar_service():
    try:
        credentials = Credentials.from_service_account_file(
            GOOGLE_CREDENTIALS_FILE,
            scopes=['https://www.googleapis.com/auth/calendar']
        )
        return build('calendar', 'v3', credentials=credentials)
    except Exception as e:
        logger.error(f"Error initializing Google Calendar service: {e}")
        return None

def check_calendar_availability(departure_date, direction):
    service = get_google_calendar_service()
    if not service:
        return True

    try:
        start_of_day = f"{departure_date}T00:00:00+02:00"
        end_of_day = f"{departure_date}T23:59:59+02:00"

        events = service.events().list(
            calendarId=CALENDAR_ID,
            timeMin=start_of_day,
            timeMax=end_of_day,
            singleEvents=True,
            orderBy='startTime'
        ).execute()

        for event in events.get('items', []):
            if direction.lower() in event.get('summary', '').lower():
                available_seats = int(event.get('description', '0') or '0')
                return available_seats > 0

        return True
    except Exception as e:
        logger.error(f"Error checking calendar availability: {e}")
        return True

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
    parcel_country = State()
    parcel_city = State()
    parcel_phone = State()
    parcel_from_city = State()
    parcel_from_country = State()
    parcel_weight = State()
    parcel_description = State()
    not_found_city = State()
    not_found_phone = State()

def main_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🚌 Пасажир"), KeyboardButton(text="📦 Посилка")],
            [KeyboardButton(text="ℹ️ Про нас"), KeyboardButton(text="📞 Контакти")]
        ],
        resize_keyboard=True
    )
    return keyboard

def direction_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🇺🇦 Україна → 🇩🇪 Німеччина")],
            [KeyboardButton(text="🇩🇪 Німеччина → 🇺🇦 Україна")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )
    return keyboard

def yes_no_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="Так"), KeyboardButton(text="Ні")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )
    return keyboard

def get_destinations(direction):
    if direction == "ukraine":
        return ["Київ", "Львів", "Харків", "Одеса", "Інше"]
    else:
        return ["Берлін", "Мюнхен", "Кельн", "Гамбург", "Інше"]

def destinations_keyboard(direction):
    destinations = get_destinations(direction)
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=dest)] for dest in destinations],
        resize_keyboard=True
    )
    return keyboard

def departure_dates_keyboard(direction):
    dates = FIXED_DEPARTURE_DATES.get(direction, [])
    keyboard = ReplyKeyboardMarkup(
        keyboard=[[KeyboardButton(text=date)] for date in dates],
        resize_keyboard=True
    )
    return keyboard

def parcel_countries_keyboard():
    keyboard = ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🇺🇦 Україна")],
            [KeyboardButton(text="🇩🇪 Німеччина")],
            [KeyboardButton(text="🔙 Назад")]
        ],
        resize_keyboard=True
    )
    return keyboard

@router.message(F.text == "🚌 Пасажир")
async def passenger_start(message: types.Message, state: FSMContext):
    await message.answer(
        "🚌 Обереть напрямок подорожі:",
        reply_markup=direction_keyboard()
    )
    await state.set_state(RequestForm.direction)

@router.message(RequestForm.direction, F.text.in_(["🇺🇦 Україна → 🇩🇪 Німеччина", "🇩🇪 Німеччина → 🇺🇦 Україна"]))
async def form_direction(message: types.Message, state: FSMContext):
    direction = "ukraine" if "Україна" in message.text and "Німеччина" in message.text else "germany"
    await state.update_data(direction=direction)
    await message.answer(
        "✉️ Введіть поштовий код:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.postal_code)

@router.message(RequestForm.postal_code)
async def form_postal_code(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🚌 Обереть напрямок подорожі:", reply_markup=direction_keyboard())
        await state.set_state(RequestForm.direction)
        return

    await state.update_data(location=message.text)
    await message.answer(
        "🏙️ Введіть місто:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.city)

@router.message(RequestForm.city)
async def form_city(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("✉️ Введіть поштовий код:")
        await state.set_state(RequestForm.postal_code)
        return

    data = await state.get_data()
    location = f"{data['location']} {message.text},"
    await state.update_data(location=location)
    await message.answer(
        "🛣️ Введіть вулицю:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.street)

@router.message(RequestForm.street)
async def form_street(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🏙️ Введіть місто:")
        await state.set_state(RequestForm.city)
        return

    data = await state.get_data()
    location = f"{data['location']} {message.text}"
    await state.update_data(location=location)
    await message.answer(
        "🏠 Введіть номер будинку:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.house_number)

@router.message(RequestForm.house_number)
async def form_house_number(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🛣️ Введіть вулицю:")
        await state.set_state(RequestForm.street)
        return

    data = await state.get_data()
    location = f"{data['location']} {message.text}"
    await state.update_data(location=location)
    await message.answer(
        "📞 Введіть номер телефону:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.phone)

@router.message(RequestForm.phone)
async def form_phone(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🏠 Введіть номер будинку:")
        await state.set_state(RequestForm.house_number)
        return

    await state.update_data(phone=message.text)
    await message.answer(
        "🎒 Чи буде багаж?",
        reply_markup=yes_no_keyboard()
    )
    await state.set_state(RequestForm.has_baggage)

@router.message(RequestForm.has_baggage, F.text.in_(["Так", "Ні"]))
async def form_has_baggage(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("📞 Введіть номер телефону:")
        await state.set_state(RequestForm.phone)
        return

    has_baggage = message.text == "Так"
    await state.update_data(has_baggage=has_baggage)

    if has_baggage:
        await message.answer(
            "🎒 Введіть кількість багажу:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="🔙 Назад")]],
                resize_keyboard=True
            )
        )
        await state.set_state(RequestForm.baggage_quantity)
    else:
        await state.update_data(baggage_quantity=0, weight=0)
        await message.answer(
            "🎯 Куди їдете?",
            reply_markup=destinations_keyboard(
                (await state.get_data())['direction']
            )
        )
        await state.set_state(RequestForm.destination)

@router.message(RequestForm.baggage_quantity)
async def form_baggage_quantity(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🎒 Чи буде багаж?", reply_markup=yes_no_keyboard())
        await state.set_state(RequestForm.has_baggage)
        return

    try:
        quantity = int(message.text)
        await state.update_data(baggage_quantity=quantity)
        await message.answer(
            "⚖️ Введіть вагу (в кг):",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="🔙 Назад")]],
                resize_keyboard=True
            )
        )
        await state.set_state(RequestForm.weight)
    except ValueError:
        await message.answer("❌ Будь ласка, введіть числове значення:")

@router.message(RequestForm.weight)
async def form_weight(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🎒 Введіть кількість багажу:")
        await state.set_state(RequestForm.baggage_quantity)
        return

    try:
        weight = float(message.text)
        await state.update_data(weight=weight)
        await message.answer(
            "🎯 Куди їдете?",
            reply_markup=destinations_keyboard(
                (await state.get_data())['direction']
            )
        )
        await state.set_state(RequestForm.destination)
    except ValueError:
        await message.answer("❌ Будь ласка, введіть числове значення:")

@router.message(RequestForm.destination)
async def form_destination(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        data = await state.get_data()
        if data.get('has_baggage'):
            await message.answer("⚖️ Введіть вагу (в кг):")
            await state.set_state(RequestForm.weight)
        else:
            await message.answer("🎒 Чи буде багаж?", reply_markup=yes_no_keyboard())
            await state.set_state(RequestForm.has_baggage)
        return

    await state.update_data(destination=message.text)
    direction = (await state.get_data())['direction']
    await message.answer(
        "📅 Обереть дату відправлення:",
        reply_markup=departure_dates_keyboard(direction)
    )
    await state.set_state(RequestForm.departure_date)

@router.message(RequestForm.departure_date)
async def form_departure_date(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🎯 Куди їдете?", reply_markup=destinations_keyboard(
            (await state.get_data())['direction']
        ))
        await state.set_state(RequestForm.destination)
        return

    await state.update_data(departure_date=message.text)
    await finalize_request(message, state)

@router.message(F.text == "📦 Посилка")
async def parcel_start(message: types.Message, state: FSMContext):
    await message.answer(
        "📦 З якої країни відправляємо посилку?",
        reply_markup=parcel_countries_keyboard()
    )
    await state.set_state(RequestForm.parcel_country)

@router.message(RequestForm.parcel_country, F.text.in_(["🇺🇦 Україна", "🇩🇪 Німеччина"]))
async def parcel_country_selected(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Головне меню:", reply_markup=main_keyboard())
        await state.clear()
        return

    country = "ukraine" if "Україна" in message.text else "germany"
    await state.update_data(parcel_country=country)

    if country == "ukraine":
        await message.answer(
            "🏙️ Введіть місто відправлення (Україна):",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="🔙 Назад")]],
                resize_keyboard=True
            )
        )
        await state.set_state(RequestForm.parcel_city)
    else:
        await message.answer(
            "🏙️ Введіть місто відправлення (Німеччина):",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="🔙 Назад")]],
                resize_keyboard=True
            )
        )
        await state.set_state(RequestForm.parcel_from_city)

@router.message(RequestForm.parcel_city)
async def parcel_city_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("📦 З якої країни відправляємо посилку?", reply_markup=parcel_countries_keyboard())
        await state.set_state(RequestForm.parcel_country)
        return

    await state.update_data(location=message.text)
    await message.answer(
        "🎯 Введіть місто призначення (Німеччина):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.destination)

@router.message(RequestForm.destination, StateFilter(RequestForm.parcel_city))
async def parcel_destination_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🏙️ Введіть місто відправлення (Україна):")
        await state.set_state(RequestForm.parcel_city)
        return

    await state.update_data(destination=message.text)
    await message.answer(
        "📞 Введіть номер телефону:",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.parcel_phone)

@router.message(RequestForm.parcel_phone)
async def parcel_phone_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("🎯 Введіть місто призначення (Німеччина):")
        await state.set_state(RequestForm.destination)
        return

    await state.update_data(phone=message.text)
    await message.answer(
        "⚖️ Введіть вагу посилки (в кг):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.parcel_weight)

@router.message(RequestForm.parcel_weight)
async def parcel_weight_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("📞 Введіть номер телефону:")
        await state.set_state(RequestForm.parcel_phone)
        return

    try:
        weight = float(message.text)
        await state.update_data(weight=weight)
        await message.answer(
            "📝 Введіть опис посилки:",
            reply_markup=ReplyKeyboardMarkup(
                keyboard=[[KeyboardButton(text="🔙 Назад")]],
                resize_keyboard=True
            )
        )
        await state.set_state(RequestForm.parcel_description)
    except ValueError:
        await message.answer("❌ Будь ласка, введіть числове значення:")

@router.message(RequestForm.parcel_description)
async def parcel_description_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("⚖️ Введіть вагу посилки (в кг):")
        await state.set_state(RequestForm.parcel_weight)
        return

    await state.update_data(parcel_description=message.text)
    await finalize_request(message, state)

@router.message(RequestForm.parcel_from_city)
async def parcel_from_city_input(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("📦 З якої країни відправляємо посилку?", reply_markup=parcel_countries_keyboard())
        await state.set_state(RequestForm.parcel_country)
        return

    await state.update_data(location=message.text)
    await message.answer(
        "🎯 Введіть місто призначення (Україна):",
        reply_markup=ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text="🔙 Назад")]],
            resize_keyboard=True
        )
    )
    await state.set_state(RequestForm.destination)

async def finalize_request(message: types.Message, state: FSMContext):
    data = await state.get_data()
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute('''
        INSERT INTO requests (user_id, username, direction, location, phone, has_baggage, baggage_quantity, weight, destination, departure_date, status)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 'pending')
    ''', (
        message.from_user.id,
        message.from_user.username,
        data.get('direction') or data.get('parcel_country'),
        data.get('location'),
        data.get('phone'),
        data.get('has_baggage', False),
        data.get('baggage_quantity', 0),
        data.get('weight', 0),
        data.get('destination'),
        data.get('departure_date')
    ))

    request_id = cursor.lastrowid
    cursor.execute('INSERT INTO status_log (request_id, status) VALUES (?, ?)', (request_id, 'pending'))
    conn.commit()
    conn.close()

    await message.answer(
        "✅ Вашу заявку прийнято! Номер заявки: #{}".format(request_id),
        reply_markup=main_keyboard()
    )

    await state.clear()

    asyncio.create_task(notify_admin(message.bot, request_id, data))

async def notify_admin(bot, request_id, data):
    admin_id = 123456789
    message_text = (
        f"📋 Нова заявка #{request_id}\n"
        f"Користувач: @{data.get('username', 'unknown')}\n"
        f"ID: {data.get('user_id')}\n"
        f"Напрямок: {data.get('direction') or data.get('parcel_country')}\n"
        f"Локація: {data.get('location')}\n"
        f"Телефон: {data.get('phone')}\n"
        f"Багаж: {'Так' if data.get('has_baggage') else 'Ні'}\n"
        f"Кількість: {data.get('baggage_quantity')}\n"
        f"Вага: {data.get('weight')} кг\n"
        f"Призначення: {data.get('destination')}\n"
        f"Дата відправлення: {data.get('departure_date')}\n"
    )

    keyboard = InlineKeyboardMarkup(
        inline_keyboard=[
            [InlineKeyboardButton(text="✅ Прийняти", callback_data=f"accept_{request_id}"),
             InlineKeyboardButton(text="❌ Відхилити", callback_data=f"reject_{request_id}")]
        ]
    )

    try:
        await bot.send_message(admin_id, message_text, reply_markup=keyboard)
    except Exception as e:
        logger.error(f"Error sending admin notification: {e}")

@router.callback_query(F.data.startswith("accept_"))
async def accept_request(callback_query: types.CallbackQuery):
    request_id = int(callback_query.data.split("_")[1])
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE requests SET status = ? WHERE id = ?', ('accepted', request_id))
    cursor.execute('INSERT INTO status_log (request_id, status) VALUES (?, ?)', (request_id, 'accepted'))
    conn.commit()
    conn.close()

    await callback_query.answer("✅ Заявка прийнята")
    await callback_query.message.edit_text(f"✅ Заявка #{request_id} прийнята")

@router.callback_query(F.data.startswith("reject_"))
async def reject_request(callback_query: types.CallbackQuery):
    request_id = int(callback_query.data.split("_")[1])
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute('UPDATE requests SET status = ? WHERE id = ?', ('rejected', request_id))
    cursor.execute('INSERT INTO status_log (request_id, status) VALUES (?, ?)', (request_id, 'rejected'))
    conn.commit()
    conn.close()

    await callback_query.answer("❌ Заявка відхилена")
    await callback_query.message.edit_text(f"❌ Заявка #{request_id} відхилена")

@router.message(F.text == "🔙 Назад")
async def go_back(message: types.Message, state: FSMContext):
    await state.clear()
    await message.answer("Головне меню:", reply_markup=main_keyboard())

@router.message(F.text.in_(["ℹ️ Про нас", "📞 Контакти"]))
async def info(message: types.Message):
    if message.text == "ℹ️ Про нас":
        await message.answer(
            "🚌 GlobexTrans — сервіс перевезень між Україною та Німеччиною\n\n"
            "Ми забезпечуємо надійні та безпечні перевезення пасажирів та посилок."
        )
    else:
        await message.answer(
            "📞 Контакти:\n"
            "Телефон: +49 123 456789\n"
            "Email: info@globextrans.de"
        )