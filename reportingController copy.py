# reportingController.py

import asyncio
import csv
from datetime import datetime, time, timedelta
from aiogram import Bot, types

import io
import urllib.request

CHAT_ID = -1002399248368  # Идентификатор чата
CHAT_THREAD_ID = {
    'Казань': 745,
    'Новосибирск': 747,
    'Краснодарск': 746,
}

def fetch_csv():
    url = "https://docs.google.com/spreadsheets/d/1NRGPRwpMyXTe9LhS4adwfPo7nyx68GqweYdAdqo3LpM/export?format=csv&gid=1265864442"
    resp = urllib.request.urlopen(url)
    return list(csv.DictReader(io.TextIOWrapper(resp, 'utf-8')))

def detect_city(status_text):
    status_text = status_text.lower()

    CITIES = {
        "Казань": ["казань", "казани"],
        "Новосибирск": ["новосибирск", "новосибирске"],
        "Краснодарск": ["краснодарск", "краснодаре"],
        "Самара": ["самара", "самаре"],
        "Екатеринбург": ["екатеринбург", "екатеринбурге"],
        "Москва (Дзержинский)": ["москва", "москве", "дзержинский"],
    }

    for city_name, patterns in CITIES.items():
        for pattern in patterns:
            if pattern in status_text:
                return city_name
    return None


def build_reports():
    avail = {}
    route = {}

    # Описываем возможные варианты для каждого города (гибкость в статусах)
    CITY_PATTERNS = {
        "Казань": ["казань", "казани"],
        "Новосибирск": ["новосибирск", "новосибирске"],
        "Краснодарск": ["краснодарск", "краснодаре"],
        "Самара": ["самара", "самаре"],
        "Екатеринбург": ["екатеринбург", "екатеринбурге"],
        "Москва (Дзержинский)": ["москва", "москве", "дзержинский"],
    }

    def detect_city(status_text):
        status_text = status_text.lower()
        for city_name, patterns in CITY_PATTERNS.items():
            for pattern in patterns:
                if pattern in status_text:
                    return city_name
        return None

    for row in fetch_csv():
        row = {k.strip(): v.strip() for k, v in row.items()}

        # Пропускаем товары с Кол = 0
        if row.get('Кол') in {'0', '0.0', '', None}:
            continue

        status = row['Статус'].lower()
        name = row['Название']
        link = row['Ссылка']
        description = row['Описание']
        reviews_link = row['Отзывы по модели']
        arrival = row['Прибытие']

        # Готовим текст
        if link:
            text = f"<a href='{link}'>{name}</a> {description}"
        else:
            text = f"{name} {description}"

        if reviews_link:
            text += f" <a href='{reviews_link}'>Отзывы</a>"

        # Определяем город
        city = detect_city(status)
        if not city:
            continue  # Город не определён — пропускаем
        print(city, status)

        # Инициализируем множества если ещё нет
        if city not in avail:
            avail[city] = set()
        if city not in route:
            route[city] = set()

        # Добавляем в "наличие" или "в пути"
        if "наличии" in status:
            print(city)
            avail[city].add(text)
        elif "пути" in status:
            if arrival and f"Прибытие {arrival}" not in text:
                text += f"\nПрибытие {arrival}"
            route[city].add(text)

    # Конвертируем множества в списки
    return {
        "avail": {k: list(v) for k, v in avail.items()},
        "route": {k: list(v) for k, v in route.items()}
    }


def format_report(rows):
    if not rows:
        return "Здесь пока пусто"
    return "\n\n".join(rows)

def get_inline_keyboard():
    return types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Отчет Наличие", callback_data="report_avail")],
        [types.InlineKeyboardButton(text="Отчет В пути", callback_data="report_route")]
    ])

def get_report(city: str, mode: str) -> str:
    reports = build_reports()
    if city not in reports['avail']:
        return "Город не найден"

    if mode.lower() == "Наличии".lower():
        return format_report(reports['avail'][city])
    elif mode.lower() == "В пути".lower():
        return format_report(reports['route'][city])
    else:
        return "Некорректный режим. Используйте 'Наличие' или 'В пути'."


async def daily_job(bot: Bot):
    test = True  # для тестов ставим True чтобы сразу отправить

    if not test:
        while True:
            now = datetime.now()
            next_run = now.replace(hour=9, minute=0, second=0, microsecond=0)
            if now >= next_run:
                next_run += timedelta(days=1)

            await asyncio.sleep((next_run - now).total_seconds())

            await send_reports(bot)
    else:
        await send_reports(bot)

async def send_report_in_batches(bot: Bot, city, tid, title, items,
                                 batch_size: int = 10, delay: int = 20):
    if not items:
        # await bot.send_message(
        #     CHAT_ID,
        #     f"<b>{title}</b>\n\nЗдесь пока пусто",
        #     parse_mode="HTML",
        #     message_thread_id=tid
        # )
        return

    for i in range(0, len(items), batch_size):
        batch = items[i:i + batch_size]
        text = (f"<b>{title}</b>\n\n" + "\n\n".join(batch)) if i == 0 \
               else "\n\n".join(batch)

        await bot.send_message(
            CHAT_ID,
            text,
            parse_mode="HTML",
            message_thread_id=tid
        )

        # пауза только между порциями
        if i + batch_size < len(items):
            await asyncio.sleep(delay)

async def send_city_reports(bot: Bot, city, tid, avail, route):
    # Сначала "В наличии"
    await send_report_in_batches(bot, city, tid, f"Склад ({city}):", avail)
    
    # Потом "В пути"
    await send_report_in_batches(bot, city, tid, f"В пути в ({city}):", route)

async def send_reports(bot: Bot, batch_size: int = 0, delay: int = 20):
    reports = build_reports()

    for city, tid in CHAT_THREAD_ID.items():
        # 1) заголовок
        await bot.send_message(
            CHAT_ID,
            f"<b>Отчёты по складу ({city})</b>",
            parse_mode="HTML",
            message_thread_id=tid
        )

        # 2) Наличие — всё сразу
        avail = reports['avail'].get(city, [])
        text = "Наличие:\n\n" + ("\n\n".join(avail) if avail else "Здесь пока пусто")
        await bot.send_message(
            CHAT_ID,
            text,
            parse_mode="HTML",
            message_thread_id=tid
        )

        # 3) В пути — всё сразу
        route = reports['route'].get(city, [])
        text = "В пути:\n\n" + ("\n\n".join(route) if route else "Здесь пока пусто")
        await bot.send_message(
            CHAT_ID,
            text,
            parse_mode="HTML",
            message_thread_id=tid
        )

        # 4) пауза перед следующим городом
        await asyncio.sleep(delay)
