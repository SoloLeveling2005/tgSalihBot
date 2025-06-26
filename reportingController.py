import asyncio
import csv
import io
import json
import os
import random
import re
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from dotenv import load_dotenv
from aiogram import Bot, types
import pandas as pd

load_dotenv()

CHAT_ID = os.getenv("CHAT_ID")
CHAT_PUBLIC_ID =  745

# === Работа с городами и его идентификации ===

BEGIN_PUBLICATION_CELL = "N18"
FINISH_PUBLICATION_CELL = "N19"

# Массив локаций и их данные
LOCATIONS = {
    "kazan": {
        "ru": "Казань",
        "variants_ru": ["казан", "казани"],
        "exel": {
            "intro": "N2",
            "outro": "N3",
        }
    },
    "novosibirsk": {
        "ru": "Новосибирск",
        "variants_ru": ["новосиб", "новосибир", "новосибирске"],
        "exel": {
            "intro": "O2",
            "outro": "O3",
        }
    },
    "samara": {
        "ru": "Самара",
        "variants_ru": ["самар", "самаре"],
        "exel": {
            "intro": "P2",
            "outro": "P3",
        }
    },
    "krasnodar": {
        "ru": "Краснодар",
        "variants_ru": ["краснод", "краснодаре"],
        "exel": {
            "intro": "Q2",
            "outro": "Q3",
        }
    },
    "ekaterinburg": {
        "ru": "Екатеринбург",
        "variants_ru": ["екатерен", "екатерин"],
        "exel": {
            "intro": "R2",
            "outro": "R3",
        }
    },
    "moscow_dzerzhinsky": {
        "ru": "Москва (Дзержинский)",
        "variants_ru": ["москва (дзержинский)"],
        "exel": {
            "intro": "S2",
            "outro": "S3",
        }
    }
}

def detect_location_slug(text: str) -> str | None:
    """Определяет, упоминается ли какой-либо место в строке (в любом месте)."""
    text = text.lower()
    for slug, cfg in LOCATIONS.items():
        if any(v in text for v in cfg["variants_ru"]):
            return slug
    return None

# === ===

# === Хранение message_id для редактирования сообщений после перезапуска ===

REPORT_FILE = Path("report_data.json")

def load_report_data() -> dict[str, int]:
    """Загружает message_id по каждому городу из файла."""
    if REPORT_FILE.exists():
        return json.loads(REPORT_FILE.read_text(encoding="utf-8"))
    return {}

def save_report_data(data: dict[str, int]) -> None:
    """Сохраняет message_id по каждому городу в файл."""
    REPORT_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

# === ===

# === Парсинг данных с эксель таблицы ===

def fetch_csv_df() -> pd.DataFrame:
    url = (
        "https://docs.google.com/spreadsheets/"
        "d/1NRGPRwpMyXTe9LhS4adwfPo7nyx68GqweYdAdqo3LpM/"
        "export?format=csv&gid=1265864442"
    )
    try:
        with urllib.request.urlopen(url) as resp:
            if resp.status != 200:
                raise Exception(f"Ошибка запроса: {resp.status}")
            data = io.BytesIO(resp.read())
            df = pd.read_csv(data, encoding='utf-8-sig', header=None)
            df = df.where(pd.notna(df), None)  # ← заменяет все NaN на None

            # Генерация буквенных заголовков: A, B, ..., Z, AA, AB, ...
            def colname(n):
                name = ""
                while n >= 0:
                    name = chr(n % 26 + 65) + name
                    n = n // 26 - 1
                return name

            df.columns = [colname(i) for i in range(len(df.columns))]
            return df
    except Exception as e:
        print(f"Ошибка при загрузке CSV: {e}")
        return pd.DataFrame()
    
def get_excel_cell_value(df: pd.DataFrame, cell: str):
    """Получение данных с указанной ячейки."""

    def split_cell(cell: str) -> tuple[str, int]:
        match = re.fullmatch(r'([A-Za-z]+)(\d+)', cell.strip())
        if not match:
            raise ValueError(f"Некорректный формат ячейки: {cell}")
        return match.group(1), int(match.group(2)) - 1

    column, row = split_cell(cell)

    if column not in df.columns:
        raise KeyError(f"Колонка '{column}' не найдена в DataFrame")

    if row >= len(df):
        raise IndexError(f"Строка {row+1} вне диапазона")

    return df[column].iloc[row]

# === ===


def parse_stock_data_from_csv(df: pd.DataFrame) -> dict[str, dict[str, list[dict]]]:
    # Найти строку с заголовками
    header_idx = None
    for i, row in df.iterrows():
        vals = row.astype(str).str.lower().tolist()
        if "склад" in vals and "статус" in vals:
            header_idx = i
            break
    if header_idx is None:
        raise ValueError("Не найдена строка с заголовками")

    # Вынести заголовки и сформировать датафрейм с данными
    headers = df.iloc[header_idx].tolist()
    data = df.iloc[header_idx+1 : ].copy().reset_index(drop=True)
    data.columns = headers

    result: dict[str, dict[str, list[dict]]] = {}
    for _, row in data.iterrows():
        city   = detect_location_slug(str(row.get("Склад", "")).strip())
        status = str(row.get("Статус", "")).lower().strip()
        count  = str(row.get("Кол", "")).strip()

        if not city or not status or count in {"", "0", "0.0"}:
            continue

        def safe_str(val) -> str:
            return str(val).strip() if val is not None else ""

        item = {
            "name":        safe_str(row.get("Название")),
            "link":        safe_str(row.get("Ссылка")),
            "desc":        safe_str(row.get("Описание")),
            "count":       safe_str(row.get("Кол")),
            "images":      [u.strip() for u in re.split(r"[,\s]+", safe_str(row.get("Картинки"))) if u.strip()],
            "reviews":     safe_str(row.get("Отзывы по модели")),
            "price_avail": safe_str(row.get("Цена из наличия")),
            "price_order": safe_str(row.get("Цена под заказ")),
            "link_order":  safe_str(row.get("Под заказ")),
            "arrival":     safe_str(row.get("Прибытие")),
        }

        if "наличи" in status:
            status_en = "availability"
        elif "пути" in status:
            status_en = "onTheWay"
        else:
            continue

        result.setdefault(city, {
            "availability": {"list": []},
            "onTheWay":     {"list": []},
            "intro": "", "outro": ""
        })
        result[city][status_en]["list"].append(item)

        # Собираем текст до, в начале, в конце и после, для публикаций.
        result[city]["intro"] = safe_str(get_excel_cell_value(df=df, cell=LOCATIONS[city]["exel"]["intro"]))
        result[city]["outro"] = safe_str(get_excel_cell_value(df=df, cell=LOCATIONS[city]["exel"]["outro"]))

    return result


class Mark2:
    @staticmethod
    def escape(text: str) -> str:
        return re.sub(r'([_*\[\]()~>#+=|{}.!\\-])', r'\\\1', text)

    @classmethod
    def bold(cls, text: str) -> str:
        return f"*{cls.escape(text)}*"

    @classmethod
    def italic(cls, text: str) -> str:
        return f"_{cls.escape(text)}_"

    @classmethod
    def link(cls, text: str, url: str) -> str:
        return f"[{cls.escape(text)}]({cls.escape(url)})"


async def publish_reports(bot: Bot) -> None:
    # сбросим старые IDs, чтобы сделать «новую публикацию»
    save_report_data({})
    await update_reports(bot)

def text_new_line(existing: str, addition: str) -> str:
        """Добавляет строку к существующей с двумя переносами, если обе непустые."""
        if not addition.strip():
            return existing
        if not existing.strip():
            return addition
        return f"{existing}\n\n{addition}"

def split_text_safe(text: str, limit: int = 1024) -> list[str]:
    """Разделяет текст на части, не обрывая строки, не превышая лимит."""
    lines = text.splitlines(keepends=True)
    parts = []
    current = ""
    for line in lines:
        if len(current) + len(line) > limit:
            parts.append(current.rstrip())
            current = line
        else:
            current += line
    if current.strip():
        parts.append(current.rstrip())
    return parts

async def update_reports(bot: Bot, type_='create') -> None:
    df = fetch_csv_df()
    report_data = parse_stock_data_from_csv(df)
    emojis = ['🚀🚀🚀🚀🚀🚀', '🔥🔥🔥🔥🔥🔥']
    thread_id = CHAT_PUBLIC_ID

    begin_text = get_excel_cell_value(df, BEGIN_PUBLICATION_CELL)
    finish_text = get_excel_cell_value(df, FINISH_PUBLICATION_CELL)

    if begin_text:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=Mark2.escape(begin_text),
            parse_mode="MarkdownV2",
            message_thread_id=thread_id
        )
        await bot.send_message(
            chat_id=CHAT_ID,
            text=random.choice(emojis),
            parse_mode="MarkdownV2",
            message_thread_id=thread_id
        )

    for idx, (slug, cfg) in enumerate(LOCATIONS.items(), start=1):
        thread_id = CHAT_PUBLIC_ID
        city_name = cfg["ru"]
        intro = report_data[slug]["intro"]
        outro = report_data[slug]["outro"]

        intro_part = f"\n\n{intro}" if intro else ""
        parts = [Mark2.bold(f"Отчёт по складу ({city_name}){intro_part}")]
        images = []

        for section, title in (("availability", "Наличие:"), ("onTheWay", "В пути:")):
            items = report_data[slug][section]["list"]
            if not items:
                continue
            parts.append(Mark2.bold(title))
            for item in items:
                name = item["name"]
                link = item["link"]
                desc = item["desc"]
                reviews = item["reviews"]
                price_avail = item["price_avail"]
                price_order = item["price_order"]
                link_order = item["link_order"]
                arrival = item["arrival"]
                images.extend(item.get("images", []))

                if link:
                    line = Mark2.link(name, link)
                else:
                    line = Mark2.escape(name)
                    
                if desc:
                    line += f" {Mark2.escape(desc)}"

                if reviews:
                    line += f" {Mark2.link('Отзывы', reviews)}"

                if price_avail:
                    line += f" Цена {Mark2.escape(price_avail)}"

                if link_order and price_order:
                    line += f" {Mark2.link(f'Под заказ {price_order}', link_order)}"
                elif link_order:
                    line += f" {Mark2.link('Под заказ', link_order)}"
                elif price_order:
                    line += f" Под заказ {Mark2.escape(price_order)}"

                if arrival:
                    line += f"\n{Mark2.escape(f'Прибытие {arrival}')}"

                parts.append(line)

        parts.append(Mark2.escape(outro))
        full_text = "\n\n".join(parts)

        if images:
            caption_chunk, *rest_chunks = split_text_safe(full_text, limit=1024)
            media_group = [
                types.InputMediaPhoto(media=images[0], caption=caption_chunk, parse_mode="MarkdownV2")
            ]
            for url in images[1:10]:
                media_group.append(types.InputMediaPhoto(media=url))
            await bot.send_media_group(
                chat_id=CHAT_ID,
                media=media_group,
                message_thread_id=thread_id
            )

            # пересобрать остаток текста в один блок и порезать по 4096
            remaining_text = "\n".join(rest_chunks)
            text_chunks = split_text_safe(remaining_text, limit=4096)
        else:
            text_chunks = split_text_safe(full_text, limit=4096)

        for chunk in text_chunks:
            await bot.send_message(
                chat_id=CHAT_ID,
                text=chunk,
                parse_mode="MarkdownV2",
                message_thread_id=thread_id
            )
            await asyncio.sleep(0.5)

        if idx < len(LOCATIONS):
            await bot.send_message(
                chat_id=CHAT_ID,
                text=random.choice(emojis),
                parse_mode="MarkdownV2",
                message_thread_id=thread_id
            )

    if finish_text:
        await bot.send_message(
            chat_id=CHAT_ID,
            text=Mark2.escape(finish_text),
            parse_mode="MarkdownV2",
            message_thread_id=thread_id
        )


# для совместимости: если где-то ещё зовётся send_reports
async def send_reports(bot: Bot):
    await update_reports(bot, type_='create')

# if __name__ == "__main__":
#     asyncio.run(update_reports(bot=None))