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
    "tomsk": {
        "ru": "Томск",
        "variants_ru": ["томск"],
        "exel": {
            "intro": "P2",
            "outro": "P3",
        }
    },
    "omsk": {
        "ru": "Омск",
        "variants_ru": ["омск"],
        "exel": {
            "intro": "Q2",
            "outro": "Q3",
        }
    },
    "barnaul": {
        "ru": "Барнаул",
        "variants_ru": ["барнаул"],
        "exel": {
            "intro": "R2",
            "outro": "R3",
        }
    },
    "cheboksary": {
        "ru": "Чебоксары",
        "variants_ru": ["чебокс"],
        "exel": {
            "intro": "S2",
            "outro": "S3",
        }
    }
}

def detect_location_slug(text: str) -> str | None:
    """Определяет локацию, если её вариант стоит в начале строки (без учёта регистра)."""
    text = text.lower().lstrip()
    for slug, cfg in LOCATIONS.items():
        if any(text.startswith(v) for v in cfg["variants_ru"]):
            return slug
    return None

# === ===

# === Хранение message_id для редактирования сообщений после перезапуска ===

REPORT_FILE = Path("report_data.json")
INIT_REPORT_DATA = {slug: [] for slug in LOCATIONS}

def load_report_data() -> dict[str, list[int]]:
    """Загружает message_id по каждому городу из файла, создаёт базу при отсутствии."""
    if REPORT_FILE.exists():
        try:
            data = json.loads(REPORT_FILE.read_text(encoding="utf-8"))
            # гарантируем нужную структуру
            return {slug: list(map(int, data.get(slug, []))) for slug in LOCATIONS}
        except Exception:
            pass
    return INIT_REPORT_DATA.copy()

def save_report_data(data: dict[str, list[int]]) -> None:
    """Сохраняет message_id по каждому городу в файл без дубликатов."""
    norm = {slug: sorted(set(data.get(slug, []))) for slug in LOCATIONS}
    REPORT_FILE.write_text(json.dumps(norm, ensure_ascii=False, indent=2), encoding="utf-8")


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

async def update_reports(
    message: types.Message | None,
    bot: Bot,
    type_: str = "create",
) -> None:
    """
    create  – публикуем новый отчёт, предварительно удаляя все старые сообщения  
    update  – логика обновления при необходимости (не реализована здесь)
    """
    # ---------- 1. Удаляем старые публикации ----------
    store = load_report_data()          # {slug: [...], "all": [...]}

    if type_ == "create":
        ids_to_delete: set[int] = {
            mid for lst in store.values() for mid in lst
        }
        if ids_to_delete:
            for mid in sorted(ids_to_delete, reverse=True):
                try:
                    await bot.delete_message(chat_id=CHAT_ID, message_id=mid)
                    await asyncio.sleep(0.05)        # бережём rate-limit
                except Exception:
                    pass                             # сообщение уже удалено/недоступно

            # обнуляем хранилище и сохраняем
            store = {slug: [] for slug in LOCATIONS}
            store["all"] = []
            save_report_data(store)

    # ---------- 2. Готовим данные ----------
    df         = fetch_csv_df()
    stock      = parse_stock_data_from_csv(df)
    begin_text = get_excel_cell_value(df, BEGIN_PUBLICATION_CELL)
    finish_text= get_excel_cell_value(df, FINISH_PUBLICATION_CELL)
    emojis     = ['🚀🚀🚀🚀🚀🚀', '🔥🔥🔥🔥🔥🔥']
    thread_id  = CHAT_PUBLIC_ID

    # ---------- 3. Публикация (type_ == "create") ----------
    if type_ == "create":
        # —– начало блока
        if begin_text:
            msg = await bot.send_message(
                CHAT_ID, Mark2.escape(begin_text),
                parse_mode="MarkdownV2", message_thread_id=thread_id
            )
            store["all"].append(msg.message_id)

            msg = await bot.send_message(
                CHAT_ID, random.choice(emojis),
                parse_mode="MarkdownV2", message_thread_id=thread_id
            )
            store["all"].append(msg.message_id)

        # —– города
        for idx, (slug, cfg) in enumerate(LOCATIONS.items(), start=1):
            city_name = cfg["ru"]
            intro     = stock[slug]["intro"]
            outro     = stock[slug]["outro"]

            parts: list[str] = [Mark2.bold(f"Отчёт по складу ({city_name})")]
            if intro:
                parts.append(Mark2.escape(intro))

            images: list[str] = []
            for section, title in (("availability", "Наличие:"), ("onTheWay", "В пути:")):
                items = stock[slug][section]["list"]
                if not items:
                    continue
                parts.append(Mark2.bold(title))
                for it in items:
                    line = Mark2.link(it["name"], it["link"]) if it["link"] else Mark2.escape(it["name"])
                    if it["desc"]:
                        line += f" {Mark2.escape(it['desc'])}"
                    if it["reviews"]:
                        line += f" {Mark2.link('Отзывы', it['reviews'])}"
                    if it["price_avail"]:
                        line += f" Цена {Mark2.escape(it['price_avail'])}"
                    if it["link_order"] and it["price_order"]:
                        line += f" {Mark2.link(f'Под заказ {it['price_order']}', it['link_order'])}"
                    elif it["link_order"]:
                        line += f" {Mark2.link('Под заказ', it['link_order'])}"
                    elif it["price_order"]:
                        line += f" Под заказ {Mark2.escape(it['price_order'])}"
                    if it["arrival"]:
                        line += f"\n{Mark2.escape(f'Прибытие {it['arrival']}')}"
                    parts.append(line)
                    images.extend(it["images"])

            if outro:
                parts.append(Mark2.escape(outro))

            full_text = "\n\n".join(parts)

            # --- публикация с картинками / без
            if images:
                cap, *rest = split_text_safe(full_text, 1024)
                media = [types.InputMediaPhoto(images[0], caption=cap, parse_mode="MarkdownV2")]
                media += [types.InputMediaPhoto(u) for u in images[1:10]]
                msgs = await bot.send_media_group(CHAT_ID, media, message_thread_id=thread_id)
                store[slug].extend(m.message_id for m in msgs)

                remaining = "\n".join(rest)
                chunks = split_text_safe(remaining, 4096)
            else:
                chunks = split_text_safe(full_text, 4096)

            for txt in chunks:
                msg = await bot.send_message(
                    CHAT_ID, txt, parse_mode="MarkdownV2", message_thread_id=thread_id
                )
                store[slug].append(msg.message_id)
                await asyncio.sleep(0.5)

            # разделитель
            if idx < len(LOCATIONS):
                msg = await bot.send_message(
                    CHAT_ID, random.choice(emojis),
                    parse_mode="MarkdownV2", message_thread_id=thread_id
                )
                store["all"].append(msg.message_id)

        # —– конец блока
        if finish_text:
            msg = await bot.send_message(
                CHAT_ID, Mark2.escape(finish_text),
                parse_mode="MarkdownV2", message_thread_id=thread_id
            )
            store["all"].append(msg.message_id)

        save_report_data(store)

    # ---------- 4. Обновление (type_ == "update") ----------
    else:
        if all(not store[s] for s in LOCATIONS):
            await message.answer("Обновление невозможно. Публикаций не найдено.")
            return
        # (допишите логику при необходимости)


# для совместимости: если где-то ещё зовётся send_reports
async def send_reports(message: types.Message, bot: Bot):
    await update_reports(message, bot, type_='create')

if __name__ == "__main__":
    # asyncio.run(update_reports(bot=None))
    print(load_report_data())