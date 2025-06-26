# generalController.py
import csv
import io
import urllib.request
import asyncio
from aiogram import Bot

GID_GENERAL = "1339673984"
URL_GENERAL = (
    "https://docs.google.com/spreadsheets/"
    "d/1NRGPRwpMyXTe9LhS4adwfPo7nyx68GqweYdAdqo3LpM/"
    f"export?format=csv&gid={GID_GENERAL}"
)

def fetch_general() -> list[dict[str, str]]:
    resp = urllib.request.urlopen(URL_GENERAL)
    return list(csv.DictReader(io.TextIOWrapper(resp, 'utf-8')))

def build_item_caption(row: dict[str, str]) -> str | None:
    row = {k.strip(): v.strip() for k, v in row.items()}  # очищаем пробелы

    name        = row.get("Название", "")
    link        = row.get("Ссылка", "")
    desc        = row.get("Описание", "")
    reviews     = row.get("Отзывы по модели", "")
    price_order = row.get("Под заказ", "")
    link_order  = row.get("Под заказ ссылка", "")
    count       = int(row.get("Кол", 0))
    
    if count < 1:
        return None

    text = f"<a href='{link}'>{name}</a> {desc}" if link else f"{name} {desc}"

    if reviews:
        text += f" <a href='{reviews}'>Отзывы</a>"

    # if count:
    #     text += f" (Кол-во: {count})"

    if price_order:
        text += (
            f" <a href='{link_order}'>Под заказ {price_order}</a>"
            if link_order else f" Под заказ {price_order}"
        )
    elif link_order:
        text += f" <a href='{link_order}'>Под заказ</a>"

    return text

async def send_general(bot: Bot, chat_id: int, thread_id: int | None = None) -> None:
    rows = fetch_general()
    if not rows:
        return
    first_txt = rows[0].get("В начале", "").strip()
    last_txt  = rows[0].get("В конце", "").strip()

    if first_txt:
        await bot.send_message(
            chat_id=chat_id,
            text=f"<b>{first_txt}</b>",
            parse_mode="HTML",
            message_thread_id=thread_id
        )
        print(first_txt)
        await asyncio.sleep(1)

    for row in rows:
        caption = build_item_caption(row)
        if not caption:
            continue

        photo = row.get("Фото", "").strip()

        if photo:
            await bot.send_photo(
                chat_id=chat_id,
                photo=photo,
                caption=caption,
                parse_mode="HTML",
                message_thread_id=thread_id
            )
        else:
            await bot.send_message(
                chat_id=chat_id,
                text=caption,
                parse_mode="HTML",
                message_thread_id=thread_id
            )

        print('---------------------------')
        print(caption)
        print('---------------------------')
        await asyncio.sleep(1)

    if last_txt:
        await bot.send_message(
            chat_id=chat_id,
            text=f"<b>{last_txt}</b>",
            parse_mode="HTML",
            message_thread_id=thread_id
        )
        print(last_txt)
