# generalController.py
import csv, io, asyncio, urllib.request, re
from aiogram import Bot, types

# --- константы и util --------------------------------------------------------

GID_GENERAL = "1339673984"
URL_GENERAL = (
    "https://docs.google.com/spreadsheets/"
    "d/1NRGPRwpMyXTe9LhS4adwfPo7nyx68GqweYdAdqo3LpM/"
    f"export?format=csv&gid={GID_GENERAL}"
)

HTML_ESC = re.compile(r"[&<>]")

def esc(text: str) -> str:
    return HTML_ESC.sub(lambda m: {"&":"&amp;","<":"&lt;",">":"&gt;"}[m.group()], text)

def split_safe(text: str, limit: int) -> list[str]:
    """Делит текст по строкам, не превышая limit."""
    lines, parts, cur = text.splitlines(keepends=True), [], ""
    for ln in lines:
        if len(cur) + len(ln) > limit:
            parts.append(cur.rstrip())
            cur = ln
        else:
            cur += ln
    if cur.strip():
        parts.append(cur.rstrip())
    return parts

# --- загрузка ----------------------------------------------------------------

def fetch_general() -> list[dict[str, str]]:
    resp = urllib.request.urlopen(URL_GENERAL)
    return list(csv.DictReader(io.TextIOWrapper(resp, "utf-8")))

def build_item_caption(row: dict[str, str]) -> str | None:
    row = {k.strip(): v.strip() for k, v in row.items()}
    if int(row.get("Кол", 0)) < 1:
        return None

    name, link      = esc(row.get("Название", "")), row.get("Ссылка", "")
    desc            = esc(row.get("Описание", ""))
    reviews         = row.get("Отзывы по модели", "")
    price_order     = esc(row.get("Под заказ", ""))
    link_order      = row.get("Под заказ ссылка", "")

    txt = f"<a href='{link}'>{name}</a> {desc}" if link else f"{name} {desc}"
    if reviews:
        txt += f" <a href='{reviews}'>Отзывы</a>"
    if price_order:
        txt += f" <a href='{link_order}'>Под заказ {price_order}</a>" if link_order else f" Под заказ {price_order}"
    elif link_order:
        txt += f" <a href='{link_order}'>Под заказ</a>"
    return txt

# --- публикация --------------------------------------------------------------

async def send_general(bot: Bot, chat_id: int, thread_id: int | None = None) -> None:
    rows = fetch_general()
    if not rows:
        return

    beg_txt = rows[0].get("В начале", "").strip()
    end_txt = rows[0].get("В конце", "").strip()

    if beg_txt:
        await bot.send_message(chat_id, f"<b>{esc(beg_txt)}</b>",
                               parse_mode="HTML", message_thread_id=thread_id)
        await asyncio.sleep(0.3)

    # собираем все строки и фото
    texts, photos = [], []
    for r in rows:
        line = build_item_caption(r)
        if not line:
            continue
        texts.append(line)
        p = r.get("Фото", "").strip()
        if p:
            photos.append(p)

    full_text = "\n\n".join(texts)

    if photos:                                    # отправляем медиа-группой
        cap, *rest = split_safe(full_text, 1024)  # 1024 для caption
        media = [types.InputMediaPhoto(media=photos[0], caption=cap, parse_mode="HTML")]
        media += [types.InputMediaPhoto(media=u) for u in photos[1:10]]  # max 10
        await bot.send_media_group(chat_id, media, message_thread_id=thread_id)

        remaining = "\n".join(rest)
        for chunk in split_safe(remaining, 4000):  # 4000 пост-лимит
            await bot.send_message(chat_id, chunk, parse_mode="HTML",
                                   message_thread_id=thread_id)
            await asyncio.sleep(0.3)
    else:                                         # без фото — просто текстами
        for chunk in split_safe(full_text, 4000):
            await bot.send_message(chat_id, chunk, parse_mode="HTML",
                                   message_thread_id=thread_id)
            await asyncio.sleep(0.3)

    if end_txt:
        await bot.send_message(chat_id, f"<b>{esc(end_txt)}</b>",
                               parse_mode="HTML", message_thread_id=thread_id)
