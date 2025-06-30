# main.py

import asyncio
from functools import wraps
import json
import os
import random
import re
import time
import logging
from pathlib import Path

from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command, StateFilter
from aiogram import F
from dotenv import load_dotenv

from reportingController import send_reports, update_reports
from generalController import send_general
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.fsm.storage.memory import MemoryStorage

# from chatController import THRESHOLD, build_question_vectors, chat_listener_active, cosine_similarity, find_answer, text_to_vector

load_dotenv()

API_TOKEN = os.getenv("API_TOKEN_OLD") or ""  # Токен бота
CHAT_ID = os.getenv("CHAT_ID") or ""   # Идентификатор чата
CHAT_THREAD_ID = {
    'Казань':   745,  # Тема в которую публикуют
}
DATA_FILE = Path('promo_data.json')

logging.basicConfig(level=logging.INFO)

# Фунция для проверки админ ли это
def is_admin(user_id):
    if user_id == 1303257033 or user_id == 577151281:
        return True
    return False

# Декоратор для проверки на обращение от админов
def admin_only(handler):
    @wraps(handler)
    async def wrapper(message: types.Message, *args, **kwargs):
        if is_admin(message.from_user.id):
            return await handler(message, *args, **kwargs)
        await message.answer("Недостаточно прав.", show_alert=True)
        return
    return wrapper

# Декоратор для проверки на обращение от админов в callback кнопках
def admin_only_callback(handler):
    @wraps(handler)
    async def wrapper(callback: types.CallbackQuery, *args, **kwargs):
        if is_admin(callback.from_user.id):
            return await handler(callback, *args, **kwargs)
        await callback.answer("Недостаточно прав.", show_alert=True)
        return
    return wrapper

# Декоратор проверяет пришло ли сообщение с группы. Если пришло с бота то игнорирует
def from_group_only(handler):
    @wraps(handler)
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.chat.type in {"group", "supergroup"}:
            return await handler(message, *args, **kwargs)
        return
    return wrapper

# Декоратор проверяет пришло ли сообщение с группы callback. Если пришло с бота то игнорирует
def from_group_only_callback(handler):
    @wraps(handler)
    async def wrapper(callback: types.CallbackQuery, *args, **kwargs):
        if callback.message.chat.type in {"group", "supergroup"}:
            return await handler(callback, *args, **kwargs)
        return
    return wrapper

# Декоратор проверяет пришло ли сообщение с бота. Если пришло с группы то игнорирует
def from_personal_only(handler):
    @wraps(handler)
    async def wrapper(message: types.Message, *args, **kwargs):
        if message.chat.type == "private":
            return await handler(message, *args, **kwargs)
        return
    return wrapper

# Декоратор проверяет пришло ли сообщение с бота callback. Если пришло с группы то игнорирует
def from_private_only_callback(handler):
    @wraps(handler)
    async def wrapper(callback: types.CallbackQuery, *args, **kwargs):
        if callback.message.chat.type == "private":
            return await handler(callback, *args, **kwargs)
        return
    return wrapper


# === Глобальные клавиатуры ===

# Клавиатура отмены создания акции
CANCEL_CREATION_KB = types.ReplyKeyboardMarkup(
    keyboard=[
        [
            types.KeyboardButton(text="Отменить создание акции")  # Отменить процесс создания акции и вернуться в меню
        ]
    ],
    resize_keyboard=True
)

# Главное меню для админов
def get_main_menu_kb():
    return types.ReplyKeyboardMarkup(
        keyboard=[
            [
                types.KeyboardButton(text="Просмотреть акции"),
                types.KeyboardButton(text="Создать акцию")
            ],
            [
                types.KeyboardButton(text="Опубликовать отчет 'Отчет о наличии'"),
                types.KeyboardButton(text="Обновить отчёт 'Отчет о наличии'"),
            ],
            [
                types.KeyboardButton(text="Опубликовать отчет 'Отправление в общую'")
            ]
        ],
        resize_keyboard=True
    )

# Клавиатура подтверждения замены акции
def get_replace_confirm_kb():
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="Да, заменить", callback_data='confirm_replace'),  # Заменить акцию
                types.InlineKeyboardButton(text="Нет", callback_data='cancel_replace')            # Отменить замену
            ]
        ]
    )

# Клавиатура управления активной акцией
def get_active_promo_kb():
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="Деактивировать", callback_data="deactivate"),  # Остановить акцию
                types.InlineKeyboardButton(text="Сброс времени", callback_data="reset")         # Сбросить таймер акции
            ],
            [
                types.InlineKeyboardButton(text="Удалить", callback_data="delete")  # Удалить акцию
            ]
        ]
    )

# Клавиатура управления неактивной акцией
def get_inactive_promo_kb():
    return types.InlineKeyboardMarkup(
        inline_keyboard=[
            [
                types.InlineKeyboardButton(text="Активировать", callback_data="activate")  # Запустить акцию
            ],
            [
                types.InlineKeyboardButton(text="Удалить", callback_data="delete")  # Удалить акцию
            ]
        ]
    )

# Функция для загрузки данных с json
def load_data():
    if not DATA_FILE.exists():
        return {'admin_id': None, 'promo': None}
    try:
        return json.loads(DATA_FILE.read_text())
    except Exception:
        logging.exception("Ошибка чтения данных, загружаем пустые.")
        return {'admin_id': None, 'promo': None}

# Функция сохранения данных в json
def save_data(data):
    try:
        DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception:
        logging.exception("Ошибка при сохранении данных.")

# Загружаем данные
data = load_data()

# Форма для создания акции
class Form(StatesGroup):
    template = State()
    duration = State()

# Функция форматированного вывода времени h:m:s
def fmt_secs(secs: int) -> str:
    h = secs // 3600
    m = (secs % 3600) // 60
    s = secs % 60
    return f"{h:02d}:{m:02d}:{s:02d}"

# 
def render_text(template: str, remaining: int) -> str:
    return template.replace('{{time}}', fmt_secs(remaining))

# Функция проверки на введенную команду
def group_command_reader(command: str):
    if len(command.split(' ')) == 1:

        if command == '/help':
            return "Text"

    return None


# Функция публикации акции. Закреп акции в темах.
async def send_initial_messages(bot: Bot, promo: dict):
    promo['messages'] = {}
    # for thread_name, thread_id in CHAT_THREAD_ID.items():
    text = render_text(promo['template'], promo['initial'])
    try:
        msg = await bot.send_message(
            chat_id=CHAT_ID,
            text=text,
            message_thread_id=CHAT_THREAD_ID['Казань']
        )
        promo['messages'][str(CHAT_THREAD_ID['Казань'])] = msg.message_id

        # Закрепляем сообщение
        await bot.pin_chat_message(
            chat_id=CHAT_ID,
            message_id=msg.message_id,
            disable_notification=True  # Чтобы без лишнего уведомления
        )
    except Exception:
        logging.exception(f"Не удалось отправить или закрепить сообщение в теме {CHAT_THREAD_ID['Казань']}")

    promo['start_time'] = int(time.time())
    save_data(data)

# Функция завершения акции. Удаление акции из тем.
async def finish_promo(bot: Bot, promo: dict):
    thread_id = CHAT_THREAD_ID['Казань']
    # удаляем единственное сообщение акции
    msg_id = promo.get('messages', {}).get(str(thread_id))
    if msg_id:
        try:
            await bot.delete_message(chat_id=CHAT_ID, message_id=msg_id)
        except Exception:
            logging.exception(f"Не удалось удалить сообщение {msg_id} в теме {thread_id}")
    # уведомляем об окончании только в этой теме
    try:
        await bot.send_message(
            chat_id=CHAT_ID,
            text="Акция завершена.",
            message_thread_id=thread_id
        )
    except Exception:
        logging.exception(f"Не удалось уведомить об окончании в теме {thread_id}")
    promo['active'] = False
    promo['messages'] = {}
    save_data(data)

# Функция обновляющая таймер акции в чатах
async def minute_updater(bot: Bot):
    while True:
        await asyncio.sleep(60)
        print('update minute')
        promo = data.get('promo')
        if not promo or not promo.get('active'):
            continue
        now = int(time.time())
        rem = max(0, promo['initial'] - (now - promo['start_time']))
        if rem <= 0:
            await finish_promo(bot, promo)
            continue
        text = render_text(promo['template'], rem)
        for thread_id_str, message_id in promo['messages'].items():
            try:
                await bot.edit_message_text(
                    text=text,
                    chat_id=CHAT_ID,
                    message_id=message_id
                )
            except Exception:
                logging.exception(f"Ошибка обновления сообщения {message_id} в теме {thread_id_str}")

# Функция запускающая таймер акции заново после перезапуска бота
# async def on_startup(bot: Bot, dispatcher: Dispatcher):
#     global question_vectors
#     question_vectors = build_question_vectors()
#     promo = data.get('promo')
#     if promo and promo.get('active'):
#         logging.info("Восстановление активной акции после рестарта.")
#         if not promo.get('messages'):
#             logging.info("Сообщения отсутствуют, пересоздаем...")
#             await send_initial_messages(bot, promo)
#         asyncio.create_task(minute_updater(bot))
#     else:
#         logging.info("Активных акций для восстановления нет.")

# Функция инициализации и запуска бота
async def main():
    bot = Bot(token=API_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # Команада запуска бота: /start. Доступна только в персональном чате. 
    @dp.message(Command("start"))
    @from_personal_only
    async def cmd_start(message: types.Message, state: FSMContext):
        if (user := message.from_user) and is_admin(user.id):
            return await message.answer("Привет! Выберите действие:", reply_markup=get_main_menu_kb())
        
        return await message.answer("Добро пожаловать!")

    # Создание акции. Доступн только у администратора. Доступн только в персональном чате.
    @dp.message(F.text == "Создать акцию")
    @admin_only
    @from_personal_only
    async def cmd_create(message: types.Message, state: FSMContext):
        promo = data.get('promo')
        if promo:
            return await message.answer("Акция уже существует. Заменить её?", reply_markup=get_replace_confirm_kb())
        await state.set_state(Form.template)

        await message.answer("Введите шаблон (с {{time}}):", reply_markup=CANCEL_CREATION_KB)

    @dp.message(F.text == "Опубликовать отчет 'Отчет о наличии'")
    @admin_only
    @from_personal_only
    async def cmd_publish_report(message: types.Message):
        if message.bot:
            await message.answer("Публикую отчет…")
            await send_reports(message, message.bot)
            await message.answer("Отчет опубликован.", reply_markup=get_main_menu_kb())

    @dp.message(F.text == "Обновить отчёт 'Отчет о наличии'")
    @admin_only
    @from_personal_only
    async def cmd_update_report(message: types.Message):
        if message.bot:
            await message.answer("Обновляю…")
            await update_reports(message, message.bot)
            await message.answer("Готово.", reply_markup=get_main_menu_kb())

    @dp.message(F.text == "Опубликовать отчет 'Отправление в общую'")
    @admin_only
    @from_personal_only
    async def cmd_general(message: types.Message):
        if message.bot:
            await message.answer("Формирую и отправляю…")
            await send_general(message.bot, CHAT_ID, CHAT_THREAD_ID["Казань"])
            await message.answer("Готово.", reply_markup=get_main_menu_kb())

    # 
    @dp.callback_query(F.data == "confirm_replace")
    @admin_only_callback
    @from_personal_only
    async def cb_confirm_replace(callback: types.CallbackQuery, state: FSMContext):
        data['promo'] = None
        save_data(data)
        await callback.message.edit_reply_markup(None)
        await state.set_state(Form.template)

        await callback.message.answer("Введите новый шаблон (с {{time}}):", reply_markup=CANCEL_CREATION_KB)
        await callback.answer()

    # 
    @dp.callback_query(F.data == "cancel_replace")
    @admin_only_callback
    @from_personal_only
    async def cb_cancel_replace(callback: types.CallbackQuery, state: FSMContext):
        await callback.message.edit_reply_markup(None)
        await state.clear()

        await callback.message.answer("Главное меню:", reply_markup=get_main_menu_kb())
        await callback.answer()

    # 

    @dp.message(F.text == "Отменить создание акции")
    @admin_only
    @from_personal_only
    async def cancel_flow(message: types.Message, state: FSMContext):
        await state.clear()

        await message.answer("Создание акции отменено.", reply_markup=get_main_menu_kb())

    @dp.message(StateFilter(Form.template), F.text)
    @admin_only
    @from_personal_only
    async def process_template(message: types.Message, state: FSMContext):
        if not message.text or message.text.count("{{time}}") != 1:
            return await message.answer("Шаблон должен содержать ровно один {{time}}. Повторите ввод:")
        await state.update_data(template=message.text)
        await state.set_state(Form.duration)

        await message.answer("Укажите длительность акции в формате ЧЧ:ММ[:СС]:", reply_markup=CANCEL_CREATION_KB)

    @dp.message(StateFilter(Form.duration), F.text)
    @admin_only
    @from_personal_only
    async def process_duration(message: types.Message, state: FSMContext):
        if not message.text:
            return await message.answer("Отправьте текстовое сообщение.")
        m = re.match(r'^(\d{1,2}):(\d{2})(?::(\d{2}))?$', message.text.strip())
        if not m:
            return await message.answer("Неверный формат. Пример: 02:30 или 01:15:00. Повторите:")
        h, mi, s = int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)
        if mi >= 60 or s >= 60 or (h == 0 and mi == 0 and s == 0) or h > 24:
            return await message.answer("Минуты/секунды <60, длительность >0 и ≤24ч. Повторите:")
        data['promo'] = {
            'template': (await state.get_data())['template'],
            'initial': h * 3600 + mi * 60 + s,
            'duration': h * 3600 + mi * 60 + s,
            'start_time': None,
            'active': False,
            'messages': {}
        }
        save_data(data)
        await state.clear()

        await message.answer("Акция создана.", reply_markup=get_main_menu_kb())


    @dp.message(F.text == "Просмотреть акции")
    @admin_only
    @from_personal_only
    async def cmd_view(message: types.Message):
        promo = data.get('promo')
        if not promo:

            return await message.answer("Нет созданных акций.", reply_markup=get_main_menu_kb())

        now = int(time.time())
        rem = promo['initial'] if not promo['active'] else max(0, promo['initial'] - (now - promo['start_time']))
        text = (
            f"Шаблон:\n{render_text(promo['template'], rem)}\n\n"
            f"Оставшееся время: {fmt_secs(rem)}\n"
            f"Статус: {'Активна' if promo['active'] else 'Неактивна'}"
        )

        # Один раз создаём разметку
        kb = get_active_promo_kb() if promo['active'] else get_inactive_promo_kb()
        await message.answer(text, reply_markup=kb)

    @dp.message(F.text == "get_chat_id")
    @admin_only
    async def get_chat_id(message: types.Message):
        chat_id = message.chat.id
        thread_id = message.message_thread_id

        text = f"Chat ID: `{chat_id}`"
        if thread_id:
            text += f"\nThread (Topic) ID: `{thread_id}`"
        else:
            text += "\n(Сообщение не в топике)"

        await message.reply(text, parse_mode="Markdown")


    @dp.callback_query(F.data.in_(["activate", "deactivate", "reset", "delete"]))
    @admin_only_callback
    @from_private_only_callback
    async def cb_action(callback: types.CallbackQuery):
        promo = data.get('promo')
        if not promo:
            return await callback.answer("Акция отсутствует.")
        act = callback.data
        if act == 'activate':
            promo['active'] = True
            await send_initial_messages(callback.bot, promo)
            await callback.answer("Акция активирована!")
        elif act == 'deactivate':
            await finish_promo(callback.bot, promo)
            await callback.answer("Акция деактивирована.")
        elif act == 'reset':
            promo['start_time'] = None
            promo['messages'] = {}
            promo['active'] = True
            await send_initial_messages(callback.bot, promo)
            await callback.answer("Время акции сброшено.")
        elif act == 'delete':
            if promo.get('active'):
                await finish_promo(callback.bot, promo)
            data['promo'] = None
            save_data(data)
            await callback.answer("Акция удалена.")

    # dp.startup.register(on_startup)

    # запускаем ежедневную рассылку в фоне
    # asyncio.create_task(daily_job(bot))

    # и дальше запускаем polling
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())

