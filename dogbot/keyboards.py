
# dogbot/keyboards.py
from aiogram.types import ReplyKeyboardMarkup, KeyboardButton

def main_menu() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="🐶 Услуги для собак")],
            [KeyboardButton(text="👤 Работать у нас")],
            [KeyboardButton(text="📞 Позвать менеджера")],
            [KeyboardButton(text="❓ Общие вопросы")],
        ],
        resize_keyboard=True
    )
