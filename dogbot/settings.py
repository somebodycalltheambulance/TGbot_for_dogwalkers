# dogbot/settings.py
import os
from dotenv import load_dotenv

load_dotenv()

def _to_int(val, default=0):
    try:
        return int(val)
    except (TypeError, ValueError):
        return default

class Settings:
    def __init__(self):
        # токены/чаты
        self.BOT_TOKEN = os.getenv("BOT_TOKEN", "")
        self.DISPATCHER_CHAT_ID = _to_int(os.getenv("DISPATCHER_CHAT_ID"), 0)
        self.WALKERS_CHAT_ID = _to_int(os.getenv("WALKERS_CHAT_ID"), 0)

        # список админов через запятую: "111,222"
        admin_ids = os.getenv("ADMIN_IDS", "")
        self.ADMIN_IDS = [int(x) for x in admin_ids.split(",") if x.strip().isdigit()]

        # строка подключения к БД
        # по дефолту Postgres, но для тестов можно подменить env на sqlite+aiosqlite:///:memory:
        self.DATABASE_URL = os.getenv(
            "DATABASE_URL",
            "postgresql+asyncpg://tg_user:supersecret@localhost:5432/tg_bot"
        )

settings = Settings()
