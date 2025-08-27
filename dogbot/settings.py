import os, json
from pathlib import Path

def _load_env():
    try:
        from dotenv import load_dotenv
    except ImportError:
        return  # библиотека не установлена — пропустим
    # пробуем несколько мест
    candidates = [
        Path.cwd() / ".env",                               # текущая папка запуска
        Path(__file__).resolve().parent.parent / ".env",   # корень проекта
        Path(__file__).resolve().parent / ".env",          # рядом с settings.py
    ]
    for p in candidates:
        if p.exists():
            load_dotenv(p, override=False)

_load_env()

def _parse_admin_ids(val: str | None) -> set[int]:
    if not val:
        return set()
    s = val.strip()
    # поддержим JSON-список: "[1,2,3]"
    if s.startswith("["):
        try:
            arr = json.loads(s)
            return {int(x) for x in arr}
        except Exception:
            pass
    # обычная строка "1,2,3" или "1, 2, 3"
    parts = s.replace(" ", "").split(",")
    return {int(x) for x in parts if x.isdigit()}

def _to_int(val: str | None, default: int = 0) -> int:
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

        # админы: "111,222" или "[111,222]"
        self.ADMIN_IDS: set[int] = _parse_admin_ids(os.getenv("ADMIN_IDS"))

        # строка подключения к БД
        self.DATABASE_URL = os.getenv(
            "DATABASE_URL",
            "postgresql+asyncpg://tg_user:supersecret@localhost:5432/tg_bot"
        )

settings = Settings()