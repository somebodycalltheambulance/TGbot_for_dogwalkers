
# Архитектура DogBot

## Папки
- dogbot/ — пакет бота
  - bot.py — точка входа (`python -m dogbot.bot`)
  - settings.py — чтение .env
  - states.py — FSM состояния
  - keyboards.py — клавиатуры
  - texts.py — тексты/FAQ (пока заглушки)
  - db.py — добавим на этапе БД
- tests/ — pytest-тесты
  - test_fsm.py — базовая проверка FSM
- docs/ — документация (plan.md, architecture.md)

## Запуск
1) `python -m venv venv && source venv/bin/activate`
2) `pip install -r requirements.txt`
3) скопируй `.env.example` в `.env` и заполни `BOT_TOKEN`
4) `python -m dogbot.bot`

## Тесты
- `pytest -v`
