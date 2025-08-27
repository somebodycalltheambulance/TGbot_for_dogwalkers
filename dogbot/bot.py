# dogbot/bot.py
import logging
import asyncio
import datetime as dt
import re

from aiogram import Bot, Dispatcher, F
from aiogram.types import (
    Message,
    CallbackQuery,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
)
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext

from dogbot.settings import settings
from dogbot.keyboards import main_menu
from dogbot.states import OrderStates, WorkStates, ProposalStates
from dogbot import db

logging.basicConfig(level=logging.INFO)

if not settings.BOT_TOKEN:
    raise RuntimeError("BOT_TOKEN пуст. Заполни .env")

bot = Bot(settings.BOT_TOKEN)
dp = Dispatcher()

# ====================== Клавиатуры / хелперы UI ======================
def kb_walk_types() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Обычный", callback_data="walk:normal")],
        [InlineKeyboardButton(text="Активный", callback_data="walk:active")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="back:main")],
    ])

def kb_services() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="Выгул", callback_data="srv:walk")],
        [InlineKeyboardButton(text="Передержка", callback_data="srv:boarding")],
        [InlineKeyboardButton(text="Няня", callback_data="srv:nanny")],
        [InlineKeyboardButton(text="⬅️ Отмена", callback_data="back:main")],
    ])

def kb_order_candidates(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="👀 Кандидаты", callback_data=f"cands:{order_id}")],
    ])

def kb_respond(order_id: int) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✋ Откликнуться", callback_data=f"pr:{order_id}")],
    ])

def order_title(service: str, walk_type: str | None) -> str:
    title = {"walk": "🦮 Выгул", "boarding": "🏡 Передержка", "nanny": "👩‍🍼 Няня"}.get(service, "🐶 Услуга")
    if service == "walk" and walk_type:
        sub = {"normal": "Обычный", "active": "Активный"}.get(walk_type, "")
        if sub:
            title += f" ({sub})"
    return title

def is_admin(user_id: int) -> bool:
    return user_id in settings.ADMIN_IDS

async def require_walker(user_id: int) -> bool:
    role = await db.get_user_role(user_id)
    return role == "walker"

async def send_order_to_walkers(card_text: str, photo_file_id: str | None, order_id: int):
    """Рассылка заказа всем walker'ам в личку (вариант B)."""
    walker_ids = await db.list_walkers_ids()
    if not walker_ids:
        return
    batch = 25
    for i in range(0, len(walker_ids), batch):
        chunk = walker_ids[i:i + batch]
        tasks = []
        for wid in chunk:
            if photo_file_id:
                tasks.append(bot.send_photo(wid, photo=photo_file_id, caption=card_text, reply_markup=kb_respond(order_id)))
            else:
                tasks.append(bot.send_message(wid, card_text, reply_markup=kb_respond(order_id)))
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(1)

async def send_order_to_walkers_by_area(card_text: str, photo_file_id: str | None, order_id: int, area: str):
    """Рассылка только тем walker'ам, у кого район совпадает с заказом."""
    ids = await db.list_walkers_by_area(area)
    if not ids:
        # fallback: если нет совпадений, шлём всем
        return await send_order_to_walkers(card_text, photo_file_id, order_id)

    batch = 25
    for i in range(0, len(ids), batch):
        chunk = ids[i:i + batch]
        tasks = []
        for wid in chunk:
            if photo_file_id:
                tasks.append(bot.send_photo(
                    wid, photo=photo_file_id,
                    caption=card_text,
                    reply_markup=kb_respond(order_id)
                ))
            else:
                tasks.append(bot.send_message(
                    wid, card_text,
                    reply_markup=kb_respond(order_id)
                ))
        await asyncio.gather(*tasks, return_exceptions=True)
        await asyncio.sleep(1)

@dp.message(Command("whoami"))
async def whoami_cmd(m: Message):
    await m.answer(f"Твой Telegram ID: {m.from_user.id}")

# на всякий случай дубль по тексту, если Command где-то перехватывают
@dp.message(F.text.startswith("/whoami"))
async def whoami_text(m: Message):
    await m.answer(f"Твой Telegram ID: {m.from_user.id}")


@dp.message(F.text == "👤 Работать у нас")
async def on_work(m: Message, state: FSMContext):
    await state.set_state(WorkStates.collecting_name)
    await m.answer("Как к тебе обращаться? (Имя и, если хочешь, коротко о себе)")

@dp.message(WorkStates.collecting_name, F.text)
async def work_name(m: Message, state: FSMContext):
    name = m.text.strip()[:64]
    await state.update_data(name=name)
    await state.set_state(WorkStates.collecting_phone)
    await m.answer("Телефон для связи (пример: +79990000000).")

@dp.message(WorkStates.collecting_phone, F.text)
async def work_phone(m: Message, state: FSMContext):
    phone = m.text.strip().replace(" ", "")
    if not phone.startswith("+") or len(phone) < 10:
        return await m.reply("Дай нормальный телефон с +, ок? Например +79990000000")
    await state.update_data(phone=phone)
    await state.set_state(WorkStates.collecting_exp)
    await m.answer("Коротко об опыте: породы, сколько водишь, особенности. Можно указать ставку числом (руб/ч).")

@dp.message(WorkStates.collecting_exp, F.text)
async def work_exp(m: Message, state: FSMContext):
    bio = m.text.strip()[:500]
    # вытащим ставку (первое число 3-5 цифр)
    rate = None
    m_rate = re.search(r"\b(\d{3,5})\b", bio)
    if m_rate:
        try:
            rate = int(m_rate.group(1))
        except ValueError:
            rate = None
    await state.update_data(bio=bio, rate=rate)
    await state.set_state(WorkStates.collecting_areas)
    await m.answer("В каких районах работаешь? Укажи через запятую (например: Центр, Савёловский, Купчино).")

@dp.message(WorkStates.collecting_areas, F.text)
async def work_areas(m: Message, state: FSMContext):
    areas = m.text.strip()[:200]
    data = await state.get_data()

    # Создаём/обновляем пользователя и профиль
    await db.upsert_user(
        tg_id=m.from_user.id,
        username=m.from_user.username,
        full_name=data["name"],
        role="walker",
    )
    await db.upsert_walker_profile(
        walker_id=m.from_user.id,
        phone=data.get("phone"),
        bio=data.get("bio"),
        rate=data.get("rate"),
        areas=areas,
    )

    await state.clear()
    msg = "Готово! Профиль исполнителя создан и роль выдана (walker).\n"
    if data.get("rate"):
        msg += f"Ставка: {data['rate']}₽/час.\n"
    msg += f"Районы: {areas or '—'}\nТеперь ты можешь откликаться на заявки."
    await m.answer(msg)


# ====================== Утилиты ======================
def _clean_int(s: str) -> int | None:
    s = (s or "").strip().replace(" ", "")
    return int(s) if s.isdigit() else None

def _parse_when(txt: str) -> dt.datetime | None:
    """
    Поддерживаем:
    - 'YYYY-MM-DD HH:MM'
    - 'YYYY-MM-DD HH.MM'
    - 'сегодня 19:00' / 'завтра 10:30'
    Возвращаем aware datetime в UTC. (MVP без локализации по TZ юзера)
    """
    if not txt:
        return None
    t = txt.strip().lower()

    for fmt in ("%Y-%m-%d %H:%M", "%Y-%m-%d %H.%M"):
        try:
            naive = dt.datetime.strptime(t, fmt)
            return naive.replace(tzinfo=dt.timezone.utc)
        except ValueError:
            pass

    def _hhmm_to_utc(day_offset: int) -> dt.datetime | None:
        try:
            parts = t.split(maxsplit=1)
            if len(parts) < 2:
                return None
            hhmm = parts[1].replace(".", ":")
            hh, mm = [int(x) for x in hhmm.split(":")]
            local = dt.datetime.now().replace(hour=hh, minute=mm, second=0, microsecond=0)
            local = local + dt.timedelta(days=day_offset)
            return local.astimezone(dt.timezone.utc)
        except Exception:
            return None

    if t.startswith("сегодня"):
        return _hhmm_to_utc(0)
    if t.startswith("завтра"):
        return _hhmm_to_utc(1)
    return None

# ====================== Базовые команды ======================
@dp.message(Command("start"))
async def cmd_start(m: Message, state: FSMContext):
    await state.clear()
    await db.upsert_user(
        tg_id=m.from_user.id,
        username=m.from_user.username,
        full_name=m.from_user.full_name,
        phone=None,
        role="client",
    )
    await m.answer("Привет! Это DogBot: выгул/передержка/няня. Выбирай ниже 👇", reply_markup=main_menu())

@dp.message(Command("help"))
async def cmd_help(m: Message):
    await m.answer("Команды: /start, /help, /my_orders, /cancel, /role")

@dp.message(Command("state"))
async def check_state(m: Message, state: FSMContext):
    await m.answer(f"state: {await state.get_state()}")

@dp.message(Command("cancel"))
async def cmd_cancel(m: Message, state: FSMContext):
    await state.clear()
    await m.answer("Окей, отменил. Возвращаюсь в меню.", reply_markup=main_menu())

# ====================== Роли / админка ======================
@dp.message(Command("role"))
async def cmd_role(m: Message):
    role = await db.get_user_role(m.from_user.id)
    await m.answer(f"Твоя роль: {role or 'не зарегистрирован'}")

@dp.message(Command("set_role"))
async def cmd_set_role(m: Message):
    if not is_admin(m.from_user.id):
        return await m.answer("Не админ. И не пытайся 😉")
    parts = (m.text or "").split()
    if len(parts) != 3:
        return await m.answer("Использование: /set_role <tg_id> <client|walker|admin>")
    try:
        uid = int(parts[1]); role = parts[2].strip()
        await db.set_user_role(uid, role)
        await m.answer(f"OK: {uid} → {role}")
    except Exception as e:
        await m.answer(f"Ошибка: {e}")

@dp.message(F.text == "👤 Работать у нас")
async def on_work(m, state):
    await state.set_state(WorkStates.collecting_name)
    await m.answer("Как к тебе обращаться?")

@dp.message(WorkStates.collecting_name, F.text)
async def work_name(m, state):
    await state.update_data(name=m.text.strip()[:64])
    await state.set_state(WorkStates.collecting_phone)
    await m.answer("Телефон с +, напр. +79990000000")

@dp.message(WorkStates.collecting_phone, F.text)
async def w_phone(m: Message, state: FSMContext):
    await state.update_data(phone=m.text.strip()[:32])
    await state.set_state(WorkStates.city)
    await m.answer("Город:")

@dp.message(WorkStates.collecting_exp, F.text)
async def work_exp(m, state):
    import re
    bio = m.text.strip()[:500]
    m_rate = re.search(r"\b(\d{3,5})\b", bio)
    rate = int(m_rate.group(1)) if m_rate else None
    await state.update_data(bio=bio, rate=rate)
    await state.set_state(WorkStates.collecting_areas)
    await m.answer("Районы через запятую (например: Центр, Савёловский).")

@dp.message(WorkStates.collecting_phone, F.text)
async def work_phone(m, state):
    phone = m.text.strip().replace(" ", "")
    if not phone.startswith("+") or len(phone) < 10:
        return await m.reply("Дай телефон формата +7...")
    await state.update_data(phone=phone)
    await state.set_state(WorkStates.collecting_exp)
    await m.answer("Коротко об опыте. Можно указать ставку (числом).")

@dp.message(WorkStates.collecting_exp, F.text)
async def w_experience(m: Message, state: FSMContext):
    await state.update_data(experience=m.text.strip()[:300])
    await state.set_state(WorkStates.price_from)
    await m.answer("Базовая ставка (руб, число):")
    
@dp.message(WorkStates.collecting_areas, F.text)
async def work_areas(m, state):
    areas = m.text.strip()[:200]
    data = await state.get_data()
    await db.upsert_user(m.from_user.id, m.from_user.username, data["name"], role="walker")
    await db.upsert_walker_profile(m.from_user.id, phone=data.get("phone"), bio=data.get("bio"),
            price_from=data.get("rate"), areas=areas)
    await state.clear()
    await m.answer(f"Готово! Роль выдана (walker). Районы: {areas or '—'}")


# ====================== Главный мастер заказа ======================
@dp.message(F.text == "🐶 Услуги для собак")
async def on_services(m: Message, state: FSMContext):
    await state.set_state(OrderStates.choosing_service)
    await m.answer("Какую услугу выбираем?", reply_markup=kb_services())

@dp.callback_query(F.data == "back:main")
async def cb_back_main(cq: CallbackQuery, state: FSMContext):
    await state.clear()
    await cq.message.answer("Главное меню:", reply_markup=main_menu())
    await cq.answer()

@dp.callback_query(OrderStates.choosing_service, F.data.startswith("srv:"))
async def cb_choose_service(cq: CallbackQuery, state: FSMContext):
    service = cq.data.split(":", 1)[1]
    await state.update_data(service=service, walk_type=None)
    if service == "walk":
        await state.set_state(OrderStates.choosing_walk_type)
        await cq.message.edit_text("Выгул — какой нужен?", reply_markup=kb_walk_types())
    else:
        await state.set_state(OrderStates.pet_name)
        await cq.message.edit_text("Как зовут собаку?")
    await cq.answer()

@dp.callback_query(OrderStates.choosing_walk_type, F.data.startswith("walk:"))
async def cb_choose_walk_type(cq: CallbackQuery, state: FSMContext):
    await state.update_data(walk_type=cq.data.split(":", 1)[1])
    await state.set_state(OrderStates.pet_name)
    await cq.message.edit_text("Как зовут собаку?")
    await cq.answer()

@dp.message(OrderStates.pet_name, F.text)
async def step_pet_name(m: Message, state: FSMContext):
    await state.update_data(pet_name=m.text.strip()[:64])
    await state.set_state(OrderStates.pet_size)
    await m.answer("Размер собаки? (small/medium/large)")

@dp.message(OrderStates.pet_size, F.text)
async def step_pet_size(m: Message, state: FSMContext):
    size = m.text.strip().lower()
    if size not in {"small", "medium", "large"}:
        return await m.reply("Введи один из вариантов: small / medium / large")
    await state.update_data(pet_size=size)
    await state.set_state(OrderStates.area)
    await m.answer("В каком районе нужен исполнитель? (например: Центр, Савёловский, Купчино)")

@dp.message(OrderStates.area, F.text)
async def step_area(m: Message, state: FSMContext):
    area = m.text.strip()[:64]
    if len(area) < 2:
        return await m.reply("Дай название района поконкретнее.")
    await state.update_data(area=area)
    await state.set_state(OrderStates.when_at)
    await m.answer("Когда? Формат: 2025-08-23 19:00 или «сегодня 19:00», «завтра 10:30». /cancel — отмена.")

@dp.message(OrderStates.when_at, F.text)
async def step_when(m: Message, state: FSMContext):
    ts = _parse_when(m.text)
    if not ts or ts <= dt.datetime.now(dt.timezone.utc):
        return await m.reply("Не понял дату/время или это уже в прошлом. Пример: 2025-08-23 19:00")
    await state.update_data(when_at=ts)
    await state.set_state(OrderStates.duration_min)
    await m.answer("Длительность в минутах? (например 60)")

@dp.message(OrderStates.duration_min, F.text)
async def step_duration(m: Message, state: FSMContext):
    val = _clean_int(m.text)
    if not val or val <= 0 or val > 12 * 60:
        return await m.reply("Минуты должны быть числом > 0 и <= 720.")
    await state.update_data(duration_min=val)
    await state.set_state(OrderStates.address)
    await m.answer("Адрес (улица, дом, подъезд).")

@dp.message(OrderStates.address, F.text)
async def step_address(m: Message, state: FSMContext):
    addr = m.text.strip()
    if len(addr) < 5:
        return await m.reply("Слишком короткий адрес, давай точнее.")
    await state.update_data(address=addr)
    await state.set_state(OrderStates.budget)
    await m.answer("Бюджет (руб), опционально. Можешь написать 0 или пропустить командой /skip.")

@dp.message(Command("skip"))
async def skip_any(m: Message, state: FSMContext):
    st = await state.get_state()
    if st == OrderStates.budget.state:
        await state.update_data(budget=None)
        await state.set_state(OrderStates.comment)
        return await m.answer("Комментарий для исполнителя (опционально). /skip если нечего добавить.")
    if st == OrderStates.comment.state:
        await state.update_data(comment=None)
        data = await state.get_data()
        return await _confirm_order(m, state, data)
    await m.answer("Эта команда сейчас не к месту :)")

# === подтверждение заказа (должно быть выше step_comment) ===
async def _confirm_order(m: Message, state: FSMContext, data: dict):
    title = order_title(data["service"], data.get("walk_type"))
    when_local = data["when_at"].astimezone(dt.timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    text = (
        f"{title}\n"
        f"Имя: {data['pet_name']} | Размер: {data['pet_size']}\n"
        f"Район: {data['area']}\n"
        f"Когда: {when_local} • {data['duration_min']} мин\n"
        f"Адрес: {data['address']}\n"
        f"Бюджет: {data.get('budget') if data.get('budget') is not None else '—'}\n"
        f"Комментарий: {data.get('comment') or '—'}\n\n"
        f"Отправляю заказ исполнителям?"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [
            InlineKeyboardButton(text="✅ Да", callback_data="ord:confirm"),
            InlineKeyboardButton(text="❌ Нет", callback_data="ord:cancel"),
        ]
    ])
    await state.set_state(OrderStates.confirming)
    await m.answer(text, reply_markup=kb)

@dp.callback_query(OrderStates.confirming, F.data.in_({"ord:confirm", "ord:cancel"}))
async def cb_confirm(cq: CallbackQuery, state: FSMContext):
    if cq.data == "ord:cancel":
        await state.clear()
        await cq.message.edit_text("Окей, отменил. Вернулся в меню.")
        return await cq.answer()

    data = await state.get_data()
    order_id = await db.add_order(
    client_id=cq.from_user.id,
    service=data["service"],
    pet_name=data["pet_name"],
    pet_size=data["pet_size"],
    when_at=data["when_at"],
    duration_min=data["duration_min"],
    address=data["address"],
    budget=data.get("budget"),
    comment=data.get("comment"),
    walk_type=data.get("walk_type"),
    )
    await db.publish_order(order_id)

    title = order_title(data["service"], data.get("walk_type"))
    card = (
        f"{title}\n"
        f"Заказ #{order_id}\n"
        f"Клиент: {cq.from_user.full_name} @{cq.from_user.username}\n"
        f"{data['pet_name']} • {data['pet_size']} • {data['duration_min']} мин\n"
        f"Район: {data['area']}\n"
        f"Когда: {data['when_at'].astimezone(dt.timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}\n"
        f"Адрес: {data['address']}\n"
        f"Бюджет: {data.get('budget') if data.get('budget') is not None else '—'}\n"
        f"Комментарий: {data.get('comment') or '—'}\n"
    )

    await send_order_to_walkers_by_area(card, None, order_id, data["area"])

    await cq.message.edit_text(f"Заявка #{order_id} создана ✅ Я разослал её исполнителям.")
    await state.clear()
    await cq.answer()

# === последний шаг мастера: комментарий → подтверждение ===

@dp.message(OrderStates.budget, F.text)
async def step_budget(m: Message, state: FSMContext):
    val = _clean_int(m.text)
    if val is None or val < 0 or val > 1_000_000:
        return await m.reply("Бюджет — неотрицательное число. Или набери /skip, если не важно.")
    await state.update_data(budget=val)
    await state.set_state(OrderStates.comment)
    await m.answer("Комментарий для исполнителя (опционально). /skip если нечего добавить.")


@dp.message(OrderStates.comment, F.text)
async def step_comment(m: Message, state: FSMContext):
    await state.update_data(comment=m.text.strip()[:500])
    data = await state.get_data()
    await _confirm_order(m, state, data)

# ====================== Отклики исполнителей ======================
@dp.callback_query(F.data.startswith("pr:"))
async def cb_proposal_start(cq: CallbackQuery, state: FSMContext):
    order_id = int(cq.data.split(":", 1)[1])
    if not await require_walker(cq.from_user.id):
        kb = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="👤 Стать исполнителем", callback_data="become:walker")]
        ])
        await cq.message.reply("Откликаться могут только исполнители (walker).", reply_markup=kb)
        return await cq.answer()

    await state.set_state(ProposalStates.waiting_price)
    await state.update_data(order_id=order_id)
    await cq.message.reply(f"Отклик на заказ #{order_id}. Ваша цена (числом)?")
    await cq.answer()

@dp.callback_query(F.data == "become:walker")
async def cb_become_walker(cq: CallbackQuery):
    await db.set_user_role(cq.from_user.id, "walker")
    await cq.message.reply("Готово. Теперь у тебя роль walker. Можно откликаться.")
    await cq.answer()

@dp.message(ProposalStates.waiting_price, F.text)
async def proposal_price(m: Message, state: FSMContext):
    txt = (m.text or "").strip().replace(" ", "")
    if not txt.isdigit():
        return await m.reply("Цена должна быть числом, без пробелов. Ещё раз:")
    await state.update_data(price=int(txt))
    await state.set_state(ProposalStates.waiting_note)
    await m.reply("Короткий комментарий (опционально).")

@dp.message(ProposalStates.waiting_note, F.text)
async def proposal_note(m: Message, state: FSMContext):
    data = await state.get_data()
    order_id = data["order_id"]
    price = data["price"]
    note = m.text.strip() or None

    prop_id = await db.add_proposal(order_id, m.from_user.id, price, note)

    order = await db.get_order(order_id)
    client_id = order["client_id"]
    walker_tag = f"{m.from_user.full_name} @{m.from_user.username}" if m.from_user.username else f"{m.from_user.full_name}"
    msg = (
        f"📝 Новый отклик на заказ #{order_id}\n"
        f"Исполнитель: {walker_tag}\n"
        f"Цена: {price}\n"
        f"Комментарий: {note or '—'}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="✅ Выбрать этого исполнителя", callback_data=f"choose:{order_id}:{m.from_user.id}")],
        [InlineKeyboardButton(text="👀 Все кандидаты", callback_data=f"cands:{order_id}")],
    ])
    try:
        await bot.send_message(chat_id=client_id, text=msg, reply_markup=kb)
    except Exception as e:
        logging.warning("notify client failed: %s", e)

    await m.reply(f"Отклик отправлен (#{prop_id}). Ждите решения клиента.")
    await state.clear()

# ====================== Просмотр заказов и выбор исполнителя ======================
@dp.message(Command("my_orders"))
async def my_orders(m: Message):
    orders = await db.list_orders_by_client(m.from_user.id)
    if not orders:
        return await m.answer("Пока заказов нет. Создай новый через меню «Услуги для собак».")
    for o in orders[:10]:
        title = order_title(o["service"], o.get("walk_type"))
        text = (
            f"{title}\n"
            f"#{o['id']} • статус: {o['status']}\n"
            f"{o.get('comment') or ''}".strip()
        )
        kb = kb_order_candidates(o["id"])
        await m.answer(text, reply_markup=kb)

@dp.callback_query(F.data.startswith("cands:"))
async def cb_candidates(cq: CallbackQuery):
    order_id = int(cq.data.split(":", 1)[1])
    props = await db.list_proposals(order_id)
    if not props:
        await cq.message.reply(f"На заказ #{order_id} пока нет откликов.")
        return await cq.answer()
    lines = []
    rows = []
    for p in props[:20]:
        rate = f", ставка {p['rate']}₽/ч" if p.get("rate") else ""
        phone = f", {p['phone']}" if p.get("phone") else ""
        note = p.get("note") or '—'
        name = p.get("full_name") or f"id {p['walker_id']}"
        username = f"@{p['username']}" if p.get("username") else ""

        lines.append(f"• {name} {username}{rate}{phone} — {p['price']}₽ — {note}")

        rows.append([InlineKeyboardButton(text=f"✅ Выбрать {name}", callback_data=f"choose:{order_id}:{p['walker_id']}")])
    await cq.message.reply(f"Кандидаты на #{order_id}:\n" + "\n".join(lines),
                           reply_markup=InlineKeyboardMarkup(inline_keyboard=rows))
    await cq.answer()

@dp.callback_query(F.data.startswith("choose:"))
async def cb_choose(cq: CallbackQuery):
    _, oid, wid = cq.data.split(":")
    order_id = int(oid); walker_id = int(wid)
    ok = await db.assign_walker(order_id, walker_id)
    if not ok:
        await cq.message.reply("Не удалось назначить: заказ уже не в статусе open/published.")
        return await cq.answer()
    order = await db.get_order(order_id)
    client_id = order["client_id"]
    await cq.message.reply(f"Исполнитель назначен на заказ #{order_id}.")
    try:
        await bot.send_message(client_id, f"✅ Исполнитель назначен (id {walker_id}). Свяжитесь друг с другом.")
        await bot.send_message(walker_id, f"✅ Вы назначены исполнителем на заказ #{order_id}. Клиент: id {client_id}.")
    except Exception as e:
        logging.warning("notify parties failed: %s", e)
    await cq.answer()

# ====================== Прочее меню ======================
@dp.message(F.text == "📞 Позвать менеджера")
async def on_call_manager(m: Message):
    if not settings.DISPATCHER_CHAT_ID:
        return await m.answer("Чат менеджеров не настроен. Добавь DISPATCHER_CHAT_ID в .env")
    await bot.send_message(settings.DISPATCHER_CHAT_ID,
                           f"📞 Запрос менеджера: {m.from_user.full_name} @{m.from_user.username} (id {m.from_user.id})")
    await m.answer("Зову менеджера. Он свяжется с тобой в лс.")

@dp.message(F.text == "❓ Общие вопросы")
async def on_faq(m: Message):
    await m.answer("FAQ прикрутим позже. Сейчас главный сценарий — заявки/отклики/выбор.")

@dp.message()
async def fallback(m: Message):
    await m.answer("Ткни в меню ниже, не забивай голову 🙂", reply_markup=main_menu())


# ====================== main ======================
async def main():
    await db.init_db()
    await dp.start_polling(bot)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        print("Пока!")


