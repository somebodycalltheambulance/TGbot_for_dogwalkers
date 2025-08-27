import os, importlib, pytest, datetime as dt, asyncio

@pytest.mark.asyncio
async def test_send_by_area(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("BOT_TOKEN", "12345:TEST")

    from dogbot import db, bot as bot_mod
    importlib.reload(db); importlib.reload(bot_mod)
    await db.init_db()

    # walker'ы
    await db.upsert_user(1, "w1", "W1", role="walker")
    await db.upsert_walker_profile(1, phone="+7", bio="", rate=500, areas="Купчино")
    await db.upsert_user(2, "w2", "W2", role="walker")
    await db.upsert_walker_profile(2, phone="+7", bio="", rate=600, areas="Петроградка")

    sent = []
    async def fake_send_message(chat_id, text, reply_markup=None):
        sent.append(("msg", chat_id, text))
    async def fake_send_photo(chat_id, photo, caption, reply_markup=None):
        sent.append(("photo", chat_id, caption))

    monkeypatch.setattr(bot_mod.bot, "send_message", fake_send_message)
    monkeypatch.setattr(bot_mod.bot, "send_photo", fake_send_photo)

    # заказ с районом Купчино
    when = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=2)
    oid = await db.add_order(3, "walk", "Бублик", "medium", when, 60, "ул.", 1000, "", "normal", area="Купчино")
    await db.publish_order(oid)

    await bot_mod.send_order_to_walkers_by_area("CARD", None, oid, "Купчино")

    assert {x[1] for x in sent} == {1}  # только W1
