import os, importlib, datetime as dt, pytest

@pytest.mark.asyncio
async def test_area_broadcast_filter(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    monkeypatch.setenv("BOT_TOKEN", "12345:TEST")

    from dogbot import db
    importlib.reload(db)
    await db.init_db()

    # двое исполнителей
    await db.upsert_user(10, "walker1", "W1", role="walker")
    await db.upsert_walker_profile(10, phone="+7000", bio="ok", rate=500, areas="Купчино, Центр")

    await db.upsert_user(11, "walker2", "W2", role="walker")
    await db.upsert_walker_profile(11, phone="+7001", bio="ok", rate=600, areas="Петроградка")

    # заказ в Купчино
    when = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=2)
    order_id = await db.add_order(
        client_id=1, service="walk", pet_name="Бублик", pet_size="medium",
        when_at=when, duration_min=60, address="ул. X", budget=1000, comment="",
        walk_type="normal", area="Купчино"
    )
    await db.publish_order(order_id)

    # кто должен получить
    ids = await db.list_walkers_by_area("Купчино")
    assert set(ids) == {10}
