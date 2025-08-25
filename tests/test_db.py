import os
import datetime as dt
import importlib
import pytest

@pytest.mark.asyncio
async def test_db_crud(monkeypatch):
    # Гоним тесты на SQLite in-memory, чтобы не трогать Postgres
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")

    # Импорт после подмены env, чтобы engine собрался с правильным URL
    from dogbot import db
    importlib.reload(db)

    # init DDL
    await db.init_db()

    # пользователи
    await db.upsert_user(1, "client", "Client User")
    await db.upsert_user(2, "walker", "Walker User", role="walker")

    # заказ
    when = dt.datetime.now(dt.timezone.utc) + dt.timedelta(hours=1)
    order_id = await db.add_order(
        client_id=1,
        service="walk",
        pet_name="Бублик",
        pet_size="medium",
        when_at=when,
        duration_min=60,
        address="Some street, 1",
        budget=1000,
        comment="не любит кошек",
        walk_type="normal",
    )
    assert isinstance(order_id, int)

    orders = await db.list_orders_by_client(1)
    assert len(orders) == 1
    assert orders[0]["status"] == "open"

    # публикация + отклик
    await db.publish_order(order_id)
    prop_id = await db.add_proposal(order_id, 2, 1200, "Сделаю красиво")
    assert isinstance(prop_id, int)

    props = await db.list_proposals(order_id)
    assert len(props) == 1
    assert props[0]["price"] == 1200
    assert props[0]["walker_id"] == 2

    # назначение исполнителя
    ok = await db.assign_walker(order_id, 2)
    assert ok is True

    # завершение
    await db.mark_done(order_id)
    order = await db.get_order(order_id)
    assert order is not None
    assert order["status"] == "done"
