import os, importlib, pytest

@pytest.mark.asyncio
async def test_walker_profile_crud(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "sqlite+aiosqlite:///:memory:")
    from dogbot import db
    importlib.reload(db)
    await db.init_db()

    await db.upsert_user(99, "u99", "User 99", role="walker")
    await db.upsert_walker_profile(99, phone="+7999", bio="Опыт 3 года, 600", rate=600, areas="Центр, Купчино")

    p = await db.get_walker_profile(99)
    assert p and p["phone"] == "+7999" and p["rate"] == 600
