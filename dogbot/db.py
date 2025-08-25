"""
Async слой доступа к БД (SQLAlchemy Core + async engine).
Таблицы: users, orders, proposals, assignments.
"""

from __future__ import annotations
import datetime as dt
from typing import Any, Dict, List, Optional

from sqlalchemy.ext.asyncio import create_async_engine, AsyncEngine, AsyncConnection
from sqlalchemy import text
from dogbot.settings import settings
from sqlalchemy.exc import OperationalError

# ленивый engine
_engine: Optional[AsyncEngine] = None


def get_engine() -> AsyncEngine:
    global _engine
    if _engine is None:
        _engine = create_async_engine(settings.DATABASE_URL, future=True, pool_pre_ping=True)
    return _engine


# --------------------- DDL ---------------------
INIT_SQL_POSTGRES = """
CREATE TABLE IF NOT EXISTS users (
    tg_id      BIGINT PRIMARY KEY,
    role       TEXT NOT NULL DEFAULT 'client',
    username   TEXT,
    full_name  TEXT,
    phone      TEXT
);

CREATE TABLE IF NOT EXISTS orders (
    id           SERIAL PRIMARY KEY,
    client_id    BIGINT NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
    service      TEXT   NOT NULL,
    walk_type    TEXT,
    pet_name     TEXT   NOT NULL,
    pet_size     TEXT   NOT NULL,
    when_at      TIMESTAMPTZ NOT NULL,
    duration_min INT     NOT NULL CHECK (duration_min > 0),
    address      TEXT    NOT NULL,
    budget       INT,
    area         TEXT,
    comment      TEXT,
    status       TEXT    NOT NULL DEFAULT 'open',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_orders_client_status ON orders (client_id, status);

CREATE TABLE IF NOT EXISTS proposals (
    id         SERIAL PRIMARY KEY,
    order_id   INT    NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
    walker_id  BIGINT NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
    price      INT    NOT NULL,
    area       TEXT,
    note       TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(order_id, walker_id)
);
CREATE INDEX IF NOT EXISTS ix_proposals_order ON proposals (order_id);
CREATE INDEX IF NOT EXISTS ix_proposals_walker ON proposals (walker_id);

CREATE TABLE IF NOT EXISTS assignments (
    order_id   INT    PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
    walker_id  BIGINT NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
    assigned_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE TABLE IF NOT EXISTS walker_profiles (
    walker_id  BIGINT PRIMARY KEY REFERENCES users(tg_id) ON DELETE CASCADE,
    phone      TEXT,
    city       TEXT,
    areas      TEXT,  -- районы/локации (через запятую)
    experience TEXT,  -- опыт
    price_from INT,   -- базовая ставка от
    bio        TEXT,
    is_approved BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
"""

# SQLite не знает SERIAL/TIMESTAMPTZ/NOW(), делаем эквиваленты
INIT_SQL_SQLITE = [
    """
    CREATE TABLE IF NOT EXISTS users (
        tg_id      INTEGER PRIMARY KEY,
        role       TEXT NOT NULL DEFAULT 'client',
        username   TEXT,
        full_name  TEXT,
        phone      TEXT
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        id           INTEGER PRIMARY KEY AUTOINCREMENT,
        client_id    INTEGER NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
        service      TEXT   NOT NULL,
        walk_type    TEXT,
        pet_name     TEXT   NOT NULL,
        pet_size     TEXT   NOT NULL,
        when_at      TEXT   NOT NULL, -- ISO-строка времени
        duration_min INTEGER NOT NULL CHECK (duration_min > 0),
        address      TEXT    NOT NULL,
        budget       INTEGER,
        area         TEXT,
        comment      TEXT,
        status       TEXT    NOT NULL DEFAULT 'open',
        created_at   TEXT    NOT NULL DEFAULT (datetime('now'))
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_orders_client_status ON orders (client_id, status);",
    """
    CREATE TABLE IF NOT EXISTS proposals (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        order_id   INTEGER NOT NULL REFERENCES orders(id) ON DELETE CASCADE,
        walker_id  INTEGER NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
        price      INTEGER NOT NULL,
        note       TEXT,
        created_at TEXT NOT NULL DEFAULT (datetime('now')),
        UNIQUE(order_id, walker_id)
    );
    """,
    "CREATE INDEX IF NOT EXISTS ix_proposals_order ON proposals (order_id);",
    "CREATE INDEX IF NOT EXISTS ix_proposals_walker ON proposals (walker_id);",
    """
    CREATE TABLE IF NOT EXISTS assignments (
        order_id    INTEGER PRIMARY KEY REFERENCES orders(id) ON DELETE CASCADE,
        walker_id   INTEGER NOT NULL REFERENCES users(tg_id) ON DELETE CASCADE,
        assigned_at TEXT NOT NULL DEFAULT (datetime('now'))
    );
    CREATE TABLE IF NOT EXISTS walker_profiles (
    walker_id   INTEGER PRIMARY KEY REFERENCES users(tg_id) ON DELETE CASCADE,
    phone       TEXT,
    city        TEXT,
    areas       TEXT,
    experience  TEXT,
    price_from  INTEGER,
    bio         TEXT,
    is_approved INTEGER NOT NULL DEFAULT 0,
    created_at  TEXT NOT NULL DEFAULT (datetime('now'))
);
    """,
]


async def _exec(conn: AsyncConnection, sql: str, params: Dict[str, Any] | None = None):
    return await conn.execute(text(sql), params or {})


# --------------------- API ---------------------
async def init_db() -> None:
    """
    Создать таблицы, если их нет, с учётом диалекта.
    После DDL — «ленивая миграция»: пытаемся добавить колонку orders.area,
    если она уже есть — молча игнорируем.
    """
    from sqlalchemy.exc import OperationalError

    engine = get_engine()
    backend = engine.url.get_backend_name()

    async with engine.begin() as conn:
        # --- базовый DDL ---
        if backend == "sqlite":
            # SQLite: каждый стейтмент по отдельности
            for stmt in INIT_SQL_SQLITE:
                await _exec(conn, stmt)
        else:
            # Postgres (и прочие): можно порезать multi-statement по ';'
            for stmt in (s.strip() for s in INIT_SQL_POSTGRES.split(";")):
                if not stmt:
                    continue
                await _exec(conn, stmt + ";")

        # --- «ленивая миграция»: добавить колонку orders.area ---
        # в обоих диалектах ADD COLUMN без DEFAULT и NOT NULL — безопасно
        try:
            await _exec(conn, "ALTER TABLE orders ADD COLUMN area TEXT;")
        except OperationalError:
            # колонка уже есть или диалект ворчит — не страшно
            pass
        except Exception:
            # на всякий пожарный — не роняем и идём дальше
            pass
        


async def upsert_user(
    tg_id: int,
    username: Optional[str],
    full_name: Optional[str],
    phone: Optional[str] = None,
    role: str = "client",
) -> None:
    sql = """
    INSERT INTO users (tg_id, role, username, full_name, phone)
    VALUES (:tg_id, :role, :username, :full_name, :phone)
    ON CONFLICT (tg_id) DO UPDATE SET
        role = EXCLUDED.role,
        username = COALESCE(EXCLUDED.username, users.username),
        full_name = COALESCE(EXCLUDED.full_name, users.full_name),
        phone = COALESCE(EXCLUDED.phone, users.phone);
    """
    engine = get_engine()
    async with engine.begin() as conn:
        await _exec(conn, sql, {
            "tg_id": tg_id,
            "role": role,
            "username": username,
            "full_name": full_name,
            "phone": phone,
        })



async def add_order(
    client_id: int,
    service: str,
    pet_name: str,
    pet_size: str,
    when_at: dt.datetime,
    duration_min: int,
    address: str,
    budget: Optional[int],
    comment: Optional[str],
    walk_type: Optional[str] = None,
    area: Optional[str] = None,   # <— НОВОЕ
) -> int:
    sql = """
    INSERT INTO orders (client_id, service, walk_type, pet_name, pet_size,
                        when_at, duration_min, address, budget, comment, area)
    VALUES (:client_id, :service, :walk_type, :pet_name, :pet_size,
            :when_at, :duration_min, :address, :budget, :comment, :area)
    RETURNING id;
    """
    engine = get_engine()
    async with engine.begin() as conn:
        res = await _exec(conn, sql, {
            "client_id": client_id,
            "service": service,
            "walk_type": walk_type,
            "pet_name": pet_name,
            "pet_size": pet_size,
            "when_at": when_at,
            "duration_min": duration_min,
            "address": address,
            "budget": budget,
            "comment": comment,
            "area": area,  # <— НОВОЕ
        })
        return int(res.scalar_one())


async def publish_order(order_id: int) -> None:
    sql = "UPDATE orders SET status='published' WHERE id=:oid;"
    engine = get_engine()
    async with engine.begin() as conn:
        await _exec(conn, sql, {"oid": order_id})


async def list_orders_by_client(client_id: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT id, service, walk_type, pet_name, pet_size,
           when_at, duration_min, address, budget, comment, status
    FROM orders
    WHERE client_id=:cid
    ORDER BY id DESC;
    """
    engine = get_engine()
    async with engine.connect() as conn:
        res = await _exec(conn, sql, {"cid": client_id})
        return [dict(row) for row in res.mappings().all()]


async def get_order(order_id: int) -> Optional[Dict[str, Any]]:
    sql = "SELECT * FROM orders WHERE id=:oid;"
    engine = get_engine()
    async with engine.connect() as conn:
        res = await _exec(conn, sql, {"oid": order_id})
        row = res.mappings().first()
        return dict(row) if row else None


async def add_proposal(order_id: int, walker_id: int, price: int, note: Optional[str]) -> int:
    sql = """
    INSERT INTO proposals (order_id, walker_id, price, note)
    VALUES (:oid, :wid, :price, :note)
    ON CONFLICT (order_id, walker_id)
    DO UPDATE SET price=EXCLUDED.price, note=EXCLUDED.note
    RETURNING id;
    """
    engine = get_engine()
    async with engine.begin() as conn:
        res = await _exec(conn, sql, {"oid": order_id, "wid": walker_id, "price": price, "note": note})
        return int(res.scalar_one())


async def list_proposals(order_id: int) -> List[Dict[str, Any]]:
    sql = """
    SELECT p.id, p.price, p.note, p.walker_id,
           u.username, u.full_name,
           wp.phone, wp.rate, wp.areas
    FROM proposals p
    LEFT JOIN users u ON u.tg_id = p.walker_id
    LEFT JOIN walker_profiles wp ON wp.walker_id = p.walker_id
    WHERE p.order_id=:oid
    ORDER BY p.price ASC, p.id ASC;
    """
    engine = get_engine()
    async with engine.connect() as conn:
        res = await _exec(conn, sql, {"oid": order_id})
        return [dict(r) for r in res.mappings().all()]


async def assign_walker(order_id: int, walker_id: int) -> bool:
    """Назначение исполнителя. В Postgres лочим строку, в SQLite — без FOR UPDATE (тестовый режим)."""
    engine = get_engine()
    backend = engine.url.get_backend_name()
    lock_sql = (
        "SELECT status FROM orders WHERE id=:oid FOR UPDATE;"
        if backend != "sqlite"
        else "SELECT status FROM orders WHERE id=:oid;"
    )

    async with engine.begin() as conn:
        # 1) проверяем статус (и лочим в PG)
        chk = await _exec(conn, lock_sql, {"oid": order_id})
        row = chk.mappings().first()
        if not row or row["status"] not in ("open", "published"):
            return False

        # 2) создаём/обновляем назначение
        await _exec(
            conn,
            """
            INSERT INTO assignments (order_id, walker_id)
            VALUES (:oid, :wid)
            ON CONFLICT (order_id) DO UPDATE SET walker_id=EXCLUDED.walker_id;
            """,
            {"oid": order_id, "wid": walker_id},
        )
        # 3) переводим заказ в assigned
        await _exec(conn, "UPDATE orders SET status='assigned' WHERE id=:oid;", {"oid": order_id})
        return True
    

async def mark_done(order_id: int) -> None:
    engine = get_engine()
    async with engine.begin() as conn:
        await _exec(conn, "UPDATE orders SET status='done' WHERE id=:oid;", {"oid": order_id})

async def get_user_role(tg_id: int) -> str | None:
    sql = "SELECT role FROM users WHERE tg_id=:uid;"
    engine = get_engine()
    async with engine.connect() as conn:
        res = await _exec(conn, sql, {"uid": tg_id})
        row = res.mappings().first()
        return row["role"] if row else None

async def set_user_role(tg_id: int, role: str) -> None:
    # роль только из фиксированного списка
    if role not in ("client", "walker", "admin"):
        raise ValueError("bad role")
    sql = """
    INSERT INTO users (tg_id, role) VALUES (:uid, :role)
    ON CONFLICT (tg_id) DO UPDATE SET role=EXCLUDED.role;
    """
    engine = get_engine()
    async with engine.begin() as conn:
        await _exec(conn, sql, {"uid": tg_id, "role": role})

async def list_walkers_ids() -> list[int]:
    sql = "SELECT tg_id FROM users WHERE role='walker';"
    engine = get_engine()
    async with engine.connect() as conn:
        res = await _exec(conn, sql)
        return [row[0] for row in res.fetchall()]


async def upsert_walker_profile(
    walker_id: int,
    phone: str | None,
    city: str | None,
    areas: str | None,
    experience: str | None,
    price_from: int | None,
    bio: str | None,
) -> None:
    sql = """
    INSERT INTO walker_profiles (walker_id, phone, city, areas, experience, price_from, bio)
    VALUES (:wid, :phone, :city, :areas, :experience, :price_from, :bio)
    ON CONFLICT (walker_id) DO UPDATE SET
        phone=EXCLUDED.phone,
        city=EXCLUDED.city,
        areas=EXCLUDED.areas,
        experience=EXCLUDED.experience,
        price_from=EXCLUDED.price_from,
        bio=EXCLUDED.bio;
    """
    engine = get_engine()
    async with engine.begin() as conn:
        await _exec(conn, sql, {
            "wid": walker_id,
            "phone": phone, "city": city, "areas": areas,
            "experience": experience, "price_from": price_from, "bio": bio
        })

async def set_walker_approval(walker_id: int, approved: bool) -> None:
    sql = "UPDATE walker_profiles SET is_approved=:ap WHERE walker_id=:wid;"
    engine = get_engine()
    async with engine.begin() as conn:
        await _exec(conn, sql, {"ap": approved, "wid": walker_id})

async def get_walker_profile(walker_id: int) -> dict | None:
    sql = """
    SELECT u.tg_id, u.username, u.full_name, u.role,
           p.phone, p.city, p.areas, p.experience, p.price_from, p.bio, p.is_approved
    FROM users u
    LEFT JOIN walker_profiles p ON p.walker_id = u.tg_id
    WHERE u.tg_id=:wid;
    """
    engine = get_engine()
    async with engine.connect() as conn:
        res = await _exec(conn, sql, {"wid": walker_id})
        row = res.mappings().first()
        return dict(row) if row else None

async def list_walkers_ids() -> list[int]:
    # теперь только одобренные
    sql = "SELECT walker_id FROM walker_profiles WHERE is_approved=1;"
    engine = get_engine()
    async with engine.connect() as conn:
        res = await _exec(conn, sql)
        return [r[0] for r in res.fetchall()]

async def list_walkers_by_area(area: str) -> list[int]:
    """
    Очень простой матч: ищем area как подстроку в wp.areas (CSV/текст).
    Для продакшена делаем нормализацию/таблицу.
    """
    sql = "SELECT u.tg_id FROM users u JOIN walker_profiles wp ON wp.walker_id=u.tg_id WHERE u.role='walker' AND wp.areas ILIKE :a;"
    # В SQLite нет ILIKE — подменим на LIKE с lower()
    engine = get_engine()
    backend = engine.url.get_backend_name()
    if backend == "sqlite":
        sql = "SELECT u.tg_id FROM users u JOIN walker_profiles wp ON wp.walker_id=u.tg_id WHERE u.role='walker' AND lower(wp.areas) LIKE lower(:a);"
    a = f"%{area}%"
    async with engine.connect() as conn:
        res = await _exec(conn, sql, {"a": a})
        return [row[0] for row in res.fetchall()]