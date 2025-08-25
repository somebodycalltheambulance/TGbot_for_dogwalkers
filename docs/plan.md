# План-чеклист проекта DogBot


Этап 0. Цели и роли
* [ ] Роли: client, walker, admin.
* [ ] KPI MVP: время от заявки до первого отклика < 10 мин; конверсия «заявка→назначение» ≥ 50%.
* [ ] Список чатов: DISPATCHER_CHAT_ID (менеджеры), WALKERS_CHAT_ID (выгульщики).

Этап 1. Скелет проекта
* [ ] Структура:
  tgbot/
    dogbot/{__init__.py, bot.py, states.py, keyboards.py, texts.py, db.py, settings.py}
    tests/{__init__.py, test_fsm.py, test_db.py}
    .env, requirements.txt, pytest.ini, Dockerfile, docker-compose.yml
* [ ] requirements.txt: aiogram, python-dotenv, sqlalchemy[asyncio], asyncpg, pytest, pytest-asyncio.
* [ ] settings.py: чтение .env (BOT_TOKEN, DATABASE_URL, DISPATCHER_CHAT_ID, WALKERS_CHAT_ID, ADMIN_IDS).
* [ ] Логирование (logging.INFO), префиксы в логах (user_id, order_id).

Этап 2. БД и миграции
* [ ] Схема (Postgres):
  - users(tg_id PK, role, username, full_name, phone)
  - orders(id PK, client_id FK, service, walk_type?, pet_name, pet_size, when_at, duration_min, address, budget, comment, status ENUM: open/published/assigned/done/cancelled, created_at)
  - proposals(id PK, order_id FK, walker_id FK, price, note, created_at, UNIQUE(order_id, walker_id))
  - assignments(order_id PK FK, walker_id FK, assigned_at)
  - (опц.) reviews(id PK, order_id FK, rating, text)
* [ ] Alembic (или init SQL в db.py на MVP).
* [ ] Индексы: orders(client_id, status), proposals(order_id), proposals(walker_id).
* [ ] Триггеры/проверки: duration_min > 0, budget >= 0.

Этап 3. Бот: базовое меню и FSM
* [ ] Главное меню (reply): «🐶 Услуги для собак», «👤 Работать у нас», «📞 Менеджер», «❓ FAQ».
* [ ] FSM для заказов: choosing_service → (walk_type) → collecting_description → confirming.
* [ ] FSM для анкеты исполнителя: name → phone → experience (+фото/сертификаты – позже).
* [ ] Команды: /start, /help, /my_orders, /state (для отладки).

Этап 4. Регистрация/роли/безопасность
* [ ] При первом /start — создать users (upsert), роль по умолчанию client.
* [ ] ADMIN_IDS из .env — белый список.
* [ ] Команды админа: /set_role <tg_id> <role>, /ban_walker <tg_id> (MVP: soft-ban в памяти/таблице).
* [ ] Rate limiting на спам (простой in-memory/TTL по user_id).

Этап 5. Создание заказа (клиент)
* [ ] Сбор полей: услуга (walk/boarding/nanny), тип выгула (normal/active), описание (имя/порода/размер/характер), дата/время, длительность, адрес, бюджет, фото.
* [ ] Парсинг времени: правила (форматы YYYY-MM-DD HH:MM, «сегодня 19:00», «завтра 10:30») — на MVP строгое поле.
* [ ] Валидация: адрес не пустой, время в будущем, длительность > 0.
* [ ] Сохранение orders.status = open.

Этап 6. Публикация заказа выгульщикам
* [ ] Пост в WALKERS_CHAT_ID с карточкой заказа + inline-кнопка «Откликнуться» (callback pr:order_id).
* [ ] Дублирование фото/описания.
* [ ] Перевод заказа в status = published (для аналитики).
* [ ] Антидубль: если уже published — не постить ещё раз.

Этап 7. Отклик исполнителя
* [ ] При нажатии «Откликнуться» — проверка роли walker, если нет — вежливо послать в «Работать у нас».
* [ ] Мини-диалог: цена → комментарий → proposals.insert(...) (UNIQUE); обработать «уже откликался».
* [ ] Уведомить клиента/менеджера о новом отклике (опц. только клиента).

Этап 8. Просмотр откликов и выбор исполнителя (клиент)
* [ ] /my_orders → список активных заказов (open/published/assigned).
* [ ] Карточка заказа: все предложения (цена, комментарий, рейтинг исполнителя — позже) + кнопки «Выбрать <walker_name>».
* [ ] При выборе:
  - транзакция: создать assignments, обновить orders.status = assigned.
  - отправить контакты обоим (username/phone), детали заказа.
  - всем прочим откликнувшимся — «Заказ назначен другому, спасибо».

Этап 9. Исполнение и завершение
* [ ] У исполнителя кнопка «Начать» (опц., для логирования времени).
* [ ] Кнопка «Завершить» → orders.status = done.
* [ ] Запрос отзыва у клиента (1–5 ⭐ + текст) → таблица reviews.

Этап 10. Менеджерские функции (минимум)
* [ ] Кнопка «📞 Менеджер» — уведомление в DISPATCHER_CHAT_ID (кто, что, ссылка).
* [ ] Команда /orders_open — список открытых/без откликов.
* [ ] Эскалация: если нет откликов N минут → напомнить в DISPATCHER_CHAT_ID.

Этап 11. Тестирование
* [ ] tests/test_fsm.py — шаги состояний (как уже начали).
* [ ] tests/test_db.py — CRUD заказов/откликов (использовать тестовую БД/транзакции с откатами).
* [ ] (Опц.) Тест хендлеров с mock Bot (проверка вызываемых методов send_message, send_photo).

Этап 12. Деплой и эксплуатация
* [ ] docker-compose.yml: bot, postgres, (опц.) adminer.
* [ ] Миграции применяются на старте (init скрипт).
* [ ] Production конфиг: autorestart (systemd/Docker), лог-ротация, таймауты на сетевые вызовы.
* [ ] Бэкапы Postgres (cron + pg_dump).
* [ ] Мониторинг: health-пинг /health (просто sendChatAction себе), алерты в DISPATCHER_CHAT_ID.
* [ ] Sentry/логирование ошибок (по желанию).

Этап 13. Безопасность и приватность
* [ ] Ограничить админ-команды по ADMIN_IDS.
* [ ] Все SQL — параметризованные запросы.
* [ ] Маскировать телефон в логах.
* [ ] GDPR-минимум: по запросу удалить данные (/gdpr_delete – позже).
* [ ] Антиспам и антиддос (rate limit на пользователя/чат).

Этап 14. Контент и UX
* [ ] Тексты FAQ (цены, оплата, ключи, аптечка, кормление, договор).
* [ ] Шаблоны карточек: внятные эмодзи, форматирование, краткость.
* [ ] Ошибки: дружелюбные сообщения (а не трейсбеки).

Этап 15. Дорога вперёд (после MVP)
* [ ] Гео и покрытие районов (walker availability + фильтр по гео).
* [ ] Планировщик слотов, календарь исполнителя.
* [ ] Предоплата/оплата (LNP/Stripe/YooKassa — что ближе).
* [ ] Авто-подбор исполнителя (скор, ближайший, рейтинг).
* [ ] Рейтинг исполнителей, верификация документов.
* [ ] Отчёт по прогулке (фото, GPS-трек, заметки).

Мини-DOD (Definition of Done) для MVP
* [ ] Клиент может создать заказ из меню, заявка улетает в чат выгульщиков.
* [ ] Выгульщик откликается; запись появляется у клиента.
* [ ] Клиент выбирает исполнителя; обоим приходят контакты и детали.
* [ ] Менеджер получает «Позвать менеджера».
* [ ] Всё хранится в Postgres; после рестарта ничего не теряется.
* [ ] Есть базовые тесты FSM и DB; pytest -v зелёный.
