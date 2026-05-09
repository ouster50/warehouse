# Smart Warehouse: Event-Driven State Management

## Запуск

```bash
docker-compose up --build -d
```

## Запуск тестов
```bash
python3 -m pytest tests/ -v
```

Обоснование выбора ключей
inventory_by_product_zone — PRIMARY KEY ((product_id, zone_id))
Составной ключ обеспечивает быстрый поиск за O(1) по конкретному товару в конкретной зоне.

inventory_by_product — PRIMARY KEY (product_id, zone_id)

Partition key = product_id, clustering key = zone_id.
Все зоны товара хранятся в одной партиции, поэтому один запрос по product_id возвращает все зоны, в которых есть этот товар
inventory_by_zone — PRIMARY KEY (zone_id, product_id)

Partition key = zone_id, clustering key = product_id.
Легко получать все товары зоны в рамках одной партиции

processed_events — PRIMARY KEY (event_id)
Обеспечивает идемпотентность: быстрая проверка по event_id.

last_event_timestamp — PRIMARY KEY ((product_id, zone_id))
Отслеживание последнего обработанного timestamp для пары (товар, зона).
Используется для игнорирования событий, пришедших не по порядку.

orders — PRIMARY KEY (order_id)

Хранение заказов с их позициями (денормализовано как JSON).
event_history — PRIMARY KEY (product_id, event_ts, event_id)

Аудит-лог по товару с обратной сортировкой по времени.
Денормализация
Три таблицы инвентаря (_by_product_zone, _by_product, _by_zone) содержат
одни и те же данные, денормализованные под разные паттерны запросов.
Атомарность обновления обеспечивается Cassandra Logged Batch.

Архитектурные решения
At-least-once — enable.auto.commit=false, commit после записи в Cassandra
Идемпотентность — таблица processed_events, проверка перед обработкой
Консистентность — Logged Batch для атомарного обновления 3 таблиц
Out-of-order — Сравнение ISO timestamp с last_event_timestamp
DLQ	Ошибки — warehouse-events-dlq с метаданными
