CREATE KEYSPACE IF NOT EXISTS warehouse

WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

-- Остаток товара в конкретной зоне
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product_zone (
    product_id text,
    zone_id    text,
    available  int,
    reserved   int,
    PRIMARY KEY ((product_id, zone_id))
);

-- Все зоны одного товара
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_product (
    product_id text,
    zone_id    text,
    available  int,
    reserved   int,
    PRIMARY KEY (product_id, zone_id)
);

-- Все товары в конкретной зоне
CREATE TABLE IF NOT EXISTS warehouse.inventory_by_zone (
    zone_id    text,
    product_id text,
    available  int,
    reserved   int,
    PRIMARY KEY (zone_id, product_id)
);

-- Обработанные event_id (для идемпотентности)
CREATE TABLE IF NOT EXISTS warehouse.processed_events (
    event_id    text PRIMARY KEY,
    processed_at timestamp
);

-- Последний timestamp на пару (product, zone).
CREATE TABLE IF NOT EXISTS warehouse.last_event_timestamp (
    product_id text,
    zone_id    text,
    last_ts    text,
    PRIMARY KEY ((product_id, zone_id))
);

-- Заказы
CREATE TABLE IF NOT EXISTS warehouse.orders (
    order_id   text PRIMARY KEY,
    status     text,
    items_json text,
    created_at timestamp,
    updated_at timestamp
);

-- Аудит-лог по товару
CREATE TABLE IF NOT EXISTS warehouse.event_history (
    product_id text,
    event_ts   text,
    event_id   text,
    event_type text,
    payload    text,
    PRIMARY KEY (product_id, event_ts, event_id)
) WITH CLUSTERING ORDER BY (event_ts DESC, event_id ASC);
