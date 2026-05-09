from cassandra.cqlengine import columns
from cassandra.cqlengine.models import Model


class InventoryByProductZone(Model):
    """Остаток товара в конкретной зоне"""
    __keyspace__ = "warehouse"
    __table_name__ = "inventory_by_product_zone"

    product_id = columns.Text(partition_key=True)
    zone_id = columns.Text(partition_key=True)
    available = columns.Integer(default=0)
    reserved = columns.Integer(default=0)


class InventoryByProduct(Model):
    """Все зоны одного товара"""
    __keyspace__ = "warehouse"
    __table_name__ = "inventory_by_product"

    product_id = columns.Text(partition_key=True)
    zone_id = columns.Text(primary_key=True, clustering_order="ASC")
    available = columns.Integer(default=0)
    reserved = columns.Integer(default=0)


class InventoryByZone(Model):
    """Все товары в конкретной зоне"""
    __keyspace__ = "warehouse"
    __table_name__ = "inventory_by_zone"

    zone_id = columns.Text(partition_key=True)
    product_id = columns.Text(primary_key=True, clustering_order="ASC")
    available = columns.Integer(default=0)
    reserved = columns.Integer(default=0)


class ProcessedEvent(Model):
    """Обработанные event_id (для идемпотентности)"""
    __keyspace__ = "warehouse"
    __table_name__ = "processed_events"

    event_id = columns.Text(primary_key=True)
    processed_at = columns.DateTime()


class LastEventTimestamp(Model):
    """Последний обработанный timestamp на пару (product_id, zone_id)"""
    __keyspace__ = "warehouse"
    __table_name__ = "last_event_timestamp"

    product_id = columns.Text(partition_key=True)
    zone_id = columns.Text(partition_key=True)
    last_ts = columns.Text()


class Order(Model):
    __keyspace__ = "warehouse"
    __table_name__ = "orders"

    order_id = columns.Text(primary_key=True)
    status = columns.Text()
    items_json = columns.Text()
    created_at = columns.DateTime()
    updated_at = columns.DateTime()


class EventHistory(Model):
    """Аудит-лог по товару"""
    __keyspace__ = "warehouse"
    __table_name__ = "event_history"

    product_id = columns.Text(partition_key=True)
    event_ts = columns.Text(primary_key=True, clustering_order="DESC")
    event_id = columns.Text(primary_key=True, clustering_order="ASC")
    event_type = columns.Text()
    payload = columns.Text()
