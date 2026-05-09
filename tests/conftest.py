import json
import os
import time
import uuid
from datetime import datetime, timezone

import pytest
from cassandra.cluster import Cluster
from confluent_kafka import Consumer, SerializingProducer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer

KAFKA = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SR_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CASS_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
TOPIC = "warehouse-events"
DLQ_TOPIC = "warehouse-events-dlq"

AVRO_SCHEMA = """{
  "type":"record","name":"WarehouseEvent","namespace":"com.warehouse",
  "fields":[
    {"name":"event_id","type":"string"},{"name":"event_type","type":"string"},
    {"name":"timestamp","type":"string"},
    {"name":"product_id","type":["null","string"],"default":null},
    {"name":"zone_id","type":["null","string"],"default":null},
    {"name":"from_zone_id","type":["null","string"],"default":null},
    {"name":"to_zone_id","type":["null","string"],"default":null},
    {"name":"quantity","type":["null","int"],"default":null},
    {"name":"order_id","type":["null","string"],"default":null},
    {"name":"order_items_json","type":["null","string"],"default":null},
    {"name":"counted_quantity","type":["null","int"],"default":null}
  ]
}"""


@pytest.fixture(scope="session")
def cass():
    cluster = Cluster(CASS_HOSTS)
    for _ in range(30):
        try:
            return cluster.connect("warehouse")
        except Exception:
            time.sleep(2)
    raise RuntimeError("Cassandra unavailable")


@pytest.fixture(scope="session")
def kp():
    sr = SchemaRegistryClient({"url": SR_URL})
    return SerializingProducer({
        "bootstrap.servers": KAFKA,
        "value.serializer": AvroSerializer(sr, AVRO_SCHEMA),
    })


def send(kp, event_type, event_id=None, timestamp=None, **kw):
    if "order_items" in kw:
        kw["order_items_json"] = json.dumps(kw.pop("order_items"))
    evt = {
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "timestamp": timestamp or datetime.now(timezone.utc).isoformat(),
        **{k: kw.get(k) for k in (
            "product_id", "zone_id", "from_zone_id", "to_zone_id",
            "quantity", "order_id", "order_items_json", "counted_quantity")},
    }
    kp.produce(topic=TOPIC, value=evt, key=evt.get("product_id") or evt.get("order_id") or "x")
    kp.flush()
    return evt["event_id"]


def wait(cass, eid, timeout=15):
    for _ in range(timeout * 5):
        if cass.execute("SELECT event_id FROM processed_events WHERE event_id=%s", (eid,)).one():
            return True
        time.sleep(0.2)
    return False


def inv(cass, pid, zid):
    r = cass.execute(
        "SELECT available, reserved FROM inventory_by_product_zone "
        "WHERE product_id=%s AND zone_id=%s", (pid, zid)).one()
    return (r.available or 0, r.reserved or 0) if r else (0, 0)


def inv_by_product(cass, pid):
    return {r.zone_id: (r.available or 0, r.reserved or 0)
            for r in cass.execute(
                "SELECT zone_id,available,reserved FROM inventory_by_product "
                "WHERE product_id=%s", (pid,))}


def inv_by_zone(cass, zid):
    return {r.product_id: (r.available or 0, r.reserved or 0)
            for r in cass.execute(
                "SELECT product_id,available,reserved FROM inventory_by_zone "
                "WHERE zone_id=%s", (zid,))}


def read_dlq(timeout=10):
    c = Consumer({
        "bootstrap.servers": KAFKA,
        "group.id": f"test-dlq-{uuid.uuid4()}",
        "auto.offset.reset": "earliest",
    })
    c.subscribe([DLQ_TOPIC])
    deadline = time.time() + timeout
    try:
        while time.time() < deadline:
            msg = c.poll(1.0)
            if msg and not msg.error():
                return json.loads(msg.value())
    finally:
        c.close()
    return None
