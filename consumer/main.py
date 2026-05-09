import json
import logging
import os
import time
from datetime import datetime, timezone

from cassandra.cluster import Cluster
from cassandra.query import BatchStatement, ConsistencyLevel
from confluent_kafka import Consumer, Producer, KafkaError
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import SerializationContext, MessageField

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger("consumer")

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
CASSANDRA_HOSTS = os.getenv("CASSANDRA_HOSTS", "localhost").split(",")
CASSANDRA_PORT = int(os.getenv("CASSANDRA_PORT", "9042"))
TOPIC = "warehouse-events"
DLQ_TOPIC = "warehouse-events-dlq"
GROUP_ID = "warehouse-state-consumer"

AVRO_SCHEMA = """{
  "type":"record","name":"WarehouseEvent","namespace":"com.warehouse",
  "fields":[
    {"name":"event_id","type":"string"},
    {"name":"event_type","type":"string"},
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


def connect_cassandra():
    for i in range(30):
        try:
            c = Cluster(CASSANDRA_HOSTS, port=CASSANDRA_PORT)
            s = c.connect("warehouse")
            log.info("Cassandra connected")
            return c, s
        except Exception as e:
            log.warning("Cassandra retry %d/30: %s", i + 1, e)
            time.sleep(3)
    raise RuntimeError("Cassandra unavailable")


_ps = {}


def ps(session, key, cql):
    if key not in _ps:
        _ps[key] = session.prepare(cql)
    return _ps[key]


def get_inv(session, pid, zid):
    r = session.execute(
        ps(session, "gi", "SELECT available, reserved FROM inventory_by_product_zone "
                          "WHERE product_id=? AND zone_id=?"),
        (pid, zid)).one()
    return (r.available or 0, r.reserved or 0) if r else (0, 0)


def inv_stmts(session, pid, zid, avail, res):
    """3 bound statements — одно обновление во все денормализованные таблицы."""
    return [
        ps(session, "u_pz", "INSERT INTO inventory_by_product_zone "
           "(product_id,zone_id,available,reserved) VALUES(?,?,?,?)").bind((pid, zid, avail, res)),
        ps(session, "u_p", "INSERT INTO inventory_by_product "
           "(product_id,zone_id,available,reserved) VALUES(?,?,?,?)").bind((pid, zid, avail, res)),
        ps(session, "u_z", "INSERT INTO inventory_by_zone "
           "(zone_id,product_id,available,reserved) VALUES(?,?,?,?)").bind((zid, pid, avail, res)),
    ]


def commit_batch(session, stmts, event_id):
    """Logged batch: inventory + mark processed — атомарно."""
    b = BatchStatement(consistency_level=ConsistencyLevel.LOCAL_ONE)
    for s in stmts:
        b.add(s)
    b.add(ps(session, "mark",
             "INSERT INTO processed_events(event_id,processed_at) VALUES(?,toTimestamp(now()))")
          .bind((event_id,)))
    session.execute(b)


def is_outdated(session, pid, zid, ts):
    r = session.execute(
        ps(session, "gts", "SELECT last_ts FROM last_event_timestamp "
                           "WHERE product_id=? AND zone_id=?"),
        (pid, zid)).one()
    return bool(r and r.last_ts and r.last_ts >= ts)


def ts_stmt(session, pid, zid, ts):
    return ps(session, "sts",
              "INSERT INTO last_event_timestamp(product_id,zone_id,last_ts) VALUES(?,?,?)") \
        .bind((pid, zid, ts))


NEED_POS_QTY = {"PRODUCT_RECEIVED", "PRODUCT_SHIPPED", "PRODUCT_RESERVED",
                "PRODUCT_RELEASED", "PRODUCT_MOVED"}

def validate(e):
    t = e.get("event_type")
    if t in NEED_POS_QTY:
        q = e.get("quantity")
        if q is None or q <= 0:
            raise ValueError(f"Invalid quantity: {q} (must be positive)")
    if t in ("PRODUCT_RECEIVED", "PRODUCT_SHIPPED", "PRODUCT_RESERVED", "PRODUCT_RELEASED"):
        if not e.get("product_id") or not e.get("zone_id"):
            raise ValueError("product_id and zone_id required")
    elif t == "PRODUCT_MOVED":
        if not all(e.get(k) for k in ("product_id", "from_zone_id", "to_zone_id")):
            raise ValueError("product_id, from_zone_id, to_zone_id required")
    elif t == "INVENTORY_COUNTED":
        if e.get("counted_quantity") is None or e["counted_quantity"] < 0:
            raise ValueError("counted_quantity must be >= 0")
    elif t == "ORDER_CREATED":
        if not e.get("order_id") or not e.get("order_items_json"):
            raise ValueError("order_id and order_items_json required")
    elif t == "ORDER_COMPLETED":
        if not e.get("order_id"):
            raise ValueError("order_id required")
    else:
        raise ValueError(f"Unknown event_type: {t}")


def already_processed(session, eid):
    r = session.execute(
        ps(session, "dup", "SELECT event_id FROM processed_events WHERE event_id=?"),
        (eid,)).one()
    if r:
        log.info("Dup %s, skip", eid)
    return bool(r)


def mark_only(session, eid):
    session.execute(
        ps(session, "mark",
           "INSERT INTO processed_events(event_id,processed_at) VALUES(?,toTimestamp(now()))"),
        (eid,))


def handle(session, e):
    eid, t, ts = e["event_id"], e["event_type"], e["timestamp"]
    if already_processed(session, eid):
        return
    validate(e)

    q = e.get("quantity") or 0
    handlers = {
        "PRODUCT_RECEIVED":  lambda: _delta(session, e, eid, ts, +q, 0),
        "PRODUCT_SHIPPED":   lambda: _delta(session, e, eid, ts, -q, 0),
        "PRODUCT_RESERVED":  lambda: _delta(session, e, eid, ts, -q, +q),
        "PRODUCT_RELEASED":  lambda: _delta(session, e, eid, ts, +q, -q),
        "PRODUCT_MOVED":     lambda: _moved(session, e, eid, ts),
        "INVENTORY_COUNTED": lambda: _counted(session, e, eid, ts),
        "ORDER_CREATED":     lambda: _order_created(session, e, eid, ts),
        "ORDER_COMPLETED":   lambda: _order_completed(session, e, eid, ts),
    }
    handlers[t]()

    session.execute(
        ps(session, "hist",
           "INSERT INTO event_history(product_id,event_ts,event_id,event_type,payload) "
           "VALUES(?,?,?,?,?)"),
        (e.get("product_id") or e.get("order_id", "?"), ts, eid, t, json.dumps(e)))


def _delta(session, e, eid, ts, da, dr):
    pid, zid = e["product_id"], e["zone_id"]
    if is_outdated(session, pid, zid, ts):
        log.info("Out-of-order %s, skip", eid); mark_only(session, eid); return
    a, r = get_inv(session, pid, zid)
    stmts = inv_stmts(session, pid, zid, a + da, r + dr)
    stmts.append(ts_stmt(session, pid, zid, ts))
    commit_batch(session, stmts, eid)


def _moved(session, e, eid, ts):
    pid, q = e["product_id"], e["quantity"]
    fz, tz = e["from_zone_id"], e["to_zone_id"]
    if is_outdated(session, pid, fz, ts) or is_outdated(session, pid, tz, ts):
        log.info("Out-of-order MOVE %s, skip", eid); mark_only(session, eid); return
    a1, r1 = get_inv(session, pid, fz)
    a2, r2 = get_inv(session, pid, tz)
    stmts = inv_stmts(session, pid, fz, a1 - q, r1) + inv_stmts(session, pid, tz, a2 + q, r2)
    stmts += [ts_stmt(session, pid, fz, ts), ts_stmt(session, pid, tz, ts)]
    commit_batch(session, stmts, eid)


def _counted(session, e, eid, ts):
    pid, zid = e["product_id"], e["zone_id"]
    if is_outdated(session, pid, zid, ts):
        log.info("Out-of-order COUNTED %s, skip", eid); mark_only(session, eid); return
    _, r = get_inv(session, pid, zid)
    stmts = inv_stmts(session, pid, zid, e["counted_quantity"], r)
    stmts.append(ts_stmt(session, pid, zid, ts))
    commit_batch(session, stmts, eid)


def _order_created(session, e, eid, ts):
    items = json.loads(e["order_items_json"])
    stmts = []
    for it in items:
        pid, zid, q = it["product_id"], it["zone_id"], it["quantity"]
        if is_outdated(session, pid, zid, ts):
            continue
        a, r = get_inv(session, pid, zid)
        stmts += inv_stmts(session, pid, zid, a - q, r + q)
        stmts.append(ts_stmt(session, pid, zid, ts))
    stmts.append(
        ps(session, "ins_o",
           "INSERT INTO orders(order_id,status,items_json,created_at,updated_at) "
           "VALUES(?,?,?,toTimestamp(now()),toTimestamp(now()))")
        .bind((e["order_id"], "CREATED", e["order_items_json"])))
    commit_batch(session, stmts, eid)


def _order_completed(session, e, eid, ts):
    oid = e["order_id"]
    row = session.execute(
        ps(session, "get_o", "SELECT items_json,status FROM orders WHERE order_id=?"),
        (oid,)).one()
    if not row:
        raise ValueError(f"Order {oid} not found")
    if row.status == "COMPLETED":
        mark_only(session, eid); return
    items = json.loads(row.items_json)
    stmts = []
    for it in items:
        pid, zid, q = it["product_id"], it["zone_id"], it["quantity"]
        a, r = get_inv(session, pid, zid)
        stmts += inv_stmts(session, pid, zid, a, r - q)
    stmts.append(
        ps(session, "upd_o", "UPDATE orders SET status=?,updated_at=toTimestamp(now()) WHERE order_id=?")
        .bind(("COMPLETED", oid)))
    commit_batch(session, stmts, eid)


def send_dlq(dlq, evt, err, part=None, off=None):
    msg = json.dumps({
        "original_event": evt,
        "error_reason": str(err),
        "error_code": type(err).__name__,
        "failed_at": datetime.now(timezone.utc).isoformat(),
        "kafka_metadata": {"partition": part, "offset": off},
    })
    dlq.produce(DLQ_TOPIC, value=msg.encode())
    dlq.flush()
    log.warning("DLQ: %s — %s", evt.get("event_id", "?"), err)


def main():
    cluster, session = connect_cassandra()

    deser = AvroDeserializer(
        SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL}), AVRO_SCHEMA)

    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    })
    consumer.subscribe([TOPIC])
    dlq = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP})
    log.info("Listening topic=%s group=%s", TOPIC, GROUP_ID)

    try:
        while True:
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    log.error("Kafka: %s", msg.error())
                continue
            evt = None
            try:
                evt = deser(msg.value(), SerializationContext(TOPIC, MessageField.VALUE))
                log.info("event=%s type=%s p=%d off=%d",
                         evt["event_id"], evt["event_type"], msg.partition(), msg.offset())
                handle(session, evt)
                consumer.commit(msg)
            except Exception as e:
                log.exception("Error p=%d off=%d", msg.partition(), msg.offset())
                send_dlq(dlq, evt or {}, e, msg.partition(), msg.offset())
                consumer.commit(msg)
    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()
        cluster.shutdown()


if __name__ == "__main__":
    main()
