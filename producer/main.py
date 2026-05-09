import json
import os
import uuid
from datetime import datetime, timezone
from typing import Optional, List

from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import SerializingProducer
from fastapi import FastAPI
from pydantic import BaseModel

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
SCHEMA_REGISTRY_URL = os.getenv("SCHEMA_REGISTRY_URL", "http://localhost:8081")
TOPIC = "warehouse-events"

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

app = FastAPI(title="WMS Producer")
producer: SerializingProducer = None


@app.on_event("startup")
def startup():
    global producer
    sr = SchemaRegistryClient({"url": SCHEMA_REGISTRY_URL})
    producer = SerializingProducer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "value.serializer": AvroSerializer(sr, AVRO_SCHEMA),
    })


class EventRequest(BaseModel):
    event_id: Optional[str] = None
    event_type: str
    timestamp: Optional[str] = None
    product_id: Optional[str] = None
    zone_id: Optional[str] = None
    from_zone_id: Optional[str] = None
    to_zone_id: Optional[str] = None
    quantity: Optional[int] = None
    order_id: Optional[str] = None
    order_items: Optional[List[dict]] = None
    counted_quantity: Optional[int] = None


@app.post("/events")
def send_event(req: EventRequest):
    evt = {
        "event_id": req.event_id or str(uuid.uuid4()),
        "event_type": req.event_type,
        "timestamp": req.timestamp or datetime.now(timezone.utc).isoformat(),
        "product_id": req.product_id,
        "zone_id": req.zone_id,
        "from_zone_id": req.from_zone_id,
        "to_zone_id": req.to_zone_id,
        "quantity": req.quantity,
        "order_id": req.order_id,
        "order_items_json": json.dumps(req.order_items) if req.order_items else None,
        "counted_quantity": req.counted_quantity,
    }
    producer.produce(topic=TOPIC, value=evt, key=req.product_id or req.order_id or "x")
    producer.flush()
    return {"status": "ok", "event_id": evt["event_id"]}
