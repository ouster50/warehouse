import time
import uuid
from conftest import send, wait, inv, inv_by_product, inv_by_zone, read_dlq


class TestBasicCycle:
    def test_01_receive(self, kp, cass):
        eid = send(kp, "PRODUCT_RECEIVED", product_id="SKU-001", zone_id="ZONE-A", quantity=100)
        assert wait(cass, eid)
        assert inv(cass, "SKU-001", "ZONE-A") == (100, 0)

    def test_02_by_product(self, cass):
        assert inv_by_product(cass, "SKU-001")["ZONE-A"] == (100, 0)

    def test_03_reserve(self, kp, cass):
        eid = send(kp, "PRODUCT_RESERVED", product_id="SKU-001", zone_id="ZONE-A", quantity=30)
        assert wait(cass, eid)
        assert inv(cass, "SKU-001", "ZONE-A") == (70, 30)

    def test_04_move(self, kp, cass):
        eid = send(kp, "PRODUCT_MOVED", product_id="SKU-001",
                   from_zone_id="ZONE-A", to_zone_id="ZONE-B", quantity=20)
        assert wait(cass, eid)
        assert inv(cass, "SKU-001", "ZONE-A") == (50, 30)
        assert inv(cass, "SKU-001", "ZONE-B") == (20, 0)

    def test_05_ship(self, kp, cass):
        eid = send(kp, "PRODUCT_SHIPPED", product_id="SKU-001", zone_id="ZONE-A", quantity=10)
        assert wait(cass, eid)
        assert inv(cass, "SKU-001", "ZONE-A")[0] == 40

    def test_06_order_created(self, kp, cass):
        eid = send(kp, "ORDER_CREATED", order_id="ORD-001",
                   order_items=[{"product_id": "SKU-001", "zone_id": "ZONE-A", "quantity": 15}])
        assert wait(cass, eid)
        assert inv(cass, "SKU-001", "ZONE-A") == (25, 45)

    def test_07_order_completed(self, kp, cass):
        eid = send(kp, "ORDER_COMPLETED", order_id="ORD-001")
        assert wait(cass, eid)
        assert inv(cass, "SKU-001", "ZONE-A") == (25, 30)


class TestIdempotency:
    def test_duplicate_ignored(self, kp, cass):
        eid = str(uuid.uuid4())
        send(kp, "PRODUCT_RECEIVED", event_id=eid,
             product_id="SKU-002", zone_id="ZONE-A", quantity=50)
        assert wait(cass, eid)
        assert inv(cass, "SKU-002", "ZONE-A") == (50, 0)

        send(kp, "PRODUCT_RECEIVED", event_id=eid,
             product_id="SKU-002", zone_id="ZONE-A", quantity=50)
        time.sleep(3)
        assert inv(cass, "SKU-002", "ZONE-A") == (50, 0)


class TestConsistency:
    def test_all_tables_match(self, kp, cass):
        eid = send(kp, "PRODUCT_RECEIVED", product_id="SKU-003", zone_id="ZONE-A", quantity=100)
        assert wait(cass, eid)
        assert inv(cass, "SKU-003", "ZONE-A") == (100, 0)
        assert inv_by_product(cass, "SKU-003")["ZONE-A"] == (100, 0)
        assert inv_by_zone(cass, "ZONE-A")["SKU-003"] == (100, 0)


class TestOutOfOrder:
    def test_old_event_skipped(self, kp, cass):
        e1 = send(kp, "PRODUCT_RECEIVED", product_id="SKU-004", zone_id="ZONE-A",
                  quantity=100, timestamp="2026-04-01T12:00:00Z")
        assert wait(cass, e1)
        assert inv(cass, "SKU-004", "ZONE-A")[0] == 100

        e2 = send(kp, "PRODUCT_SHIPPED", product_id="SKU-004", zone_id="ZONE-A",
                  quantity=20, timestamp="2026-04-01T12:05:00Z")
        assert wait(cass, e2)
        assert inv(cass, "SKU-004", "ZONE-A")[0] == 80

        e3 = send(kp, "PRODUCT_RECEIVED", product_id="SKU-004", zone_id="ZONE-A",
                  quantity=50, timestamp="2026-04-01T12:02:00Z")
        assert wait(cass, e3)
        assert inv(cass, "SKU-004", "ZONE-A")[0] == 80


class TestDLQ:
    def test_invalid_to_dlq(self, kp, cass):
        bad_eid = send(kp, "PRODUCT_SHIPPED",
                       product_id="SKU-005", zone_id="ZONE-A", quantity=-5)
        time.sleep(5)

        ok_eid = send(kp, "PRODUCT_RECEIVED",
                      product_id="SKU-005", zone_id="ZONE-A", quantity=10)
        assert wait(cass, ok_eid)
        assert inv(cass, "SKU-005", "ZONE-A") == (10, 0)

        dlq = read_dlq()
        assert dlq is not None
        assert dlq["original_event"]["event_id"] == bad_eid
        assert "quantity" in dlq["error_reason"].lower()
