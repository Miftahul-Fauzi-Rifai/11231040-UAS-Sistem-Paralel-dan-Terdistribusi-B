"""T06-T08: Pengujian idempotency & deduplication."""
import uuid
import time
import pytest
from utils import make_event


def test_dedup_same_event_id_stored_once(client):
    """T06: Event dengan event_id sama dikirim 2x — hanya 1 record di DB."""
    topic    = f"dedup.once.{uuid.uuid4().hex[:6]}"
    event_id = str(uuid.uuid4())
    event    = make_event(topic=topic, event_id=event_id)

    # Kirim dua kali
    r1 = client.post("/publish", json=event)
    r2 = client.post("/publish", json=event)
    assert r1.status_code == 200
    assert r2.status_code == 200

    time.sleep(2)  # tunggu consumer memproses

    r3 = client.get(f"/events?topic={topic}")
    ids = [e["event_id"] for e in r3.json()["events"]]
    assert ids.count(event_id) == 1, "Event duplikat seharusnya hanya tersimpan 1x"


def test_dedup_increments_duplicate_dropped(client):
    """T07: Stats duplicate_dropped bertambah sesuai jumlah duplikat."""
    stats_before = client.get("/stats").json()
    dup_before   = stats_before["duplicate_dropped"]

    event_id = str(uuid.uuid4())
    event    = make_event(topic="dedup.stats", event_id=event_id)

    # Kirim 3x — 2 seharusnya masuk duplicate_dropped
    for _ in range(3):
        client.post("/publish", json=event)

    time.sleep(2)

    stats_after = client.get("/stats").json()
    assert stats_after["duplicate_dropped"] >= dup_before + 2


def test_dedup_batch_with_repeated_event_id(client):
    """T08: Batch berisi event_id yang sama 3x — hanya 1 masuk DB."""
    topic    = f"dedup.batch.{uuid.uuid4().hex[:6]}"
    event_id = str(uuid.uuid4())
    base     = make_event(topic=topic, event_id=event_id)
    batch    = [base.copy() for _ in range(3)]

    r = client.post("/publish", json={"events": batch})
    assert r.status_code == 200
    assert r.json()["accepted"] == 3

    time.sleep(2)

    r2  = client.get(f"/events?topic={topic}")
    ids = [e["event_id"] for e in r2.json()["events"]]
    assert ids.count(event_id) == 1
