"""T16-T20: Pengujian konkurensi, performa, dan konsistensi."""
import uuid
import time
import threading
import pytest
from utils import make_event


def test_concurrent_same_event_processed_once(client):
    """T16: 10 thread mengirim event_id yang sama — hanya 1 masuk DB (no race condition)."""
    topic    = f"concurrent.dedup.{uuid.uuid4().hex[:6]}"
    event_id = str(uuid.uuid4())
    event    = make_event(topic=topic, event_id=event_id)
    statuses = []

    def send():
        r = client.post("/publish", json=event)
        statuses.append(r.status_code)

    threads = [threading.Thread(target=send) for _ in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert all(s == 200 for s in statuses), "Semua request harus berhasil 200"

    time.sleep(3)

    r    = client.get(f"/events?topic={topic}")
    ids  = [e["event_id"] for e in r.json()["events"]]
    assert ids.count(event_id) == 1, "Event harus tersimpan tepat 1x meski ada race condition"


def test_concurrent_unique_events_all_processed(client):
    """T17: 10 thread mengirim event_id berbeda — semua harus masuk DB."""
    topic     = f"concurrent.unique.{uuid.uuid4().hex[:6]}"
    event_ids = [str(uuid.uuid4()) for _ in range(10)]
    statuses  = []

    def send(eid):
        r = client.post("/publish", json=make_event(topic=topic, event_id=eid))
        statuses.append(r.status_code)

    threads = [threading.Thread(target=send, args=(eid,)) for eid in event_ids]
    for t in threads: t.start()
    for t in threads: t.join()

    assert all(s == 200 for s in statuses)

    time.sleep(3)

    r          = client.get(f"/events?topic={topic}")
    stored_ids = [e["event_id"] for e in r.json()["events"]]
    for eid in event_ids:
        assert eid in stored_ids, f"event_id {eid} tidak ditemukan di DB"


def test_large_batch_performance(client):
    """T18: Batch 500 event diproses API dalam < 10 detik."""
    import time as t
    events = [make_event(event_id=str(uuid.uuid4())) for _ in range(500)]
    start  = t.time()
    r      = client.post("/publish", json={"events": events})
    elapsed = t.time() - start

    assert r.status_code == 200
    assert r.json()["accepted"] == 500
    assert elapsed < 10.0, f"Terlalu lambat: {elapsed:.2f}s"


def test_stats_consistency_under_concurrent_load(client):
    """T19: received >= unique_processed + duplicate_dropped setelah concurrent publish."""
    event_id = str(uuid.uuid4())
    base     = make_event(topic="load.consistency", event_id=event_id)
    uniques  = [make_event(event_id=str(uuid.uuid4())) for _ in range(20)]
    batch    = [base.copy() for _ in range(5)] + uniques

    client.post("/publish", json={"events": batch})
    time.sleep(3)

    s = client.get("/stats").json()
    assert s["unique_processed"] + s["duplicate_dropped"] <= s["received"], \
        "Konsistensi statistik gagal: processed + dropped > received"


def test_dedup_persists_across_separate_requests(client):
    """T20: Dedup bekerja lintas request HTTP terpisah (bukan hanya dalam satu request)."""
    topic    = f"persist.dedup.{uuid.uuid4().hex[:6]}"
    event_id = str(uuid.uuid4())
    event    = make_event(topic=topic, event_id=event_id)

    client.post("/publish", json=event)
    time.sleep(2)

    # Request kedua terpisah
    client.post("/publish", json=event)
    time.sleep(2)

    r   = client.get(f"/events?topic={topic}")
    ids = [e["event_id"] for e in r.json()["events"]]
    assert ids.count(event_id) == 1, \
        "Dedup harus bekerja lintas request HTTP terpisah"
