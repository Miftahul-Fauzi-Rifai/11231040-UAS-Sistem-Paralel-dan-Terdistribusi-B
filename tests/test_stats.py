"""T09-T11: Pengujian endpoint GET /stats."""
import uuid
import time
import pytest
from utils import make_event


def test_stats_has_required_fields(client):
    """T09: GET /stats mengembalikan semua field yang diperlukan."""
    r = client.get("/stats")
    assert r.status_code == 200
    data = r.json()
    for field in ("received", "unique_processed", "duplicate_dropped", "topics", "uptime_seconds"):
        assert field in data, f"Missing field: {field}"


def test_stats_received_increments_after_publish(client):
    """T10: received bertambah setelah publish."""
    before = client.get("/stats").json()["received"]
    events = [make_event(event_id=str(uuid.uuid4())) for _ in range(5)]
    client.post("/publish", json={"events": events})
    time.sleep(1)
    after = client.get("/stats").json()["received"]
    assert after >= before + 5


def test_stats_topics_includes_published_topic(client):
    """T11: topics mencantumkan topic yang baru saja diproses."""
    topic = f"stats.topic.{uuid.uuid4().hex[:8]}"
    client.post("/publish", json=make_event(topic=topic, event_id=str(uuid.uuid4())))
    time.sleep(2)
    topics = client.get("/stats").json()["topics"]
    assert topic in topics
