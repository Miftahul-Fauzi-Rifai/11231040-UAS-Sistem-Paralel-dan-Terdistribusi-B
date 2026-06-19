"""T12-T15: Pengujian endpoint GET /events dan GET /health."""
import uuid
import time
import pytest
from utils import make_event


def test_get_events_returns_list(client):
    """T12: GET /events mengembalikan list events."""
    r = client.get("/events")
    assert r.status_code == 200
    data = r.json()
    assert "events" in data
    assert "count" in data
    assert isinstance(data["events"], list)


def test_get_events_filter_by_topic(client):
    """T13: GET /events?topic=X hanya mengembalikan event dari topic X."""
    topic  = f"filter.{uuid.uuid4().hex[:8]}"
    events = [make_event(topic=topic, event_id=str(uuid.uuid4())) for _ in range(3)]
    for e in events:
        client.post("/publish", json=e)
    time.sleep(2)

    r    = client.get(f"/events?topic={topic}")
    data = r.json()
    assert data["count"] >= 3
    for ev in data["events"]:
        assert ev["topic"] == topic


def test_get_events_count_matches_list_length(client):
    """T14: field count = len(events)."""
    r    = client.get("/events")
    data = r.json()
    assert data["count"] == len(data["events"])


def test_health_endpoint(client):
    """T15: GET /health mengembalikan status ok."""
    r = client.get("/health")
    assert r.status_code == 200
    assert r.json()["status"] == "ok"
