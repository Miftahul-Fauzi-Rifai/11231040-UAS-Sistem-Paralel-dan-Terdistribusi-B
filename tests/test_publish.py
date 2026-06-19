"""T01-T05: Pengujian endpoint POST /publish."""
import uuid
import pytest
from utils import make_event


def test_publish_single_event(client):
    """T01: Single event (dict format) diterima dengan accepted=1."""
    event = make_event(event_id=str(uuid.uuid4()))
    r = client.post("/publish", json=event)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "accepted"
    assert data["accepted"] == 1
    assert data["errors"] == []


def test_publish_batch_events(client):
    """T02: Batch event (dict dengan key 'events') diterima dengan accepted=N."""
    events = [make_event(event_id=str(uuid.uuid4())) for _ in range(5)]
    r = client.post("/publish", json={"events": events})
    assert r.status_code == 200
    data = r.json()
    assert data["accepted"] == 5


def test_publish_array_format(client):
    """T03: Format array JSON juga diterima."""
    events = [make_event(event_id=str(uuid.uuid4())) for _ in range(3)]
    r = client.post("/publish", json=events)
    assert r.status_code == 200
    assert r.json()["accepted"] == 3


def test_publish_empty_batch_returns_400(client):
    """T04: Batch kosong dikembalikan 400."""
    r = client.post("/publish", json={"events": []})
    assert r.status_code == 400


def test_publish_invalid_event_reports_error(client):
    """T05: Event dengan field wajib yang hilang dilaporkan di 'errors'."""
    bad = {"topic": "test", "event_id": str(uuid.uuid4())}  # tidak ada timestamp & source
    r = client.post("/publish", json=bad)
    # Accepted 0 dengan errors, atau 400
    if r.status_code == 200:
        body = r.json()
        assert body["accepted"] == 0 or len(body["errors"]) > 0
    else:
        assert r.status_code in (400, 422)
