import uuid
from datetime import datetime, timezone
from typing import Optional


def make_event(
    topic: str = "test.topic",
    event_id: Optional[str] = None,
    source: str = "test",
    payload: Optional[dict] = None,
) -> dict:
    return {
        "topic":     topic,
        "event_id":  event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source":    source,
        "payload":   payload or {"key": "value"},
    }
