import asyncio
import logging
import os
import time
from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from database import Database
from consumer import ConsumerWorker
from schemas import Event

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger(__name__)

START_TIME = time.time()
db: Database = None
consumer: ConsumerWorker = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global db, consumer

    database_url = os.getenv("DATABASE_URL", "postgresql://user:pass@storage:5432/logdb")
    broker_url   = os.getenv("BROKER_URL", "redis://broker:6379")
    num_workers  = int(os.getenv("CONSUMER_WORKERS", "4"))

    # Init DB dengan retry (Postgres mungkin belum siap)
    db = Database(database_url)
    for attempt in range(15):
        try:
            await db.connect()
            break
        except Exception as exc:
            logger.warning(f"DB connect attempt {attempt + 1}/15 failed: {exc}")
            await asyncio.sleep(3)
    else:
        logger.error("Could not connect to database after 15 attempts")
        raise RuntimeError("Database unavailable")

    await db.init_tables()

    # Start consumer workers sebagai background task
    consumer = ConsumerWorker(db, broker_url, workers=num_workers)
    asyncio.create_task(consumer.start())

    logger.info("Aggregator ready ✓")
    yield

    await consumer.stop()
    await db.disconnect()
    logger.info("Aggregator shutdown complete")


app = FastAPI(
    title="Pub-Sub Log Aggregator",
    version="1.0.0",
    lifespan=lifespan,
)


@app.post("/publish")
async def publish(request: Request):
    """
    Terima single event atau batch event.
    Format yang diterima:
      - Single : { "topic":..., "event_id":..., ... }
      - Array  : [ {...}, {...} ]
      - Batch  : { "events": [ {...}, {...} ] }
    """
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Invalid JSON body")

    if isinstance(body, list):
        raw_events = body
    elif isinstance(body, dict) and "events" in body:
        raw_events = body["events"]
    elif isinstance(body, dict):
        raw_events = [body]
    else:
        raise HTTPException(status_code=400, detail="Unsupported request format")

    if not raw_events:
        raise HTTPException(status_code=400, detail="No events provided")

    accepted = 0
    errors   = []
    for i, raw in enumerate(raw_events):
        try:
            event = Event(**raw)
            await consumer.enqueue(event)
            accepted += 1
        except Exception as exc:
            errors.append({"index": i, "error": str(exc)})

    if accepted > 0:
        await db.increment_received_batch(accepted)

    return {"status": "accepted", "accepted": accepted, "errors": errors}


@app.get("/events")
async def get_events(topic: Optional[str] = Query(None, description="Filter by topic")):
    """Daftar event unik yang telah diproses, opsional difilter by topic."""
    events = await db.get_events(topic)
    return {"topic": topic, "count": len(events), "events": events}


@app.get("/stats")
async def get_stats():
    """Statistik aggregator: received, unique_processed, duplicate_dropped, topics, uptime."""
    stats = await db.get_stats()
    stats["uptime_seconds"] = round(time.time() - START_TIME, 2)
    return stats


@app.get("/health")
async def health():
    """Liveness/readiness probe."""
    return {"status": "ok", "uptime_seconds": round(time.time() - START_TIME, 2)}
