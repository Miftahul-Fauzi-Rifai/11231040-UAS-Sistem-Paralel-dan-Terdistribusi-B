"""
Publisher: generator/simulator event dengan duplikasi ~35%.
Mengirim event ke aggregator POST /publish secara batch.
"""
import asyncio
import json
import logging
import os
import random
import time
import uuid
from datetime import datetime, timezone

import httpx

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)

TARGET_URL    = os.getenv("TARGET_URL",    "http://aggregator:8080/publish")
TOTAL_EVENTS  = int(os.getenv("TOTAL_EVENTS",  "1000"))
BATCH_SIZE    = int(os.getenv("EVENTS_PER_BATCH", "20"))
DUPLICATE_RATE = float(os.getenv("DUPLICATE_RATE", "0.35"))
DELAY_MS      = int(os.getenv("DELAY_MS", "50"))

TOPICS = [
    "user.login",
    "order.created",
    "payment.processed",
    "inventory.updated",
    "system.alert",
]


def make_event(event_id: str = None, topic: str = None) -> dict:
    return {
        "topic":     topic or random.choice(TOPICS),
        "event_id":  event_id or str(uuid.uuid4()),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source":    "publisher",
        "payload": {
            "value": random.randint(1, 10000),
            "label": f"evt-{random.randint(1000, 9999)}",
        },
    }


async def wait_for_aggregator(client: httpx.AsyncClient):
    """
    Tunggu aggregator siap dengan exponential backoff.
    Backoff: 0.1s -> 0.2s -> 0.4s -> ... -> max 30s per attempt.
    Mitigasi Bab 6: failure mode omission/timeout dengan retry strategy.
    """
    health_url = TARGET_URL.replace("/publish", "/health")
    for attempt in range(30):
        try:
            r = await client.get(health_url)
            if r.status_code == 200:
                logger.info("Aggregator is ready ✓")
                return
        except Exception:
            pass

        wait = min((2 ** attempt) * 0.1, 30.0)  # cap 30 detik
        logger.info(
            f"Waiting for aggregator... ({attempt + 1}/30), "
            f"exponential backoff {wait:.1f}s"
        )
        await asyncio.sleep(wait)

    raise RuntimeError("Aggregator did not become ready in time")


async def publish_with_backoff(
    client: httpx.AsyncClient,
    url: str,
    batch: list,
    max_retries: int = 5,
) -> httpx.Response:
    """
    Kirim batch event dengan exponential backoff pada kegagalan.
    Backoff: 0.5s -> 1s -> 2s -> 4s (faktor 2, base 0.5s).
    Implementasi mitigasi Bab 6: retry + backoff pada omission/timeout failure.
    """
    last_exc = None
    for attempt in range(max_retries):
        try:
            r = await client.post(
                url,
                json={"events": batch},
                headers={"Content-Type": "application/json"},
            )
            if r.status_code == 200:
                return r
            logger.warning(
                f"Publish HTTP {r.status_code} (attempt {attempt + 1}/{max_retries})"
            )
        except (httpx.TimeoutException, httpx.ConnectError) as exc:
            last_exc = exc
            logger.warning(
                f"Publish error: {exc} (attempt {attempt + 1}/{max_retries})"
            )
        if attempt < max_retries - 1:
            wait = (2 ** attempt) * 0.5   # 0.5, 1.0, 2.0, 4.0 detik
            logger.info(f"Exponential backoff {wait:.1f}s sebelum retry...")
            await asyncio.sleep(wait)
    raise RuntimeError(f"Gagal publish setelah {max_retries} percobaan: {last_exc}")


async def run():
    # Pool event untuk diduplikasi
    event_pool: list[dict] = [make_event() for _ in range(100)]

    sent = 0
    dup_sent = 0

    async with httpx.AsyncClient(timeout=30.0) as client:
        await wait_for_aggregator(client)

        logger.info(
            f"Publishing {TOTAL_EVENTS} events "
            f"(batch={BATCH_SIZE}, dup_rate={DUPLICATE_RATE})"
        )
        t_start = time.time()

        while sent < TOTAL_EVENTS:
            batch = []
            for _ in range(min(BATCH_SIZE, TOTAL_EVENTS - sent)):
                if random.random() < DUPLICATE_RATE and event_pool:
                    # Kirim duplikat dari pool
                    evt = random.choice(event_pool).copy()
                    evt["timestamp"] = datetime.now(timezone.utc).isoformat()
                    batch.append(evt)
                    dup_sent += 1
                else:
                    # Event baru
                    evt = make_event()
                    event_pool.append(evt)
                    if len(event_pool) > 500:
                        event_pool.pop(0)
                    batch.append(evt)

            try:
                r = await publish_with_backoff(client, TARGET_URL, batch)
                sent += len(batch)
                if sent % 500 == 0 or sent == TOTAL_EVENTS:
                    elapsed = time.time() - t_start
                    logger.info(
                        f"Progress: {sent}/{TOTAL_EVENTS} "
                        f"(dup_sent={dup_sent}) | "
                        f"throughput={sent / elapsed:.0f} ev/s"
                    )
            except RuntimeError as exc:
                logger.error(f"Batch gagal setelah backoff: {exc}")

            if DELAY_MS > 0:
                await asyncio.sleep(DELAY_MS / 1000)

        elapsed = time.time() - t_start
        logger.info(
            f"Done! sent={sent} dup_sent={dup_sent} "
            f"elapsed={elapsed:.1f}s "
            f"throughput={sent / elapsed:.0f} ev/s"
        )


if __name__ == "__main__":
    asyncio.run(run())
