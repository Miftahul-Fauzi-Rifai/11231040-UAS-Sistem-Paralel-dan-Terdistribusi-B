import asyncio
import json
import logging
from typing import Optional

import redis.asyncio as aioredis

from database import Database
from schemas import Event

logger = logging.getLogger(__name__)

QUEUE_KEY = "events_queue"


class ConsumerWorker:
    def __init__(self, db: Database, broker_url: str, workers: int = 4):
        self.db = db
        self.broker_url = broker_url
        self.num_workers = workers
        self.redis: Optional[aioredis.Redis] = None
        self._running = False
        self._tasks: list = []

    async def _connect_redis(self):
        self.redis = aioredis.from_url(self.broker_url, decode_responses=True)
        logger.info(f"Consumer connected to broker {self.broker_url}")

    async def enqueue(self, event: Event):
        """Push event JSON string ke Redis queue (RPUSH)."""
        if self.redis is None:
            await self._connect_redis()
        await self.redis.rpush(QUEUE_KEY, event.model_dump_json())

    async def start(self):
        """Jalankan N worker coroutine secara concurrent."""
        await self._connect_redis()
        self._running = True
        self._tasks = [
            asyncio.create_task(self._worker(i))
            for i in range(self.num_workers)
        ]
        logger.info(f"Started {self.num_workers} consumer workers")
        await asyncio.gather(*self._tasks, return_exceptions=True)

    async def stop(self):
        self._running = False
        for t in self._tasks:
            t.cancel()
        if self.redis:
            await self.redis.aclose()
        logger.info("Consumer workers stopped")

    async def _worker(self, worker_id: int):
        """Loop BLPOP → process_event per worker."""
        logger.info(f"Worker-{worker_id} started")
        while self._running:
            try:
                # Blocking pop dengan timeout 1 detik agar bisa cek _running
                result = await self.redis.blpop(QUEUE_KEY, timeout=1)
                if result is None:
                    continue

                _, raw = result
                event_data = json.loads(raw)
                await self.db.process_event(event_data)

            except asyncio.CancelledError:
                break
            except Exception as exc:
                logger.error(f"Worker-{worker_id} error: {exc}", exc_info=True)
                await asyncio.sleep(0.5)

        logger.info(f"Worker-{worker_id} stopped")
