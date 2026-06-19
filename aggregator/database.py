import asyncpg
import json
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any

logger = logging.getLogger(__name__)


class Database:
    def __init__(self, dsn: str):
        self.dsn = dsn
        self.pool: Optional[asyncpg.Pool] = None

    async def connect(self):
        self.pool = await asyncpg.create_pool(self.dsn, min_size=2, max_size=20)
        logger.info("Database pool created")

    async def disconnect(self):
        if self.pool:
            await self.pool.close()
            logger.info("Database pool closed")

    async def init_tables(self):
        async with self.pool.acquire() as conn:
            # Tabel utama event dengan unique constraint (topic, event_id)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS processed_events (
                    id          BIGSERIAL PRIMARY KEY,
                    topic       VARCHAR(255) NOT NULL,
                    event_id    VARCHAR(255) NOT NULL,
                    timestamp   TIMESTAMPTZ  NOT NULL,
                    source      VARCHAR(255) NOT NULL,
                    payload     JSONB        NOT NULL DEFAULT '{}',
                    processed_at TIMESTAMPTZ DEFAULT NOW(),
                    CONSTRAINT unique_topic_event UNIQUE (topic, event_id)
                )
            """)
            await conn.execute("""
                CREATE INDEX IF NOT EXISTS idx_topic
                ON processed_events(topic)
            """)
            # Tabel statistik — satu baris dengan id=1
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS stats (
                    id                INTEGER PRIMARY KEY DEFAULT 1,
                    received          BIGINT  DEFAULT 0,
                    unique_processed  BIGINT  DEFAULT 0,
                    duplicate_dropped BIGINT  DEFAULT 0,
                    started_at        TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute("""
                INSERT INTO stats (id) VALUES (1)
                ON CONFLICT (id) DO NOTHING
            """)
        logger.info("Tables initialized")

    async def process_event(self, event: dict) -> bool:
        """
        Menyimpan event ke DB dalam satu transaksi (READ COMMITTED).
        Menggunakan INSERT ... ON CONFLICT DO NOTHING untuk dedup atomik.
        Return True jika event baru, False jika duplikat.
        """
        topic    = event["topic"]
        event_id = event["event_id"]
        source   = event["source"]
        payload  = json.dumps(event.get("payload", {}))
        ts_str   = event["timestamp"].replace("Z", "+00:00")
        timestamp = datetime.fromisoformat(ts_str)

        async with self.pool.acquire() as conn:
            async with conn.transaction(isolation="read_committed"):
                result = await conn.execute(
                    """
                    INSERT INTO processed_events
                        (topic, event_id, timestamp, source, payload)
                    VALUES ($1, $2, $3, $4, $5::jsonb)
                    ON CONFLICT (topic, event_id) DO NOTHING
                    """,
                    topic, event_id, timestamp, source, payload,
                )
                # asyncpg returns "INSERT 0 N" – N=1 new, N=0 duplicate
                inserted = int(result.split()[-1])

                if inserted == 1:
                    await conn.execute(
                        "UPDATE stats SET unique_processed = unique_processed + 1 WHERE id = 1"
                    )
                    logger.info(f"PROCESSED  topic={topic} event_id={event_id}")
                    return True
                else:
                    await conn.execute(
                        "UPDATE stats SET duplicate_dropped = duplicate_dropped + 1 WHERE id = 1"
                    )
                    logger.info(f"DUPLICATE  topic={topic} event_id={event_id}")
                    return False

    async def increment_received_batch(self, count: int):
        async with self.pool.acquire() as conn:
            await conn.execute(
                "UPDATE stats SET received = received + $1 WHERE id = 1", count
            )

    async def get_events(self, topic: Optional[str] = None) -> List[Dict[str, Any]]:
        async with self.pool.acquire() as conn:
            if topic:
                rows = await conn.fetch(
                    """
                    SELECT topic, event_id, timestamp, source, payload, processed_at
                    FROM processed_events
                    WHERE topic = $1
                    ORDER BY processed_at DESC
                    LIMIT 1000
                    """,
                    topic,
                )
            else:
                rows = await conn.fetch(
                    """
                    SELECT topic, event_id, timestamp, source, payload, processed_at
                    FROM processed_events
                    ORDER BY processed_at DESC
                    LIMIT 1000
                    """
                )
        return [
            {
                "topic":        r["topic"],
                "event_id":     r["event_id"],
                "timestamp":    r["timestamp"].isoformat(),
                "source":       r["source"],
                "payload":      json.loads(r["payload"]),
                "processed_at": r["processed_at"].isoformat(),
            }
            for r in rows
        ]

    async def get_stats(self) -> Dict[str, Any]:
        async with self.pool.acquire() as conn:
            row    = await conn.fetchrow("SELECT * FROM stats WHERE id = 1")
            topics = await conn.fetch(
                "SELECT DISTINCT topic FROM processed_events ORDER BY topic"
            )
        return {
            "received":          row["received"],
            "unique_processed":  row["unique_processed"],
            "duplicate_dropped": row["duplicate_dropped"],
            "topics":            [t["topic"] for t in topics],
            "started_at":        row["started_at"].isoformat(),
        }
