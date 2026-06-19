# Pub-Sub Log Aggregator Terdistribusi

**UAS Sistem Terdistribusi **  
**Mahasiswa:** Miftahul Fauzi Rifai — 11231040  
**Program Studi:** Informatika, Institut Teknologi Kalimantan

---

## Arsitektur Sistem

```
Publisher ──HTTPS──▶ Aggregator (FastAPI)
                          │
                    RPUSH │ BLPOP (4 workers)
                          ▼
                      Redis Broker
                          │
                  INSERT ON CONFLICT DO NOTHING
                          ▼
                    PostgreSQL (storage)
                   ┌──────────────┐
                   │ processed_events │  ← UNIQUE(topic, event_id)
                   │ stats            │  ← atomic counters
                   └──────────────┘
```

### Komponen

| Service     | Image / Build         | Fungsi                                           |
|-------------|----------------------|--------------------------------------------------|
| aggregator  | `./aggregator`       | FastAPI API + 4 consumer workers                 |
| publisher   | `./publisher`        | Simulator event (35% duplikat)                   |
| broker      | `redis:7-alpine`     | Queue perantara (RPUSH/BLPOP)                    |
| storage     | `postgres:16-alpine` | Persistent dedup store + stats                   |

Semua service berjalan di network internal Compose (`internal`). Hanya aggregator yang expose port ke host (`:8080`).

---

## Cara Menjalankan

### 1. Build dan start semua service
```bash
docker compose up --build
```

### 2. Akses aggregator
```
http://localhost:8080
```

### 3. Cek stats secara realtime
```bash
watch -n 2 curl -s http://localhost:8080/stats | python3 -m json.tool
```

### 4. Jalankan publisher ulang (demo duplikat)
```bash
docker compose run --rm publisher
```

### 5. Load test dengan K6
```bash
# Pastikan aggregator sudah running
docker compose --profile load run k6
```

Atau K6 lokal:
```bash
k6 run -e BASE_URL=http://localhost:8080 k6/load_test.js
```

---

## API Endpoints

| Method | Path                   | Deskripsi                                    |
|--------|------------------------|----------------------------------------------|
| POST   | `/publish`             | Publish single/batch event                   |
| GET    | `/events?topic=<name>` | Daftar event unik yang telah diproses         |
| GET    | `/stats`               | Statistik (received, unique, dup, topics)    |
| GET    | `/health`              | Liveness/readiness probe                     |

### Contoh Request

**Single event:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{
    "topic": "user.login",
    "event_id": "evt-001",
    "timestamp": "2026-06-19T10:00:00+00:00",
    "source": "web-app",
    "payload": {"user_id": 42}
  }'
```

**Batch event:**
```bash
curl -X POST http://localhost:8080/publish \
  -H "Content-Type: application/json" \
  -d '{"events": [
    {"topic":"order.created","event_id":"ord-001","timestamp":"2026-06-19T10:00:00Z","source":"api","payload":{}},
    {"topic":"order.created","event_id":"ord-002","timestamp":"2026-06-19T10:00:01Z","source":"api","payload":{}}
  ]}'
```

**Get events:**
```bash
curl "http://localhost:8080/events?topic=user.login"
```

**Get stats:**
```bash
curl http://localhost:8080/stats
```

---

## Menjalankan Tests (20 tests)

Pastikan Docker Compose sudah berjalan (`docker compose up --build -d`), lalu:

```bash
# Install dependencies test
pip install pytest httpx

# Jalankan dari direktori tests/
cd tests
pytest -v

# Atau dari root
pytest tests/ -v
```

### Daftar Tests

| File                   | Tests | Cakupan                                     |
|------------------------|-------|---------------------------------------------|
| `test_publish.py`      | T01–T05 | Publish single, batch, array, empty, invalid |
| `test_dedup.py`        | T06–T08 | Dedup once, stats, batch dedup              |
| `test_stats.py`        | T09–T11 | Fields, received increment, topics          |
| `test_events.py`       | T12–T15 | List, filter, count, health                 |
| `test_concurrency.py`  | T16–T20 | Race condition, unique, performance, consistency |

---

## Persistensi Data

Data tersimpan di named volumes Docker:

| Volume      | Mount di Container              | Konten                     |
|-------------|---------------------------------|----------------------------|
| `pg_data`   | `/var/lib/postgresql/data`      | Postgres WAL + tables      |
| `broker_data`| `/data`                        | Redis RDB snapshot          |

**Bukti persistensi:**
```bash
# Hapus container (bukan volume)
docker compose down

# Restart
docker compose up -d aggregator storage broker

# Data tetap ada
curl http://localhost:8080/stats
```

---

## Desain Transaksi & Deduplication

```sql
-- Dalam satu transaksi (READ COMMITTED):
BEGIN;

INSERT INTO processed_events (topic, event_id, timestamp, source, payload)
VALUES ($1, $2, $3, $4, $5::jsonb)
ON CONFLICT (topic, event_id) DO NOTHING;

-- Jika INSERT 0 1 (baru):
UPDATE stats SET unique_processed = unique_processed + 1 WHERE id = 1;

-- Jika INSERT 0 0 (duplikat):
UPDATE stats SET duplicate_dropped = duplicate_dropped + 1 WHERE id = 1;

COMMIT;
```

Isolation level **READ COMMITTED** dipilih karena:
- Mencegah dirty reads
- Non-blocking untuk concurrent reads
- Conflict resolution dilakukan oleh UNIQUE constraint, bukan isolation level
- Lebih performant dari SERIALIZABLE untuk use case ini

---

## Variabel Environment

### Aggregator
| Variabel          | Default                                  | Deskripsi              |
|-------------------|------------------------------------------|------------------------|
| `DATABASE_URL`    | `postgresql://user:pass@storage:5432/logdb` | Postgres DSN        |
| `BROKER_URL`      | `redis://broker:6379`                    | Redis URL              |
| `CONSUMER_WORKERS`| `4`                                      | Jumlah worker consumer |

### Publisher
| Variabel          | Default  | Deskripsi                    |
|-------------------|----------|------------------------------|
| `TARGET_URL`      | `http://aggregator:8080/publish` | Aggregator URL  |
| `TOTAL_EVENTS`    | `1000`   | Total event yang dikirim     |
| `EVENTS_PER_BATCH`| `20`     | Ukuran batch per request     |
| `DUPLICATE_RATE`  | `0.35`   | Persentase duplikat (35%)    |
| `DELAY_MS`        | `50`     | Delay antar batch (ms)       |

---

## Link Video Demo

> https://youtu.be/z9hRiPWYkpo

## Laporan 

Pembahasan teori lengkap ada di [`report.md`](report.md).
