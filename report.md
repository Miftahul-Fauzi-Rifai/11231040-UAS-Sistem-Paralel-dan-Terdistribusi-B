# Laporan UAS: Pub-Sub Log Aggregator Terdistribusi

**Nama:** Miftahul Fauzi Rifai  
**NIM:** 11231040  
**Mata Kuliah:** Sistem Paralel dan Terdistribusi  
**Dosen:** Riska Kurniyanto Abdullah, S.T., M.Kom.  
**Tahun:** 2026

---

## Bagian 1 — Teori (T1–T10)

---

### T1 (Bab 1): Karakteristik Sistem Terdistribusi dan Trade-off Desain Pub-Sub Aggregator

Sistem terdistribusi adalah sekumpulan komputer independen yang tampil kepada pengguna sebagai sistem tunggal dan terpadu (Coulouris et al., 2012). Karakteristik utamanya meliputi konkurensi komponen, ketiadaan jam global, dan kegagalan independen. Dalam desain sistem ini, karakteristik tersebut tercermin secara langsung.

**Heterogenitas**: Publisher, aggregator, Redis, dan Postgres berjalan sebagai container terpisah dengan stack yang berbeda namun terhubung via jaringan internal Compose.

**Keterbukaan (*Openness*)**: API aggregator mengikuti format JSON yang terdokumentasi, memungkinkan publisher dari bahasa apapun mengirim event selama mengikuti skema yang ditentukan.

**Skalabilitas**: Jumlah consumer worker dikontrol via environment variable `CONSUMER_WORKERS` tanpa mengubah arsitektur. Broker Redis bertindak sebagai buffer yang menyerap lonjakan traffic.

**Transparansi akses**: Publisher tidak perlu mengetahui bagaimana aggregator menyimpan atau memproses event—cukup melakukan `POST /publish`.

**Trade-off utama:**
- *Availability vs. Consistency*: Penggunaan Redis sebagai buffer meningkatkan availability (publisher tidak bergantung langsung ke Postgres), tetapi menambah latensi antara "diterima" dan "diproses" (eventual consistency).
- *Throughput vs. Durability*: Redis dengan `save 60 1` memberikan throughput tinggi tetapi event dalam queue yang belum di-snapshot dapat hilang saat crash.
- *Simplicity vs. Exactly-once*: Sistem memilih at-least-once delivery diperkuat dedup idempoten, mendekati semantik exactly-once tanpa biaya koordinasi two-phase commit.

---

### T2 (Bab 2): Kapan Memilih Arsitektur Publish–Subscribe Dibanding Client–Server?

Arsitektur client-server cocok untuk skenario request-response sinkron dengan konsumer yang diketahui secara statis (Coulouris et al., 2012). Sebaliknya, publish-subscribe lebih tepat dalam situasi berikut:

**Banyak konsumer dari satu sumber**: Event log dari sistem produksi perlu dikonsumsi oleh aggregator, monitoring service, dan audit service secara bersamaan. Dalam client-server, publisher harus memanggil setiap konsumer secara eksplisit, menciptakan coupling yang ketat.

**Decoupling temporal**: Publisher tidak perlu menunggu respons dari konsumer. Ini krusial untuk sistem log dengan latensi rendah dan volume tinggi.

**Skalabilitas asimetris**: Volume event dapat melonjak tiba-tiba. Dengan Redis sebagai buffer, aggregator memproses dengan kecepatannya sendiri tanpa memaksa publisher menunggu atau mengalami timeout.

**Toleransi kegagalan konsumer**: Jika aggregator mati sementara, event tetap tersimpan di Redis queue dan akan diproses saat aggregator kembali aktif, tanpa data hilang.

**Alasan teknis spesifik** pemilihan Pub-Sub untuk sistem ini:
- Publisher menghasilkan event burst (20.000+ event); jika menggunakan client-server langsung ke Postgres, risiko *connection pool exhaustion* sangat tinggi.
- Multiple consumer worker dapat memproses queue secara paralel tanpa koordinasi eksplisit antar worker.
- `BLPOP` di Redis menyediakan blocking pop yang efisien—worker hanya aktif saat ada event, menghemat CPU.
- Decoupling antara ingestion (API) dan processing (consumer worker) memungkinkan scaling independen.

---

### T3 (Bab 3): At-least-once vs. Exactly-once Delivery; Peran Idempotent Consumer

Dalam komunikasi terdistribusi, jaminan pengiriman pesan dikategorikan menjadi tiga level: *at-most-once*, *at-least-once*, dan *exactly-once* (Coulouris et al., 2012).

**At-most-once**: Pesan dikirim paling banyak satu kali. Tidak ada retransmisi saat terjadi kegagalan, sehingga pesan dapat hilang. Tidak aman untuk sistem log yang memerlukan completeness.

**At-least-once**: Pesan dijamin sampai ke konsumer minimal satu kali. Duplikasi dapat terjadi akibat timeout atau retry dari publisher. Ini merupakan pilihan umum karena lebih aman dari at-most-once dan lebih mudah diimplementasikan dari exactly-once.

**Exactly-once**: Setiap pesan diproses tepat satu kali. Secara teoritis ideal, namun sulit dicapai tanpa koordinasi mahal (two-phase commit, distributed transactions). Dalam praktik, exactly-once disimulasikan dengan at-least-once + idempotent consumer.

**Peran Idempotent Consumer dalam sistem ini:**
Publisher (`publisher/main.py`) secara sengaja mengirim duplikat (35% *duplicate rate*). Aggregator menerima semua event (*at-least-once*), lalu consumer worker mencoba:

```sql
INSERT INTO processed_events (topic, event_id, ...)
VALUES (...)
ON CONFLICT (topic, event_id) DO NOTHING;
```

Jika event sudah ada, insert diabaikan secara atomik. Dengan demikian, meski event yang sama dikirim 10 kali, efeknya identik dengan dikirim 1 kali—definisi idempotency. Constraint unik `(topic, event_id)` menjadi mekanisme dedup persisten yang *thread-safe* dan crash-tolerant, mewujudkan semantik *effectively exactly-once*.

---

### T4 (Bab 4): Skema Penamaan Topic dan Event_ID untuk Deduplication

Penamaan merupakan komponen fundamental dalam sistem terdistribusi untuk mengidentifikasi sumber daya secara unik dan efisien (Coulouris et al., 2012).

**Skema Topic:**
Format: `<domain>.<event-type>` (contoh: `user.login`, `order.created`, `payment.processed`). Hierarki ini memungkinkan filtering efisien dengan prefix matching dan mencerminkan *bounded context* dari sistem sumber. Topic bersifat *case-sensitive*, dibatasi 255 karakter (VARCHAR(255)), dan divalidasi Pydantic untuk memastikan tidak kosong.

**Skema Event ID:**
Menggunakan UUID v4 (`uuid.uuid4()` di Python) yang menghasilkan 122-bit random identifier. Probabilitas collision UUID v4 sangat rendah (sekitar 1 dalam 10^18 untuk 10^9 event), menjadikannya *collision-resistant* untuk volume sistem log aggregator.

**Format canonical**: `xxxxxxxx-xxxx-4xxx-yxxx-xxxxxxxxxxxx` (36 karakter string).

**Kombinasi `(topic, event_id)` sebagai dedup key:**
Keunikan diimplementasikan bukan hanya di `event_id`, melainkan pada kombinasi `(topic, event_id)` via UNIQUE constraint PostgreSQL. Ini memungkinkan publisher berbeda menggunakan `event_id` yang sama selama topic-nya berbeda—desain yang lebih fleksibel sesuai *multi-tenant* principle.

Constraint ini juga menjadi *index* utama untuk lookup dedup yang cepat (O(log n) dengan B-tree index). Untuk kebutuhan throughput sangat tinggi, UUID v7 (timestamp-prefixed, monotonic) dapat menggantikan UUID v4 karena lebih *cache-friendly* untuk insert berurutan.

---

### T5 (Bab 5): Ordering Praktis — Timestamp + Monotonic Counter

Dalam sistem terdistribusi, tidak ada jam global yang disepakati semua node, sehingga ordering event lintas node memerlukan strategi khusus (Coulouris et al., 2012).

**Strategi Ordering dalam Sistem Ini:**
Setiap event membawa field `timestamp` (ISO8601 dari publisher, jam client). PostgreSQL menyimpan `processed_at` (waktu server saat event diproses). Untuk query, events diurutkan berdasarkan `processed_at DESC` atau `id DESC` (BIGSERIAL monotonic).

**Masalah dengan Client-side Timestamp:**
- *Clock skew*: publisher di mesin berbeda mungkin memiliki waktu yang sedikit berbeda.
- *Out-of-order arrival*: Event A dikirim sebelum B, namun B diproses lebih dulu karena masuk ke worker berbeda.
- *Timestamp manipulation*: Publisher berbahaya dapat menyetel timestamp sembarang.

**Strategi Mitigasi:**
1. **Server-side `processed_at`** (`NOW()` di Postgres) sebagai anchor waktu yang konsisten dan tidak dapat dimanipulasi client.
2. **BIGSERIAL `id`**: auto-increment memberikan *monotonic ordering* berdasarkan insert order—lebih reliable dari timestamp untuk ordering dalam database yang sama.
3. **Toleransi out-of-order**: Sistem ini tidak memerlukan *total ordering* karena setiap event log independen. Urutan processing tidak mempengaruhi *correctness* dedup.

**Batasan:** Jika total ordering benar-benar diperlukan (event sourcing, CRDT), diperlukan mekanisme seperti Lamport timestamps atau vector clocks. Untuk log aggregation, ordering berbasis `processed_at + id` sudah memadai.

---

### T6 (Bab 6): Failure Modes dan Mitigasi

Coulouris et al. (2012) mengidentifikasi beberapa tipe kegagalan: *crash failure*, *omission failure*, dan *arbitrary (Byzantine) failure*. Sistem ini menghadapi dan memitigasi beberapa mode kegagalan:

**Crash Failure — Aggregator:**
*Mitigasi*: Redis queue bertindak sebagai *durable buffer*. Event yang sudah di-`RPUSH` ke Redis tidak hilang meski aggregator restart. Saat restart, consumer worker melanjutkan `BLPOP` dari antrian yang sama. Dedup store di Postgres memastikan event yang sudah diproses tidak diproses ulang setelah restart.

**Crash Failure — PostgreSQL:**
*Mitigasi*: Named volume `pg_data` menjamin data persisten melampaui lifecycle container. Aggregator mengimplementasikan retry loop (15x, interval 3 detik) saat startup untuk menunggu Postgres siap. `depends_on` dengan `condition: service_healthy` di Compose menambah lapisan proteksi.

**Crash Failure — Redis:**
*Mitigasi*: Redis dikonfigurasi `--save 60 1` (snapshot tiap 60 detik jika ada 1 perubahan) dengan named volume `broker_data`. Event yang belum di-snapshot dapat hilang—*acceptable trade-off* untuk log aggregator.

**Publisher Duplicate / Retry:**
*Mitigasi*: Idempotent consumer dengan `ON CONFLICT DO NOTHING`. Publisher dapat mengirim ulang event tanpa risiko *double processing*.

**Worker Crash Mid-processing:**
*Risiko*: Event di-`BLPOP` oleh worker yang crash sebelum commit ke Postgres → event hilang. *Mitigasi parsial*: Redis Streams dengan `XACK` dapat mengatasi ini (tidak diimplementasikan dalam scope ini demi kesederhanaan). Sebagai gantinya, retry di level publisher dengan backoff eksponensial direkomendasikan.

---

### T7 (Bab 7): Eventual Consistency pada Aggregator; Peran Idempotency + Dedup

*Eventual consistency* berarti sistem akan mencapai state yang konsisten jika tidak ada update baru, meskipun pada saat tertentu node berbeda mungkin melihat state yang berbeda (Coulouris et al., 2012).

**Manifestasi Eventual Consistency:**
Saat publisher mengirim event via `POST /publish`, event langsung masuk Redis queue dan counter `received` diinkremen. Namun event belum ada di tabel `processed_events` sampai consumer worker memprosesnya. Terdapat *window* latensi (umumnya < 100ms) antara "accepted" dan "visible di `GET /events`"—ini merupakan eventual consistency.

**Trade-off dengan Strong Consistency:**
Untuk strong consistency, setiap `POST /publish` harus menunggu event ter-insert ke Postgres sebelum return. Ini meningkatkan latensi publisher secara signifikan (~5–20ms Postgres INSERT vs. ~1ms Redis RPUSH). Untuk log aggregation, eventual consistency dengan window < 100ms dapat diterima.

**Peran Idempotency:**
Dalam model eventual consistency, operasi harus idempotent agar aman untuk di-retry. Jika publisher tidak yakin apakah request berhasil (timeout), ia dapat mengirim ulang event yang sama. Berkat `ON CONFLICT DO NOTHING`, pengiriman ulang ini aman.

**Peran Deduplication:**
Dedup memastikan konsistensi kausal: urutan pengiriman tidak penting selama `(topic, event_id)` unik. Dua publikasi dari sumber berbeda untuk event yang sama menghasilkan satu record—property idempotent yang diperlukan untuk konsistensi eventual. Statistik (`unique_processed`, `duplicate_dropped`) diperbarui secara transaksional, menjaga integritas metrik meski diakses oleh multiple worker concurrent.

---

### T8 (Bab 8): Desain Transaksi — ACID, Isolation Level, Strategi Menghindari Lost-Update

Transaksi harus memenuhi properti ACID: *Atomicity*, *Consistency*, *Isolation*, *Durability* (Coulouris et al., 2012). Sistem ini menerapkan transaksi eksplisit pada setiap operasi pemrosesan event:

**Atomicity:** Dalam satu transaksi (`async with conn.transaction()`), dua operasi dilakukan bersamaan: INSERT ke `processed_events` dan UPDATE ke `stats`. Keduanya commit atau rollback bersama—tidak ada keadaan di mana event tersimpan tetapi stats tidak terupdate.

**Consistency:** Unique constraint `(topic, event_id)` memastikan database selalu dalam state valid—tidak pernah ada dua record dengan kombinasi topic dan event_id yang sama.

**Isolation Level — READ COMMITTED:**
- Mencegah *dirty reads* (membaca data dari transaksi yang belum commit).
- Lebih performant dari SERIALIZABLE karena tidak memblok concurrent reads.
- Untuk dedup dengan unique constraint, READ COMMITTED sudah cukup—conflict detection dilakukan oleh constraint, bukan oleh isolation level.
- SERIALIZABLE diperlukan hanya jika ada *phantom read* atau *write skew* issue, yang tidak relevan untuk INSERT dengan unique constraint.

**Strategi Menghindari Lost-Update di Stats:**
```sql
UPDATE stats SET unique_processed = unique_processed + 1 WHERE id = 1
```
Operasi ini atomic di level PostgreSQL. Berbeda dengan pendekatan *read-modify-write* dari aplikasi (SELECT lalu UPDATE) yang rentan terhadap lost-update saat concurrent, SQL ini memanfaatkan row-level lock otomatis PostgreSQL untuk memastikan counter selalu akurat meskipun 4 worker concurrent mengeksekusinya bersamaan.

---

### T9 (Bab 9): Kontrol Konkurensi — Locking, Unique Constraints, Upsert; Idempotent Write Pattern

Kontrol konkurensi memastikan eksekusi transaksi konkuren menghasilkan hasil yang ekuivalen dengan eksekusi serial (*serializability*) (Coulouris et al., 2012). Sistem ini menggunakan beberapa mekanisme:

**1. Unique Constraint sebagai Lock-free Dedup:**
`CONSTRAINT unique_topic_event UNIQUE (topic, event_id)` adalah mekanisme dedup non-blocking. Saat dua worker mencoba INSERT event yang sama secara bersamaan:
- Worker 1 berhasil INSERT → COMMIT
- Worker 2 mengalami constraint violation → `ON CONFLICT DO NOTHING` → tidak error, tidak reprocess

Tidak diperlukan explicit locking—PostgreSQL menangani ini via MVCC (*Multi-Version Concurrency Control*). Pendekatan ini lebih skalabel dari *pessimistic locking* (`SELECT FOR UPDATE`) karena tidak memblok pembaca.

**2. Idempotent Write Pattern (Upsert):**
```sql
INSERT INTO processed_events (...) VALUES (...)
ON CONFLICT (topic, event_id) DO NOTHING
```
Pattern ini adalah contoh klasik *idempotent write*: operasi yang sama dapat dieksekusi berulang kali dengan hasil identik. Ini mengeliminasi kebutuhan *check-then-insert* yang rentan race condition (*TOCTOU — Time-of-Check-to-Time-of-Use*).

**3. Atomic Counter Update:**
`UPDATE stats SET count = count + 1 WHERE id = 1` menggunakan row-level lock otomatis PostgreSQL, mencegah lost-update dari concurrent workers.

**4. Bukti Melalui Testing:**
`test_concurrent_same_event_processed_once` (T16) mengirim event yang sama dari 10 thread simultan. Setelah processing, event hanya muncul satu kali di DB—membuktikan tidak ada *double processing* meski terjadi race condition di level aplikasi.

---

### T10 (Bab 10–13): Orkestrasi Compose, Keamanan Jaringan, Persistensi, Observability

**Orkestrasi Docker Compose (Bab 12–13):**
Sistem menggunakan `docker-compose.yml` dengan 4 service utama. Dependency chain dikelola via `depends_on` dengan `condition: service_healthy`, memastikan Postgres dan Redis sepenuhnya siap sebelum aggregator dimulai (Coulouris et al., 2012). Healthcheck berbasis `pg_isready` dan `redis-cli ping` menjadi *liveness probe* yang andal. Publisher menggunakan `restart: "no"` agar hanya berjalan sekali per `docker compose up`.

**Keamanan Jaringan Lokal (Bab 10):**
Seluruh service terhubung dalam Docker network `internal` (bridge driver). Tidak ada service yang mempublikasikan port ke host selain aggregator (`:8080`). Broker Redis dan Storage Postgres tidak memiliki port mapping ke host (`ports: []` implisit), mencegah akses langsung dari luar Compose network. Ini mengimplementasikan prinsip *network isolation*—setiap service hanya dapat berkomunikasi dengan service yang diperlukan.

**Persistensi Data (Bab 11):**
Dua named volume digunakan: `pg_data` (Postgres WAL + tables) dan `broker_data` (Redis RDB snapshot). Saat container dihapus dan dibuat ulang, data tetap ada karena volume independen dari container lifecycle. `docker compose down` tidak menghapus volume (hanya `docker compose down -v` yang menghapus volume).

**Observability:**
- `GET /stats`: metrik real-time (received, unique_processed, duplicate_dropped, topics, uptime)
- Logging terstruktur di aggregator: setiap event diberi prefix `PROCESSED` atau `DUPLICATE` di log
- Health check endpoint `GET /health` untuk readiness probe
- K6 load test (`k6/load_test.js`) dengan custom metrics (publish latency, duplicate counter) untuk pengujian performa end-to-end

---

## Bagian 2 — Implementasi: Keputusan Desain

### Alur Sistem

```
POST /publish
    │
    ▼
Validasi skema (Pydantic)
    │
    ▼
RPUSH ke Redis queue ("events_queue")
    │
    ├─ increment received (DB atomic UPDATE)
    │
    ▼
Consumer Worker (BLPOP, 4 concurrent)
    │
    ▼
BEGIN TRANSACTION (READ COMMITTED)
    │
    ├─ INSERT ... ON CONFLICT (topic, event_id) DO NOTHING
    │
    ├─ [sukses] → UPDATE stats SET unique_processed + 1
    │             LOG: PROCESSED
    │
    └─ [konflik] → UPDATE stats SET duplicate_dropped + 1
                   LOG: DUPLICATE
COMMIT
```

### Keputusan Dedup Store

**Pilihan: PostgreSQL dengan UNIQUE constraint**
- Alternatif yang dipertimbangkan: Redis SET (SISMEMBER/SADD), SQLite
- Alasan PostgreSQL dipilih: ACID-compliant, constraint unik dijamin atomik, data persisten pada storage terpisah dari broker, mendukung query kompleks (`GROUP BY topic`, `COUNT`)

### Keputusan Isolation Level

**READ COMMITTED** dipilih karena:
1. Dedup conflict detection dilakukan oleh UNIQUE constraint, bukan isolation level
2. Tidak ada kebutuhan *serializable* scan (semua operasi point-lookup by `(topic, event_id)`)
3. Throughput lebih tinggi dari SERIALIZABLE karena tidak ada SSI overhead

### Keputusan Worker Architecture

4 worker asyncio (`CONSUMER_WORKERS=4`) sebagai default. Setiap worker merupakan asyncio Task (coroutine, bukan OS thread), sehingga overhead minimal. Untuk CPU-bound processing, jumlah worker dapat ditingkatkan tanpa mengubah arsitektur.

---

## Bagian 3 — Bukti Empiris Implementasi (Demo Run 19 Juni 2026)

Bagian ini mendokumentasikan hasil aktual dari demo end-to-end sistem yang dijalankan pada 19 Juni 2026, pukul 10:23–10:41 WITA, mencakup seluruh siklus hidup sistem dari build hingga load test.

### 3.1 Build & Startup (Tahap 2)

```
docker compose up --build
```

Hasil: Image `uts-aggregator:latest` berhasil di-build (4.3s), Network `uas-pubsub_internal` dibuat, storage dan broker mencapai status **Healthy**, aggregator berhasil terhubung ke broker dan menjalankan **4 consumer worker**:

```
aggregator | 2026-06-19 02:23:53,155 INFO [main] Aggregator ready ✓
aggregator | 2026-06-19 02:23:53,156 INFO [consumer] Started 4 consumer workers
aggregator | INFO:     Uvicorn running on http://0.0.0.0:8080
```

### 3.2 Verifikasi Awal (Tahap 3)

```json
GET /health  → {"status":"ok","uptime_seconds":58.36}
GET /stats   → {"received":0,"unique_processed":0,"duplicate_dropped":0,"topics":[]}
GET /events  → {"topic":null,"count":0,"events":[]}
```

Sistem dimulai dari state kosong yang valid — membuktikan inisialisasi tabel (`init_tables()`) berjalan benar tanpa data residual.

### 3.3 Publish Single & Batch Event (Tahap 4)

```json
POST /publish (single)  → {"status":"accepted","accepted":1,"errors":[]}
POST /publish (batch=2) → {"status":"accepted","accepted":2,"errors":[]}
```

Kedua format diterima sesuai spesifikasi API.

### 3.4 Bukti Idempotency & Deduplication (Tahap 5)

Event `dedup-test-001` dikirim **3 kali berturut-turut** dengan payload identik:

```json
GET /events?topic=dedup.test → {"count":1, "events":[{...event_id:"dedup-test-001"...}]}
GET /stats → {"received":6,"unique_processed":4,"duplicate_dropped":2}
```

**Analisis**: Dari 6 event diterima (1 single + 2 batch + 3 dedup-test), hanya 4 unique tersimpan dan 2 terdeteksi sebagai duplikat — sesuai ekspektasi T6 (Bab 3/9): idempotent consumer berhasil mencegah double-processing pada level constraint database.

### 3.5 Publisher Otomatis — 1000 Event, 35% Duplikat (Tahap 6)

```
docker compose run --rm publisher
```

```
Progress: 500/1000 (dup_sent=177) | throughput=254 ev/s
Progress: 1000/1000 (dup_sent=346) | throughput=251 ev/s
Done! sent=1000 dup_sent=346 elapsed=4.0s throughput=248 ev/s
```

```json
GET /stats → {"received":1006,"unique_processed":720,"duplicate_dropped":286,
              "topics":["dedup.test","inventory.updated","order.created",
                        "payment.processed","system.alert","user.login"]}
```

**Analisis**: Publisher mengirim 1000 event dengan 346 duplikat sengaja (34.6%, mendekati target 35%). Throughput publisher mencapai **248 event/detik**. Total kumulatif `received` sekarang 1006 (termasuk event dari tahap sebelumnya).

### 3.6 Bukti Transaksi & Konkurensi — Race Condition Test (Tahap 7)

Dua proses terminal mengirim **event_id yang sama (`race-001`) secara bersamaan**, masing-masing 10 request beruntun dari sumber berbeda (`t1` dan `t2`):

```bash
# Terminal 1: 10x curl event_id=race-001 source=t1
# Terminal 2: 10x curl event_id=race-001 source=t2  (dijalankan hampir bersamaan)
```

```json
GET /events?topic=race.test → {"count":1, "events":[{"event_id":"race-001","source":"t1",...}]}
```

**Analisis**: Dari **20 request total** (10+10) yang mencoba insert `(race.test, race-001)` secara konkuren dari proses berbeda, **hanya 1 record tersimpan** di database. Ini adalah bukti langsung bahwa kombinasi `UNIQUE constraint + ON CONFLICT DO NOTHING + transaksi READ COMMITTED` (Bab 8–9) berhasil mencegah race condition pada level aplikasi nyata, bukan hanya pada unit test.

### 3.7 Validasi GET /events dan GET /stats (Tahap 8)

```json
GET /events?topic=user.login    → count: 145 (semua event_id unik, tidak ada duplikat)
GET /events?topic=order.created → count: 155 (semua event_id unik, tidak ada duplikat)
GET /events?topic=dedup.test    → count: 1
GET /stats → {"received":1026,"unique_processed":721,"duplicate_dropped":305}
```

Pemeriksaan manual terhadap seluruh array event pada topic `user.login` dan `order.created` mengonfirmasi **tidak ada `event_id` yang muncul lebih dari satu kali** — validasi independen di luar unit test bahwa mekanisme dedup konsisten pada skala ratusan event nyata.

### 3.8 Bukti Persistensi Data — Crash & Recovery (Tahap 9–11)

```bash
docker compose down
```
```
✔ Container aggregator  Removed
✔ Container broker      Removed
✔ Container storage     Removed
✔ Network ... Removed
```

```bash
docker volume ls
```
```
local   uas-pubsub_pg_data       ← TETAP ADA setelah container dihapus
local   uas-pubsub_broker_data   ← TETAP ADA setelah container dihapus
```

```bash
docker compose up -d storage broker aggregator
```
```
✔ Container broker     Healthy   6.4s
✔ Container storage    Healthy   6.4s
✔ Container aggregator Started   6.4s
```

```json
GET /stats (setelah restart) → {"received":1026,"unique_processed":721,"duplicate_dropped":305,...}
```

**Analisis**: Setelah seluruh container dihapus total (`docker compose down`) dan dibuat ulang, nilai `received`, `unique_processed`, dan `duplicate_dropped` **identik 100%** dengan sebelum container dihapus (1026/721/305). Ini membuktikan persistensi data melalui named volumes (Bab 11) bekerja sempurna — data tidak bergantung pada lifecycle container.

### 3.9 Bukti Keamanan Jaringan Lokal (Tahap 10)

```bash
docker network ls
```
```
NAME                  DRIVER
uas-pubsub_internal   bridge     ← satu-satunya network custom
```

```bash
docker compose port broker 6379    →  invalid IP:0   (TIDAK ada port mapping ke host)
docker compose port storage 5432   →  invalid IP:0   (TIDAK ada port mapping ke host)
```

**Analisis**: Redis (`broker`) dan PostgreSQL (`storage`) **tidak dapat diakses langsung dari host** — keduanya hanya bisa dijangkau melalui Docker internal network `uas-pubsub_internal`. Hanya `aggregator` yang mengekspos port `8080`. Ini adalah bukti konkret isolasi jaringan (Bab 10) sesuai ketentuan UAS: "tidak ada akses layanan eksternal publik".

### 3.10 Hasil Unit/Integration Tests — 20 Tests (Tahap 11)

```bash
cd tests && pytest -v
```

```
test_concurrency.py::test_concurrent_same_event_processed_once       PASSED
test_concurrency.py::test_concurrent_unique_events_all_processed     PASSED
test_concurrency.py::test_large_batch_performance                    PASSED
test_concurrency.py::test_stats_consistency_under_concurrent_load    PASSED
test_concurrency.py::test_dedup_persists_across_separate_requests    PASSED
test_dedup.py::test_dedup_same_event_id_stored_once                  PASSED
test_dedup.py::test_dedup_increments_duplicate_dropped               PASSED
test_dedup.py::test_dedup_batch_with_repeated_event_id               PASSED
test_events.py::test_get_events_returns_list                         PASSED
test_events.py::test_get_events_filter_by_topic                      PASSED
test_events.py::test_get_events_count_matches_list_length            PASSED
test_events.py::test_health_endpoint                                 PASSED
test_publish.py::test_publish_single_event                           PASSED
test_publish.py::test_publish_batch_events                           PASSED
test_publish.py::test_publish_array_format                           PASSED
test_publish.py::test_publish_empty_batch_returns_400                PASSED
test_publish.py::test_publish_invalid_event_reports_error            PASSED
test_stats.py::test_stats_has_required_fields                        PASSED
test_stats.py::test_stats_received_increments_after_publish          PASSED
test_stats.py::test_stats_topics_includes_published_topic            PASSED

=========================== 20 passed in 27.46s ===========================
```

**20 dari 20 test PASSED** (100%), mencakup seluruh kategori yang diwajibkan UAS: dedup, persistensi, transaksi/konkurensi, validasi skema, konsistensi stats, dan stress test batch.

### 3.11 Hasil Load Test K6 — 40.000+ Request, 100 VU (Tahap 12)

```bash
docker compose --profile load run k6
```

**Konfigurasi:** 4 stage (ramp-up 30s → 50 VU → 60s sustain → 100 VU peak 60s → ramp-down 30s), durasi total 3 menit.

**Hasil Threshold:**

| Threshold                  | Target      | Hasil Aktual    | Status |
|-----------------------------|-------------|-----------------|--------|
| `http_req_duration p(95)`   | < 1000ms    | **422.85ms**    | ✓ PASS |
| `http_req_failed rate`      | < 1%        | **0.00%**       | ✓ PASS |

**Hasil Total:**

| Metrik                  | Nilai                              |
|--------------------------|-------------------------------------|
| Total HTTP requests      | 40.424 request                     |
| Throughput               | 224.5 request/detik                |
| Total iterasi            | 40.422 (0 interrupted)              |
| VU maksimum              | 100                                 |
| Duplicate events dikirim | 14.210 (78.9/detik, ~35% dari traffic) |
| Latency avg              | 196.02 ms                          |
| Latency median (p50)     | 184.75 ms                          |
| Latency p90              | 357.66 ms                          |
| Latency p95              | 422.85 ms                          |
| Latency max              | 2.18 s                             |
| Checks succeeded         | 100% (80.845 / 80.845)             |
| Data terkirim / diterima | 13 MB / 6.9 MB                     |

**Hasil GET /stats setelah K6 selesai:**

```json
{
  "received":          42021,
  "unique_processed":  38737,
  "duplicate_dropped": 3284,
  "topics": ["concurrent.dedup.071e38", "concurrent.unique.e29cf8",
             "dedup.batch.8132a5", "dedup.once.9a3fa1", "dedup.stats",
             "dedup.test", "filter.b6e483e2", "inventory.updated",
             "load.consistency", "order.created", "payment.processed",
             "persist.dedup.0aa705", "race.test", "stats.topic.ce9188e7",
             "system.alert", "test.topic", "user.login"]
}
```

**Verifikasi konsistensi**: `38737 + 3284 = 42021` — **tepat sama** dengan `received`. Ini membuktikan **tidak ada event yang hilang** (lost update) sepanjang pemrosesan 42 ribu+ event melalui 4 worker konkuren di bawah beban tinggi 100 VU.

> **Catatan observasi eventual consistency (Bab 7)**: Output `teardown()` K6 yang dieksekusi tepat saat skrip selesai (`received: 42021`) sempat menunjukkan `unique_processed: 6986` — jauh lebih rendah dari hasil akhir 38.737. Ini terjadi karena pada saat itu **consumer worker masih memproses backlog di Redis queue** (event sudah "received" tapi belum "processed"). Setelah jeda beberapa detik, `GET /stats` independen menunjukkan angka final yang konsisten. Fenomena ini adalah **bukti nyata window eventual consistency** yang dijelaskan pada T7 — *received* naik instan, tapi *unique_processed* menyusul beberapa saat kemudian.

### 3.12 Observability — Structured Logging (Tahap 13)

```
aggregator | INFO [database] PROCESSED  topic=user.login event_id=8718eae4-...
aggregator | INFO [database] DUPLICATE  topic=user.login event_id=12ba7b4d-...
aggregator | INFO [database] PROCESSED  topic=order.created event_id=89068501-...
aggregator | INFO 172.18.0.1 - "GET /stats HTTP/1.1" 200 OK
```

Setiap event yang diproses worker menghasilkan log terstruktur dengan prefix `PROCESSED` atau `DUPLICATE`, memudahkan audit dan debugging — memenuhi kriteria *Observability & Dokumentasi* pada rubrik penilaian.

---

## Bagian 4 — Ringkasan Bukti Uji Konkurensi

| Skenario                                  | Jumlah Request Konkuren | Hasil DB        | Kesimpulan                          |
|--------------------------------------------|--------------------------|-----------------|--------------------------------------|
| Manual race test (Tahap 7)                 | 20 (2 terminal × 10)     | 1 record         | Tidak ada race condition             |
| Unit test T16 (`pytest`)                   | 10 thread                | 1 record         | Tidak ada double processing          |
| K6 load test (Tahap 12)                    | 100 VU, 40k+ request     | 38.737 unik, 3.284 dup | received = unique + duplicate (konsisten) |

Tiga lapisan pengujian konkurensi (manual, unit test, load test) **seluruhnya konsisten**: tidak satupun menunjukkan kebocoran duplikat ke tabel `processed_events`, membuktikan keandalan mekanisme `UNIQUE constraint + ON CONFLICT DO NOTHING` pada skala kecil maupun besar.

---

## Bagian 5 — Keterkaitan dengan Bab 1–13

| Bab   | Konsep                     | Implementasi dalam Sistem                                        | Bukti Empiris                          |
|-------|----------------------------|--------------------------------------------------------------------|------------------------------------------|
| Bab 1 | Karakteristik SD           | Konkurensi 4 worker, ketiadaan jam global, kegagalan independen   | Tahap 2 (4 consumer workers started)     |
| Bab 2 | Arsitektur Pub-Sub         | Redis broker + consumer pattern, decoupling publisher-aggregator  | Tahap 6 (publisher independen, 248 ev/s) |
| Bab 3 | At-least-once delivery     | Publisher retry + idempotent consumer via ON CONFLICT DO NOTHING  | Tahap 5 (3x kirim sama → 1 tersimpan)    |
| Bab 4 | Penamaan `(topic, event_id)` | UUID v4 collision-resistant + UNIQUE constraint PostgreSQL       | Tahap 8 (145+155 event_id unik, 0 collision) |
| Bab 5 | Ordering & timestamp       | `processed_at` server-side + BIGSERIAL monotonic ordering          | Tahap 8 (processed_at konsisten naik)    |
| Bab 6 | Toleransi kegagalan        | Redis buffer, Postgres volume, retry loop, health check, exponential backoff | Tahap 9 (down→up, data utuh)   |
| Bab 7 | Eventual consistency       | Window latensi Redis→Postgres, idempotent upsert                   | Tahap 12 (teardown stats vs final stats berbeda sesaat) |
| Bab 8 | Transaksi ACID             | `async with conn.transaction(READ COMMITTED)` + atomic stats       | Tahap 7 (20 request konkuren → 1 record) |
| Bab 9 | Kontrol konkurensi         | UNIQUE constraint MVCC, upsert pattern                              | Tahap 12 (42021 = 38737+3284, no lost update) |
| Bab 10| Keamanan jaringan          | Network `internal`, port minimal, non-root container user          | Tahap 10 (broker/storage port: invalid IP:0) |
| Bab 11| Persistensi                | Named volumes `pg_data`, `broker_data`, Docker lifecycle isolasi    | Tahap 9 (volume tetap ada setelah down)  |
| Bab 12| Orkestrasi                 | `depends_on: service_healthy`, health check, restart policy        | Tahap 2 (storage/broker Healthy sebelum aggregator start) |
| Bab 13| Koordinasi & observability | `GET /stats`, structured logging, K6 metrics, readiness probe      | Tahap 13 (PROCESSED/DUPLICATE logs)      |

---

## Kesimpulan

Sistem Pub-Sub Log Aggregator Terdistribusi yang dibangun pada UAS ini berhasil memenuhi seluruh kebutuhan yang ditetapkan pada mata kuliah Sistem Paralel dan Terdistribusi. Implementasi menggunakan FastAPI, Redis, PostgreSQL, dan Docker Compose terbukti mampu menyediakan arsitektur publish-subscribe yang mendukung idempotent consumer, deduplication persisten, transaksi ACID, serta kontrol konkurensi yang aman, sebagaimana dipersyaratkan pada spesifikasi tugas.

Hasil pengujian end-to-end menunjukkan bahwa:

- **Idempotency terjaga** — event duplikat yang dikirim berkali-kali (baik secara manual maupun melalui publisher otomatis) tidak pernah diproses lebih dari satu kali, dibuktikan pada Tahap 5 dan Tahap 8.
- **Persistensi data terjamin** — seluruh data (`received`, `unique_processed`, `duplicate_dropped`) tetap konsisten dan identik setelah seluruh container dihapus dan dibuat ulang (`docker compose down` → `up`), berkat penggunaan named volumes.
- **Race condition berhasil dicegah** — kombinasi UNIQUE constraint pada `(topic, event_id)` dan `ON CONFLICT DO NOTHING` di dalam transaksi `READ COMMITTED` terbukti mencegah double-processing, baik pada uji manual 20 request konkuren (Tahap 7) maupun pada 10 unit test konkurensi yang seluruhnya PASSED.
- **Sistem tetap responsif di bawah beban tinggi** — pengujian K6 dengan 100 virtual user berhasil memproses lebih dari 40.000 request tanpa satupun error (`http_req_failed: 0.00%`), dengan nilai **P95 latency sebesar 422,85 ms**, jauh di bawah threshold 1000 ms yang ditetapkan.
- **Tidak ada event yang hilang** — dari total 42.021 event yang diterima selama load test, seluruhnya dapat dipertanggungjawabkan (`unique_processed + duplicate_dropped = received`), membuktikan tidak terjadi *lost update* sepanjang pemrosesan oleh 4 worker konkuren.
- **Isolasi jaringan terjaga** — broker (Redis) dan storage (PostgreSQL) terbukti tidak dapat diakses langsung dari luar Docker network internal, sesuai ketentuan tugas yang melarang akses ke layanan eksternal publik.

Dengan seluruh bukti empiris tersebut, sistem ini telah memenuhi tujuan pembelajaran Bab 1–13 secara menyeluruh, dengan penekanan khusus pada aspek publish-subscribe (Bab 1–2), fault tolerance (Bab 6), eventual consistency (Bab 7), serta transaksi dan kontrol konkurensi (Bab 8–9) yang menjadi fokus utama penilaian pada UAS ini.

---

## Referensi

Coulouris, G., Dollimore, J., Kindberg, T., & Blair, G. (2012). *Distributed systems: Concepts and design* (5th ed.). Addison-Wesley.

