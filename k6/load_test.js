import http from "k6/http";
import { check, sleep } from "k6";
import { uuidv4 } from "https://jslib.k6.io/k6-utils/1.4.0/index.js";
import { Counter, Rate, Trend } from "k6/metrics";

// ── Konfigurasi ──────────────────────────────────────────────────────────────
export const options = {
  stages: [
    { duration: "30s", target: 20  },   // ramp-up
    { duration: "60s", target: 50  },   // sustained load
    { duration: "60s", target: 100 },   // peak load
    { duration: "30s", target: 0   },   // ramp-down
  ],
  thresholds: {
    http_req_duration: ["p(95)<1000"],  // 95% request < 1 detik
    http_req_failed:   ["rate<0.01"],   // error rate < 1%
  },
};

// ── Metrics kustom ───────────────────────────────────────────────────────────
const duplicatesCounter = new Counter("duplicate_events_sent");
const publishTrend      = new Trend("publish_latency_ms");

// ── Konstanta ────────────────────────────────────────────────────────────────
const BASE_URL = __ENV.BASE_URL || "http://localhost:8080";
const TOPICS   = [
  "user.login",
  "order.created",
  "payment.processed",
  "inventory.updated",
  "system.alert",
];
const DUPLICATE_RATE = 0.35;

// Pool ID untuk duplikasi (dibuat sekali per VU)
const POOL_SIZE = 50;
let pool = [];

export function setup() {
  // Pastikan aggregator healthy
  const r = http.get(`${BASE_URL}/health`);
  check(r, { "aggregator healthy": (res) => res.status === 200 });
}

export default function () {
  // Inisialisasi pool per VU
  if (pool.length === 0) {
    for (let i = 0; i < POOL_SIZE; i++) {
      pool.push(uuidv4());
    }
  }

  const isDuplicate = Math.random() < DUPLICATE_RATE;
  let eventId;

  if (isDuplicate && pool.length > 0) {
    eventId = pool[Math.floor(Math.random() * pool.length)];
    duplicatesCounter.add(1);
  } else {
    eventId = uuidv4();
    pool.push(eventId);
    if (pool.length > POOL_SIZE * 2) pool.shift();
  }

  const event = {
    topic:     TOPICS[Math.floor(Math.random() * TOPICS.length)],
    event_id:  eventId,
    timestamp: new Date().toISOString(),
    source:    "k6-load-test",
    payload: {
      vu:        __VU,
      iteration: __ITER,
      value:     Math.floor(Math.random() * 10000),
    },
  };

  const start = Date.now();
  const res   = http.post(
    `${BASE_URL}/publish`,
    JSON.stringify(event),
    { headers: { "Content-Type": "application/json" } }
  );
  publishTrend.add(Date.now() - start);

  check(res, {
    "status 200":   (r) => r.status === 200,
    "accepted >= 1": (r) => {
      try { return r.json("accepted") >= 1; } catch { return false; }
    },
  });

  sleep(0.01);
}

export function teardown() {
  // Ambil stats akhir
  const r = http.get(`${BASE_URL}/stats`);
  if (r.status === 200) {
    const s = r.json();
    console.log(
      `\n=== Final Stats ===\n` +
      `received:          ${s.received}\n` +
      `unique_processed:  ${s.unique_processed}\n` +
      `duplicate_dropped: ${s.duplicate_dropped}\n` +
      `topics:            ${s.topics.join(", ")}\n`
    );
  }
}
