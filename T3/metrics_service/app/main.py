import os
import time
import json
import logging
import threading
from collections import defaultdict, deque
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

import numpy as np
import httpx
from fastapi import FastAPI
from pydantic import BaseModel, Field
from confluent_kafka import Consumer, TopicPartition


#  ============= configuracion global ============= #
logging.basicConfig(level=logging.INFO, format="%(asctime)s [metrics] %(message)s")
log = logging.getLogger(__name__)


LATENCY_WINDOW = int(os.getenv("LATENCY_WINDOW", "5000"))
SNAPSHOT_DIR = Path(os.getenv("SNAPSHOT_DIR", "/snapshots"))
CACHE_STATS_URL = os.getenv("CACHE_STATS_URL", "http://cache_service:8001/stats")

# Kafka (backlog y recovery time)
KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "cache-consumers")
TOPIC_MAIN = os.getenv("TOPIC_MAIN", "consultas_principales")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "consultas_reintentos")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "consultas_dlq")
BACKLOG_TOPICS = [TOPIC_MAIN, TOPIC_RETRY]


#  ============= clase principal de recoleccion de metricas ============= #
class Metrics:

#  ============= inicializacion y reseteo de contadores ============= #
    def __init__(self):
        self.lock = threading.Lock()
        self.start_time = time.time()
        self._reset_counters()

    def _reset_counters(self):
        self.hits_total = 0
        self.misses_total = 0
        self.errors_total = 0

        # por tipo de consulta
        self.hits_by_q: dict[str, int] = defaultdict(int)
        self.misses_by_q: dict[str, int] = defaultdict(int)

        # latencias por evento
        self.latencies_hit: deque[float] = deque(maxlen=LATENCY_WINDOW)
        self.latencies_miss: deque[float] = deque(maxlen=LATENCY_WINDOW)
        self.latencies_by_q: dict[str, deque] = defaultdict(lambda: deque(maxlen=LATENCY_WINDOW))

        # throughputs
        self.event_times: deque[float] = deque(maxlen=20000)

        # eviction rate
        self.last_eviction_snapshot: int = 0
        self.last_eviction_time: float = self.start_time

        # KPIs asíncronos
        self.processed_total = 0
        self.recovered_total = 0   
        self.retried_total = 0   
        self.dlq_total = 0       
        self.latencies_e2e: deque[float] = deque(maxlen=LATENCY_WINDOW)
        self.processed_times: deque[float] = deque(maxlen=20000)

    def reset(self):
        with self.lock:
            self.start_time = time.time()
            self._reset_counters()

    
#  ============= registro de eventos de cache ============= #
    def record(self, event: dict):
        ev = event.get("event")
        qt = (event.get("query_type") or "UNK").upper()
        latency = float(event.get("latency_ms") or 0)
        ts = float(event.get("ts") or time.time())

        with self.lock:
            if ev == "hit":
                self.hits_total += 1
                self.hits_by_q[qt] += 1
                self.latencies_hit.append(latency)
                self.latencies_by_q[qt].append(latency)
                self.event_times.append(ts)
            elif ev == "miss":
                self.misses_total += 1
                self.misses_by_q[qt] += 1
                self.latencies_miss.append(latency)
                self.latencies_by_q[qt].append(latency)
                self.event_times.append(ts)
            elif ev == "error":
                self.errors_total += 1
            # eventos asincronos emitidos por el consumer
            elif ev == "processed":
                self.processed_total += 1
                self.processed_times.append(ts)
                e2e = event.get("end_to_end_ms")
                if e2e is not None:
                    self.latencies_e2e.append(float(e2e))
                if event.get("from_retry"):
                    self.recovered_total += 1
            elif ev == "retry":
                self.retried_total += 1
            elif ev == "recovered":
                self.recovered_total += 1
            elif ev == "dlq":
                self.dlq_total += 1

    
#  ============= calculo de resumen global de metricas ============= #
    def summary(self, cache_stats: dict | None = None) -> dict:
        with self.lock:
            total = self.hits_total + self.misses_total
            elapsed = time.time() - self.start_time

            hit_rate = (self.hits_total / total) if total > 0 else None
            miss_rate = (1 - hit_rate) if hit_rate is not None else None

            # throughput de queries exitosas
            throughput = total / elapsed if elapsed > 0 else 0

            # throughput ultimos 10 seg
            now = time.time()
            recent = [t for t in self.event_times if t >= now - 10]
            throughput_recent = len(recent) / 10.0 if recent else 0

            # latencias
            def percentiles(arr: deque, ps=(50, 95, 99)):
                if not arr:
                    return {f"p{p}": None for p in ps}
                a = np.array(arr)
                return {f"p{p}": round(float(np.percentile(a, p)), 3) for p in ps}

            lat_hit = percentiles(self.latencies_hit)
            lat_miss = percentiles(self.latencies_miss)
            lat_all = percentiles(
                deque(list(self.latencies_hit) + list(self.latencies_miss))
            )

            mean_t_cache = float(np.mean(self.latencies_hit)) if self.latencies_hit else 0
            mean_t_db = float(np.mean(self.latencies_miss)) if self.latencies_miss else 0
            
            efficiency = None
            if total > 0 and mean_t_db > 0:
                
                saved = self.hits_total * (mean_t_db - mean_t_cache)
                efficiency = round(saved / total, 3)

            # eviction rate
            eviction_rate_per_min = None
            current_evicted = None
            if cache_stats:
                current_evicted = int(cache_stats.get("evicted_keys", 0))
                dt = now - self.last_eviction_time
                if dt > 0:
                    delta = current_evicted - self.last_eviction_snapshot
                    eviction_rate_per_min = round(delta * 60.0 / dt, 2)

            # KPIs asincronos
            lat_e2e = percentiles(self.latencies_e2e)
            proc_recent = [t for t in self.processed_times if t >= now - 10]
            processed_throughput = self.processed_total / elapsed if elapsed > 0 else 0
            processed_throughput_recent = len(proc_recent) / 10.0 if proc_recent else 0
            terminal = self.processed_total + self.dlq_total
            retry_rate = round(self.retried_total / terminal, 4) if terminal else None
            dlq_rate = round(self.dlq_total / terminal, 4) if terminal else None
            failed_once = self.recovered_total + self.dlq_total
            recovery_rate = (
                round(self.recovered_total / failed_once, 4) if failed_once else None
            )

            async_kpis = {
                "processed_total": self.processed_total,
                "recovered_total": self.recovered_total,
                "retried_total": self.retried_total,
                "dlq_total": self.dlq_total,
                "throughput_processed_qps_total": round(processed_throughput, 2),
                "throughput_processed_qps_recent_10s": round(processed_throughput_recent, 2),
                "latency_e2e_ms": lat_e2e,
                "retry_rate": retry_rate,
                "recovery_rate": recovery_rate,
                "dlq_rate": dlq_rate,
            }

            return {
                "elapsed_sec": round(elapsed, 2),
                "totals": {
                    "hits": self.hits_total,
                    "misses": self.misses_total,
                    "errors": self.errors_total,
                    "total_requests": total,
                },
                "hit_rate": round(hit_rate, 4) if hit_rate is not None else None,
                "miss_rate": round(miss_rate, 4) if miss_rate is not None else None,
                "throughput_qps_total": round(throughput, 2),
                "throughput_qps_recent_10s": round(throughput_recent, 2),
                "latency_ms_hit": lat_hit,
                "latency_ms_miss": lat_miss,
                "latency_ms_all": lat_all,
                "mean_t_cache_ms": round(mean_t_cache, 3),
                "mean_t_db_ms": round(mean_t_db, 3),
                "cache_efficiency": efficiency,
                "eviction": {
                    "total_evicted": current_evicted,
                    "rate_per_min": eviction_rate_per_min,
                },
                "async_kpis": async_kpis,
                "cache_redis_stats": cache_stats,
            }


    #  ============= clculo de evicciones ============= #
    def update_eviction_marker(self, current_evicted: int):
        with self.lock:
            self.last_eviction_snapshot = current_evicted
            self.last_eviction_time = time.time()


#  ============= resumen de metricas por tipo de query ============= #
    def by_query_summary(self) -> dict:
        with self.lock:
            out = {}
            for qt in ("Q1", "Q2", "Q3", "Q4", "Q5"):
                h = self.hits_by_q.get(qt, 0)
                m = self.misses_by_q.get(qt, 0)
                tot = h + m
                lats = self.latencies_by_q.get(qt, deque())
                if lats:
                    a = np.array(lats)
                    p50 = float(np.percentile(a, 50))
                    p95 = float(np.percentile(a, 95))
                    p99 = float(np.percentile(a, 99))
                else:
                    p50 = p95 = p99 = None
                out[qt] = {
                    "hits": h, "misses": m, "total": tot,
                    "hit_rate": round(h / tot, 4) if tot else None,
                    "p50_ms": round(p50, 3) if p50 is not None else None,
                    "p95_ms": round(p95, 3) if p95 is not None else None,
                    "p99_ms": round(p99, 3) if p99 is not None else None,
                }
            return out


#  ============= monitor de backlog Kafka ============= #
class KafkaMonitor:

    def __init__(self):
        self.lock = threading.Lock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._consumer: Consumer | None = None
        self.reset()

    def reset(self):
        with self.lock:
            self.backlog: dict[str, int] = {}
            self.total_backlog: int | None = None
            self.peak_backlog = 0
            self.peak_time: float | None = None
            self.recovery_time_sec: float | None = None
            self._draining = False
            self.last_update: float | None = None

    def start(self):
        self._consumer = Consumer({
            "bootstrap.servers": KAFKA_BOOTSTRAP,
            "group.id": CONSUMER_GROUP,
            "enable.auto.commit": False,
        })
        self._thread = threading.Thread(target=self._loop, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        if self._consumer:
            try:
                self._consumer.close()
            except Exception:
                pass

    def _measure(self) -> dict[str, int]:
        out: dict[str, int] = {}
        md = self._consumer.list_topics(timeout=10)
        for topic in BACKLOG_TOPICS:
            t = md.topics.get(topic)
            if t is None or t.error is not None:
                continue
            tps = [TopicPartition(topic, p) for p in t.partitions.keys()]
            if not tps:
                continue
            committed = self._consumer.committed(tps, timeout=10)
            lag = 0
            for tp in committed:
                lo, hi = self._consumer.get_watermark_offsets(tp, timeout=10, cached=False)
                offset = tp.offset if (tp.offset is not None and tp.offset >= 0) else 0
                lag += max(hi - offset, 0)
            out[topic] = lag
        return out

    def _loop(self):
        while not self._stop.is_set():
            try:
                per_topic = self._measure()
                total = sum(per_topic.values())
                now = time.time()
                with self.lock:
                    self.backlog = per_topic
                    self.total_backlog = total
                    self.last_update = now
                    if total > self.peak_backlog:
                        self.peak_backlog = total
                        self.peak_time = now
                        self._draining = True
                    if self._draining and total == 0 and self.peak_backlog > 0:
                        self.recovery_time_sec = round(now - self.peak_time, 2)
                        self._draining = False
            except Exception as e:
                log.debug(f"backlog poll error: {e}")
            self._stop.wait(2.0)

    def snapshot(self) -> dict:
        with self.lock:
            return {
                "backlog_by_topic": dict(self.backlog),
                "backlog_total": self.total_backlog,
                "peak_backlog": self.peak_backlog,
                "recovery_time_sec": self.recovery_time_sec,
                "draining": self._draining,
            }


metrics = Metrics()
monitor = KafkaMonitor()
http: httpx.AsyncClient | None = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    SNAPSHOT_DIR.mkdir(parents=True, exist_ok=True)
    http = httpx.AsyncClient(timeout=5.0)
    try:
        monitor.start()
    except Exception as e:
        log.warning(f"No se pudo iniciar KafkaMonitor: {e}")
    log.info("Metrics Service listo")
    yield
    monitor.stop()
    await http.aclose()


#  ============= inicializacion de la api ============= #
app = FastAPI(title="Metrics Service", lifespan=lifespan)


class Event(BaseModel):
    event: str
    query_type: str | None = None
    key: str | None = None
    latency_ms: float | None = None
    lookup_ms: float | None = None
    compute_ms: float | None = None
    ttl: int | None = None
    error: str | None = None
    ts: float | None = None
    end_to_end_ms: float | None = None
    from_retry: bool | None = None
    cache: str | None = None


#  ============= endpoints ============= #
@app.get("/health")
async def health():
    return {"status": "ok"}


@app.post("/event")
async def event(ev: Event):
    metrics.record(ev.dict())
    return {"ok": True}

#  ============= estadisticas del servicio de cache ============= #
async def _fetch_cache_stats() -> dict | None:
    if http is None:
        return None
    try:
        r = await http.get(CACHE_STATS_URL, timeout=2.0)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        log.debug(f"No pude obtener stats del cache: {e}")
        return None


@app.get("/summary")
async def summary():
    cache_stats = await _fetch_cache_stats()
    s = metrics.summary(cache_stats)
    s["queue_health"] = monitor.snapshot()
    if cache_stats:
        metrics.update_eviction_marker(int(cache_stats.get("evicted_keys", 0)))
    return s


@app.get("/summary/by_query")
async def by_query():
    return metrics.by_query_summary()


@app.get("/backlog")
async def backlog():
    return monitor.snapshot()


@app.post("/reset")
async def reset():
    metrics.reset()
    monitor.reset()
    log.info("Métricas reiniciadas")
    return {"status": "reset"}


class SnapshotRequest(BaseModel):
    label: str = Field("snapshot", description="Nombre descriptivo del experimento")
    extra: dict[str, Any] = Field(default_factory=dict)


@app.post("/snapshot")
async def snapshot(req: SnapshotRequest):
    cache_stats = await _fetch_cache_stats()
    summary_data = metrics.summary(cache_stats)
    summary_data["queue_health"] = monitor.snapshot()
    summary_data["by_query"] = metrics.by_query_summary()
    summary_data["label"] = req.label
    summary_data["extra"] = req.extra
    summary_data["snapshot_ts"] = time.time()

    safe_label = req.label.replace("/", "_").replace(" ", "_")
    fname = f"{int(time.time())}_{safe_label}.json"
    path = SNAPSHOT_DIR / fname
    with open(path, "w") as f:
        json.dump(summary_data, f, indent=2, default=str)
    log.info(f"Snapshot guardado: {path}")
    return {"path": str(path), "summary": summary_data}
