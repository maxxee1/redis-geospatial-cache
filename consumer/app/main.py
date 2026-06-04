#  ============= dependencias del proyecto ============= #
import os
import json
import time
import logging
import threading
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI
from confluent_kafka import Consumer, Producer, KafkaException
from confluent_kafka.admin import AdminClient, NewTopic

#  ============= configuracion global ============= #
logging.basicConfig(level=logging.INFO, format="%(asctime)s [consumer] %(message)s")
log = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
CACHE_URL = os.getenv("CACHE_URL", "http://cache_service:8001")
METRICS_URL = os.getenv("METRICS_URL", "http://metrics:8003")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "cache-consumers")
MAX_RETRIES = int(os.getenv("MAX_RETRIES", "3"))
RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "1000"))

TOPIC_MAIN = os.getenv("TOPIC_MAIN", "consultas_principales")
TOPIC_RETRY = os.getenv("TOPIC_RETRY", "consultas_reintentos")
TOPIC_DLQ = os.getenv("TOPIC_DLQ", "consultas_dlq")
PARTITIONS_MAIN = int(os.getenv("PARTITIONS_MAIN", "6"))


#  ============= estado local del worker ============= #
class WorkerState:
    def __init__(self):
        self.running = False
        self.processed = 0
        self.retried = 0
        self.recovered = 0
        self.to_dlq = 0
        self.errors = 0


wstate = WorkerState()
_stop = threading.Event()
_thread: threading.Thread | None = None


#  ============= creacion de topicos ============= #
def ensure_topics():
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP})
    new_topics = [
        NewTopic(TOPIC_MAIN, num_partitions=PARTITIONS_MAIN, replication_factor=1),
        NewTopic(TOPIC_RETRY, num_partitions=3, replication_factor=1),
        NewTopic(TOPIC_DLQ, num_partitions=1, replication_factor=1),
    ]
    futures = admin.create_topics(new_topics)
    for topic, fut in futures.items():
        try:
            fut.result()
            log.info(f"Tópico creado: {topic}")
        except Exception as e:
            log.info(f"Tópico {topic}: {e}")


#  ============= envio de eventos al servicio de metricas ============= #
def _send_metric(client: httpx.Client, event: dict):
    try:
        client.post(f"{METRICS_URL}/event", json=event, timeout=2.0)
    except Exception as e:
        log.debug(f"Metrics post failed: {e}")


#  ============= produccion a topicos de reintento / DLQ ============= #
def _route(producer: Producer, topic: str, message: dict, key: str):
    producer.produce(topic, key=key, value=json.dumps(message))
    producer.poll(0)


#  ============= procesamiento de un mensaje ============= #
def _process(msg_value: str, http: httpx.Client, producer: Producer):
    """Procesa una consulta: consulta caché (vía cache_service) y enruta
    fallas del Generador de Respuestas a reintentos/DLQ."""
    try:
        message = json.loads(msg_value)
    except json.JSONDecodeError:
        log.error("Mensaje no es JSON válido; descartando")
        wstate.errors += 1
        return

    query_type = message.get("query_type")
    params = message.get("params", {})
    retry_count = int(message.get("retry_count", 0))
    created_at = float(message.get("created_at", time.time()))
    key = str(params.get("zone_id") or params.get("zone_a") or "Z0")

    try:
        resp = http.post(
            f"{CACHE_URL}/query",
            json={"query_type": query_type, "params": params},
            timeout=15.0,
        )
    except httpx.HTTPError as e:
        resp = None
        log.debug(f"cache_service inalcanzable: {e}")

    success = resp is not None and resp.status_code == 200

    if success:
        end_to_end_ms = (time.time() - created_at) * 1000.0
        from_retry = retry_count > 0
        cache_state = None
        try:
            cache_state = resp.json().get("cache")
        except Exception:
            pass
        wstate.processed += 1
        if from_retry:
            wstate.recovered += 1
        _send_metric(http, {
            "event": "processed",
            "query_type": query_type,
            "from_retry": from_retry,
            "cache": cache_state,
            "end_to_end_ms": end_to_end_ms,
            "ts": time.time(),
        })
        return

    # ===== Falla en la politica de reintentos ===== #
    new_count = retry_count + 1
    message["retry_count"] = new_count
    wstate.retried += 1
    _send_metric(http, {"event": "retry", "query_type": query_type, "ts": time.time()})

    if new_count >= MAX_RETRIES:
        _route(producer, TOPIC_DLQ, message, key)
        wstate.to_dlq += 1
        _send_metric(http, {"event": "dlq", "query_type": query_type, "ts": time.time()})
        log.info(f"Mensaje {message.get('uuid')} -> DLQ (retry_count={new_count})")
    else:
        # Backoff antes de reintentar
        if RETRY_DELAY_MS > 0:
            time.sleep(RETRY_DELAY_MS / 1000.0)
        _route(producer, TOPIC_RETRY, message, key)


#  ============= loop de consumo ============= #
def _consume_loop():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BOOTSTRAP,
        "group.id": CONSUMER_GROUP,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
        "max.poll.interval.ms": 300000,
    })
    producer = Producer({"bootstrap.servers": KAFKA_BOOTSTRAP, "linger.ms": 5})
    http = httpx.Client()

  
    consumer.subscribe([TOPIC_MAIN, TOPIC_RETRY])
    log.info(f"Suscrito a {TOPIC_MAIN}, {TOPIC_RETRY} como grupo '{CONSUMER_GROUP}'")
    wstate.running = True

    try:
        while not _stop.is_set():
            msg = consumer.poll(1.0)
            if msg is None:
                continue
            if msg.error():
                log.warning(f"Error de Kafka: {msg.error()}")
                continue
            try:
                _process(msg.value().decode("utf-8"), http, producer)
            except Exception as e:
                wstate.errors += 1
                log.exception(f"Error procesando mensaje: {e}")
            finally:
                consumer.commit(msg, asynchronous=False)
    finally:
        wstate.running = False
        producer.flush(10)
        consumer.close()
        http.close()
        log.info("Loop de consumo detenido")


#  ============= ciclo de vida de la app ============= #
@asynccontextmanager
async def lifespan(app: FastAPI):
    global _thread
    for attempt in range(30):
        try:
            ensure_topics()
            break
        except KafkaException as e:
            log.info(f"Kafka aún no listo ({e}); reintentando...")
            time.sleep(2)
    _stop.clear()
    _thread = threading.Thread(target=_consume_loop, daemon=True)
    _thread.start()
    log.info("Consumer Service listo")
    yield
    _stop.set()
    if _thread:
        _thread.join(timeout=15)


#  ============= aplicacion fastapi ============= #
app = FastAPI(title="Consumer Service", lifespan=lifespan)


@app.get("/health")
async def health():
    return {
        "status": "ok",
        "running": wstate.running,
        "group": CONSUMER_GROUP,
        "processed": wstate.processed,
        "retried": wstate.retried,
        "recovered": wstate.recovered,
        "to_dlq": wstate.to_dlq,
        "errors": wstate.errors,
    }
