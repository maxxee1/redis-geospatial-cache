# Tarea 2 — Arquitectura asíncrona con Kafka


![Python](https://img.shields.io/badge/Python-3.12-3776AB?logo=python&logoColor=white)
![FastAPI](https://img.shields.io/badge/FastAPI-0.111-009688?logo=fastapi&logoColor=white)
![Kafka](https://img.shields.io/badge/Apache%20Kafka-3.9%20KRaft-231F20?logo=apachekafka&logoColor=white)
![Redis](https://img.shields.io/badge/Redis-7.4-DC382D?logo=redis&logoColor=white)
![Docker Compose](https://img.shields.io/badge/Docker%20Compose-v2-2496ED?logo=docker&logoColor=white)

**Curso:** Sistemas Distribuidos — Universidad Diego Portales, 2026-1  
**Autores:** Gabriel González · Maximiliano Solorza

---

## 1. Contexto

Un **Generador de Tráfico** produce consultas y las publica en un *topic* de Kafka (`consultas_principales`) en lugar de llamarlas por HTTP. Un **grupo de consumidores** (escalable a N réplicas) lee esos mensajes en paralelo y, por cada uno, consulta la **caché**:

- **Cache HIT** → responde de inmediato desde Redis.
- **Cache MISS** → delega al **Generador de Respuestas**, que computa la consulta, la guarda en caché y responde.

Si el Generador de Respuestas está **caído**, el consumidor no pierde el mensaje: lo reencola en `consultas_reintentos` (con *backoff*) y, si agota los reintentos, lo envía a `consultas_dlq` (Dead Letter Queue). Un servicio de **métricas** observa todo el sistema, incluyendo el *backlog* y el *recovery time*.

**La gran ventaja frente al modelo síncrono:** el productor **nunca se bloquea**. Una ráfaga de tráfico o una caída del backend se absorben como *backlog* en la cola, no como errores propagados al cliente.

---

## 2. Arquitectura

```
                         ┌──────────────────────────────────────────────┐
                         │                  Apache Kafka                 │
                         │   consultas_principales  (6 particiones)      │
   traffic_generator ───►│   consultas_reintentos   (3 particiones)      │
   (productor, :8000)    │   consultas_dlq          (1 partición)        │
                         └───────────────┬──────────────────────────────┘
                                         │  (poll)
                              ┌──────────▼───────────┐
                              │  consumer  (grupo     │   ← escalable: --scale consumer=N
                              │  "cache-consumers")   │
                              │  N réplicas, :8004    │
                              └──────────┬───────────┘
                                         │  POST /query
                              ┌──────────▼───────────┐     miss      ┌─────────────────────┐
                              │   cache_service       │─────────────►│ response_generator  │
                              │   (proxy Redis, :8001)│◄─────────────│ (Q1–Q5, :8002)      │
                              └──────────┬───────────┘   resultado   │ + /admin/fail       │
                                         │                           └─────────────────────┘
                                    ┌────▼────┐
                                    │  redis  │  (LRU / 200 MB, :6379)
                                    └─────────┘

   Todos los servicios emiten eventos ──►  metrics (:8003)  ──► async_kpis + queue_health
   metrics además sondea el "lag" del grupo de consumidores para medir el backlog.
```

### Servicios

| Servicio | Puerto | Rol | Archivo clave |
|---|---|---|---|
| `kafka` | 9092 | Broker en modo **KRaft** (sin Zookeeper), nodo único. Hospeda los 3 *topics*. | `docker-compose.yml` |
| `traffic_generator` | 8000 | **Productor.** Genera Q1–Q5 con distribuciones Zipf/Uniforme y las publica a Kafka (o por HTTP en modo `sync`). | `traffic_generator/app/main.py` |
| `consumer` | 8004 *(interno)* | **Grupo de consumidores.** Puente Kafka↔HTTP: lee mensajes, consulta la caché y enruta fallas a reintentos/DLQ. **Escalable.** | `consumer/app/main.py` |
| `cache_service` | 8001 | Proxy de Redis (LRU/LFU/FIFO). Construye la *cache key*, aplica TTL. Reutilizado sin cambios de la Tarea 1. | `cache_service/app/main.py`, `cache.py` |
| `response_generator` | 8002 | Backend: computa Q1–Q5 sobre el dataset en memoria. Incluye endpoint para **simular caídas**. | `response_generator/app/main.py`, `queries.py` |
| `metrics` | 8003 | Recolector de métricas: KPIs síncronos + `async_kpis` + `queue_health` (backlog/recovery vía *lag* de Kafka). | `metrics_service/app/main.py` |
| `redis` | 6379 | Almacén del caché. Fijo en **LRU / 200 MB** para esta tarea. | imagen `redis:7.4` |

### Los tres *topics* de Kafka

| Topic | Particiones | Contenido |
|---|---|---|
| `consultas_principales` | 6 | Tráfico normal. Las 6 particiones permiten repartir la carga entre hasta 6 consumidores. |
| `consultas_reintentos` | 3 | Mensajes que fallaron y serán reprocesados. |
| `consultas_dlq` | 1 | Mensajes expulsados tras agotar los reintentos (terminal, para inspección). |



### Esquema del mensaje (payload enriquecido)

Cada mensaje publicado lleva **obligatoriamente** metadatos de trazabilidad:

```json
{
  "uuid": "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  "query_type": "Q1",
  "params": { "zone_id": "Z1", "confidence_min": 0.8 },
  "retry_count": 0,
  "created_at": 1717459200.123
}
```

| Campo | Uso |
|---|---|
| `uuid` | Traza el mensaje a través de los tres *topics*. |
| `retry_count` | Dirige la política de reintentos (empieza en 0). |
| `created_at` | Permite calcular la **latencia extremo a extremo** (incluida la espera en cola) cuando el mensaje finalmente se procesa. |

Construido por `_enrich()` en `traffic_generator/app/main.py`.

---

## 3. Ciclo de vida de un mensaje


1. **Producción.** El `traffic_generator` genera una consulta (p. ej. Q1 sobre la zona Z1) según una distribución Zipf o Uniforme, la enriquece con `uuid/retry_count/created_at` y la **publica** en `consultas_principales` (clave de partición = la zona). No espera respuesta: sigue produciendo.

2. **Consumo.** Una réplica del `consumer` (todas comparten el `group.id = cache-consumers`) recibe el mensaje. Kafka garantiza que cada partición la lee **un solo** consumidor del grupo, así que al escalar réplicas la carga se reparte automáticamente.

3. **Consulta a la caché.** El consumidor hace `POST cache_service/query` con `{query_type, params}`. El `cache_service` construye la *cache key* y consulta Redis:
   - **HIT** → devuelve `200` con el resultado cacheado.
   - **MISS** → llama al `response_generator`, guarda el resultado en Redis con TTL y devuelve `200`.
   - **Generador caído** → el `cache_service` devuelve **`HTTP 502`**, señal que dispara los reintentos.

4. **Caso de éxito (200).** El consumidor calcula la latencia extremo a extremo (`now − created_at`) y emite a `metrics` un evento **`processed`**. Hace *commit* del *offset* y termina.

5. **Caso de falla (502).** El consumidor aplica la política de reintentos:
   - Incrementa `retry_count` en +1 y emite un evento **`retry`**.
   - Si `retry_count < MAX_RETRIES` (=3): espera el *backoff* de `RETRY_DELAY_MS` (1 s) y re-publica en `consultas_reintentos`.
   - Si `retry_count ≥ MAX_RETRIES`: publica en `consultas_dlq` y emite un evento **`dlq`**.
   - En ambos casos hace *commit* del *offset* original (semántica *at-least-once*).

6. **Reprocesamiento.** El mismo grupo de consumidores está suscrito a `consultas_reintentos`. Al retomar el mensaje, si el backend ya se recuperó, el mensaje tiene éxito y se cuenta como **`recovered`**. Si vuelve a fallar y agota los reintentos, va a la DLQ.

**Ejemplo con `MAX_RETRIES=3`:** un mensaje cuyo backend está caído falla con `retry_count` 0→1→2 (tres intentos, cada uno seguido de 1 s de *backoff*); al alcanzar 3 se expulsa a la DLQ. Si el backend se recupera dentro de esa ventana (~3 s), el reintento tiene éxito y el mensaje se recupera en lugar de perderse.

---

## 4. Estructura del código

```
.
├── docker-compose.yml          # Define los 7 servicios, los topics y el wiring de red
├── .env                        # Toda la configuración (ver §6)
│
├── data/
│   └── santiago_buildings.parquet   # Dataset (≈8.5 MB, 319k edificios, 5 zonas)
│
├── traffic_generator/          # ── PRODUCTOR ──
│   └── app/
│       ├── main.py             #   FastAPI. RunRequest(mode, rate, spike...).
│       │                       #   _enrich() = payload; _produce() = publica a Kafka;
│       │                       #   _run_experiment() = bucle Poisson + soporte spike;
│       │                       #   modo "sync" conserva la ruta HTTP original.
│       └── distributions.py    #   ZipfSelector, UniformSelector, PoissonInterArrival
│
├── consumer/                   # ── GRUPO DE CONSUMIDORES (NUEVO en Tarea 2) ──
│   └── app/main.py             #   ensure_topics() crea los 3 topics (idempotente);
│                               #   _consume_loop() = poll en hilo de fondo;
│                               #   _process() = consulta caché + reintentos/DLQ;
│                               #   /health expone contadores (processed/retried/...).
│
├── cache_service/              # ── CACHÉ (sin cambios respecto de Tarea 1) ──
│   └── app/
│       ├── main.py             #   POST /query: HIT/MISS, llama al backend, emite hit/miss.
│       │                       #   _build_cache_key() define el formato de las claves.
│       └── cache.py            #   CacheClient: get/set en Redis, TTL, FIFO en cliente.
│
├── response_generator/         # ── BACKEND DE CÓMPUTO ──
│   └── app/
│       ├── main.py             #   POST /query computa Q1–Q5.
│       │                       #   /admin/fail (NUEVO): simula caída → 503.
│       ├── queries.py          #   q1_count, q2_area, q3_density, q4_compare, q5_dist.
│       └── data_loader.py      #   Carga el parquet, particiona por zona, ZONES.
│
├── metrics_service/            # ── MÉTRICAS ──
│   └── app/main.py             #   Metrics: hit/miss + (NUEVO) processed/retry/dlq.
│                               #   KafkaMonitor (NUEVO): sondea el lag del grupo
│                               #   para backlog + recovery_time. /summary, /backlog.
│
├── experiments/
│   ├── master_run.py           #   Batería de la Tarea 1 (políticas × tamaños).
│   ├── run_async.py            #   (NUEVO) Los 5 escenarios del DoD de la Tarea 2.
│   ├── build_figures.py        #   Figuras de la Tarea 1.
│   └── build_figures_async.py  #   (NUEVO) Figuras de la Tarea 2 desde los snapshots.
│
├── informe/
│   ├── informe_tarea2.tex      #   Informe LaTeX (compila a 10 páginas).
│   └── figs/                   #   Figuras generadas (PDF + PNG).
│
└── results/                    #   Snapshots JSON de cada experimento (snap_*.json).
```


## 6. Configuración `.env`

```ini
# ── Caché (congelada para Tarea 2) ──
REDIS_MAXMEMORY=200mb
REDIS_POLICY_NATIVE=allkeys-lru
CACHE_POLICY=LRU             # debe ser consistente con REDIS_POLICY_NATIVE
REDIS_PORT_HOST=6379
CACHE_TTL_SEC=300            # TTL por defecto; TTL_Q1..Q5 lo afinan por consulta
TTL_Q1=300  TTL_Q2=300  TTL_Q3=180  TTL_Q4=120  TTL_Q5=600

# ── Backend ──
SIM_LATENCY_MIN_MS=30        # latencia simulada del cómputo geoespacial (30–120 ms)
SIM_LATENCY_MAX_MS=120

# ── Kafka / arquitectura asíncrona (Tarea 2) ──
KAFKA_PORT_HOST=9092
MAX_RETRIES=3                # fallas antes de expulsar un mensaje a la DLQ
RETRY_DELAY_MS=1000          # backoff antes de re-encolar (habilita el recovery)
PARTITIONS_MAIN=6            # particiones de consultas_principales (>= nº de réplicas)
```

---

## 7. Ejecutar todo

### Levantar el *stack*

```bash
docker compose up -d --build
```

Esperar ~30–60 s a que pasen los *healthchecks* y verifica:

```bash
curl http://localhost:8000/health   # traffic_generator
curl http://localhost:8001/health   # cache_service
curl http://localhost:8002/health   # response_generator
curl http://localhost:8003/health   # metrics
# el consumer expone /health solo internamente en :8004
```

### Escalar el grupo de consumidores

```bash
docker compose up -d --scale consumer=3
```

Las 6 particiones de `consultas_principales` se reparten entre las réplicas. Para verificar la asignación y el *lag*:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group cache-consumers
```

### Lanzar tráfico manualmente

```bash
# Modo Kafka (asíncrono)
curl -X POST http://localhost:8000/run \
  -H 'Content-Type: application/json' \
  -d '{"mode":"kafka","rate_qps":40,"duration_sec":30,"label":"prueba"}'

# Modo síncrono (Sistema Base, sin Kafka)
curl -X POST http://localhost:8000/run \
  -H 'Content-Type: application/json' \
  -d '{"mode":"sync","rate_qps":40,"n_queries":300,"label":"base"}'

# Spike: a los 20 s la tasa salta de 40 a 250 q/s
curl -X POST http://localhost:8000/run \
  -H 'Content-Type: application/json' \
  -d '{"mode":"kafka","rate_qps":40,"duration_sec":50,"spike_at_sec":20,"spike_rate_qps":250,"label":"spike"}'
```

### Simular una caída del backend

```bash
# Activar la falla (todas las consultas devuelven 503)
curl -X POST http://localhost:8002/admin/fail \
  -H 'Content-Type: application/json' \
  -d '{"enabled":true,"prob":1.0}'

# Restaurar
curl -X POST http://localhost:8002/admin/fail \
  -H 'Content-Type: application/json' \
  -d '{"enabled":false}'

# Alternativa: detener y reiniciar el contenedor directamente
docker compose stop response_generator
docker compose start response_generator
```

### Batería completa de experimentos

```bash
# Todos los escenarios del DoD
python experiments/run_async.py --scenario all

# Escenarios individuales: base_sync | kafka_1 | kafka_3 | failure | spike
python experiments/run_async.py --scenario failure

# Generar figuras
python experiments/build_figures_async.py   # → informe/figs/
```

### Apagar y limpiar

```bash
docker compose down -v
```

---

## 10. Referencia de la API HTTP

### `traffic_generator` — `:8000`

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/run` | Inicia un experimento. Body: `mode` (`sync`\|`kafka`), `distribution`, `rate_qps`, `duration_sec` \| `n_queries`, `zipf_s`, `spike_at_sec`, `spike_rate_qps`, `label`. |
| `GET` | `/status` | Progreso del experimento en curso. |
| `POST` | `/stop` | Detiene el experimento. |
| `GET` | `/health` | Healthcheck. |

### `consumer` — `:8004` *(interno)*

| Método | Ruta | Descripción |
|---|---|---|
| `GET` | `/health` | Estado + contadores locales: `processed`, `retried`, `recovered`, `to_dlq`, `errors`. |

### `cache_service` — `:8001`

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/query` | Consulta Q1–Q5. Devuelve HIT/MISS, o **502** si el backend falló. |
| `GET` | `/stats` | Stats de Redis. |
| `POST` | `/flush` | Limpia el caché. |
| `GET` | `/health` | Healthcheck. |

### `response_generator` — `:8002`

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/query` | Computa Q1–Q5. |
| `POST` | `/admin/fail` | Simula caída: `{"enabled": bool, "prob": float}`. |
| `GET` | `/admin/status` | Estado de la simulación de caída. |
| `GET` | `/health` | Healthcheck. |
| `GET` | `/stats` | Estadísticas del backend. |

### `metrics` — `:8003`

| Método | Ruta | Descripción |
|---|---|---|
| `POST` | `/event` | Recibe eventos (`hit`/`miss`/`processed`/`retry`/`dlq`). |
| `GET` | `/summary` | KPIs completos: hit rate, latencias, `async_kpis`, `queue_health`. |
| `GET` | `/backlog` | Solo salud de la cola (lag en vivo). |
| `GET` | `/summary/by_query` | Desglose por Q1–Q5. |
| `POST` | `/snapshot` | Persiste un snapshot a disco. |
| `POST` | `/reset` | Reinicia los contadores. |
| `GET` | `/health` | Healthcheck. |

---

## 11. Troubleshooting

**El broker `kafka` no arranca / `advertised.listeners cannot use 0.0.0.0`**

Usar host vacío en lugar de `0.0.0.0`: `PLAINTEXT://:9092,CONTROLLER://:9093` con `KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092`. Ya está corregido en `docker-compose.yml`.

**`Conflict. The container name ... is already in use`**

Quedaron contenedores de una corrida previa. Ejecuta:

```bash
docker compose down --remove-orphans
```

**El `consumer` no procesa nada / el *backlog* no baja**

Verifica que el broker esté *healthy* y que los *topics* existan:

```bash
docker compose ps
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --list
```

**`/admin/fail` parece no tener efecto**

Asegúrate de enviar el header `Content-Type: application/json`. Sin él, FastAPI rechaza el body y la falla no se activa.

**El dataset no se encuentra**

```bash
git checkout -- data/santiago_buildings.parquet
```

---

## 12. Arquitectura síncrona previa (Tarea 1)

La **Tarea 1** era una cadena puramente **síncrona** de cuatro servicios, sin Kafka ni consumidores:

```
traffic_generator ──HTTP──► cache_service ──HTTP (en miss)──► response_generator
                                  │
                                redis
```

El Generador de Tráfico llamaba directamente por HTTP al `cache_service` y **esperaba** la respuesta. En un *miss*, el `cache_service` llamaba de forma bloqueante al backend. Esto acopla cliente y backend: una ráfaga o una caída se propagan de inmediato como latencia o errores al cliente.

Esa ruta **sigue disponible** a través del modo `mode: "sync"` del `traffic_generator`, y se usa como "Sistema Base" en el escenario `base_sync` para comparar contra la nueva arquitectura asíncrona. La batería original de la Tarea 1 (políticas LRU/LFU/FIFO × tamaños 50/200/500 MB) vive en `experiments/master_run.py` y `experiments/build_figures.py`.

---

*Tarea 2 — Sistemas Distribuidos, UDP 2026-1 · Gabriel González · Maximiliano Solorza*
