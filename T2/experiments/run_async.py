# ================== dependencias del proyecto ===================== #
import argparse
import json
import os
import time
import subprocess
import urllib.request
from pathlib import Path

# ================== configuraciones globales ===================== #
TRAFFIC  = os.getenv("TRAFFIC_URL",  "http://localhost:8000")
CACHE    = os.getenv("CACHE_URL",    "http://localhost:8001")
RESP_GEN = os.getenv("RESP_GEN_URL", "http://localhost:8002")
METRICS  = os.getenv("METRICS_URL",  "http://localhost:8003")
PROJECT_ROOT = Path(__file__).parent.parent
RESULTS  = PROJECT_ROOT / "results"

RATE     = 60
DURATION = 40


# ================== helpers HTTP ===================== #
def post(url, body=None, timeout=60):
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={"Content-Type": "application/json"}, method="POST")
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read())


def get(url, timeout=10):
    with urllib.request.urlopen(url, timeout=timeout) as r:
        return json.loads(r.read())


def wait_for_services(retries=90, interval=1.0):
    for svc, url in [("traffic", TRAFFIC), ("cache", CACHE),
                     ("resp_gen", RESP_GEN), ("metrics", METRICS)]:
        print(f"  Esperando {svc}...", end="", flush=True)
        for _ in range(retries):
            try:
                if get(f"{url}/health", timeout=2)["status"] == "ok":
                    print(" OK", flush=True)
                    break
            except Exception:
                pass
            time.sleep(interval)
        else:
            raise RuntimeError(f"{svc} no respondió en {retries}s")


# ================== control de replicas de consumer ===================== #
def scale_consumers(n: int):
    print(f"  Escalando consumer={n}...", flush=True)
    subprocess.run(
        ["docker-compose", "up", "-d", "--no-recreate", "--scale", f"consumer={n}"],
        cwd=PROJECT_ROOT, check=True,
    )
    time.sleep(8)


def set_fail(enabled: bool, prob: float = 1.0):
    post(f"{RESP_GEN}/admin/fail", {"enabled": enabled, "prob": prob}, timeout=5)
    print(f"  resp_gen FAIL={'ON' if enabled else 'OFF'} (prob={prob})", flush=True)


# ================== drenado de backlog ===================== #
def wait_traffic_done(extra_wait=2.0, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if not get(f"{TRAFFIC}/status").get("running"):
                break
        except Exception:
            pass
        time.sleep(2.0)
    time.sleep(extra_wait)


def wait_backlog_drained(timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            b = get(f"{METRICS}/backlog")
            if b.get("backlog_total") == 0 and not b.get("draining"):
                return True
        except Exception:
            pass
        time.sleep(2.0)
    return False


# ================== ejecución de un escenario ===================== #
def snapshot(label, extra=None):
    snap = post(f"{METRICS}/snapshot",
                {"label": label, "extra": extra or {}}, timeout=30)
    summary = snap["summary"]
    RESULTS.mkdir(parents=True, exist_ok=True)
    with open(RESULTS / f"snap_{label}.json", "w") as f:
        json.dump(snap, f, indent=2, default=str)

    a = summary.get("async_kpis") or {}
    qh = summary.get("queue_health") or {}
    print(f"    hit={summary.get('hit_rate')}  "
          f"processed={a.get('processed_total')}  "
          f"thr={a.get('throughput_processed_qps_total')}qps  "
          f"p95_e2e={(a.get('latency_e2e_ms') or {}).get('p95')}ms  "
          f"retry_rate={a.get('retry_rate')}  dlq_rate={a.get('dlq_rate')}  "
          f"recovery_rate={a.get('recovery_rate')}  "
          f"peak_backlog={qh.get('peak_backlog')}  "
          f"recovery_time={qh.get('recovery_time_sec')}s", flush=True)
    return summary


def start_traffic(label, mode="kafka", duration=DURATION, rate=RATE,
                  distribution="zipf", spike_at=None, spike_rate=None):
    post(f"{METRICS}/reset")
    post(f"{CACHE}/flush")
    cfg = {
        "mode": mode,
        "distribution": distribution,
        "rate_qps": float(rate),
        "duration_sec": float(duration),
        "zipf_s": 1.5,
        "concurrency": 16,
        "seed": 42,
        "label": label,
    }
    if spike_at is not None:
        cfg["spike_at_sec"] = float(spike_at)
        cfg["spike_rate_qps"] = float(spike_rate)
    post(f"{TRAFFIC}/run", cfg)
    return cfg


# ================== escenarios de testeo ===================== #
def sc_base_sync():
    print("\n=== [1] Sistema Base (síncrono, sin Kafka) ===", flush=True)
    cfg = start_traffic("base_sync", mode="sync", duration=DURATION)
    wait_traffic_done()
    snapshot("base_sync", extra=cfg)


def sc_kafka_1():
    print("\n=== [2] Kafka + 1 consumidor ===", flush=True)
    scale_consumers(1)
    cfg = start_traffic("kafka_1c", mode="kafka", duration=DURATION)
    wait_traffic_done()
    wait_backlog_drained()
    snapshot("kafka_1c", extra=cfg)


def sc_kafka_3():
    print("\n=== [3] Kafka + 3 consumidores (escalamiento) ===", flush=True)
    scale_consumers(3)
    cfg = start_traffic("kafka_3c", mode="kafka", duration=DURATION)
    wait_traffic_done()
    wait_backlog_drained()
    snapshot("kafka_3c", extra=cfg)
    scale_consumers(1)  # volver al estado base


def sc_failure():
    print("\n=== [4] Falla temporal + reintentos + recuperación ===", flush=True)
    scale_consumers(2)
    cfg = start_traffic("failure", mode="kafka", duration=60, rate=60)
    time.sleep(12)                 # llenar cache y procesar normal
    set_fail(True, prob=1.0)       # tirar abajo el generador
    time.sleep(18)                 # acumular backlog en reintentos
    set_fail(False)                # restaurar -> recuperacion
    wait_traffic_done()
    wait_backlog_drained(timeout=150)
    snapshot("failure", extra={**cfg, "fail_window_sec": 18})
    scale_consumers(1)


def sc_spike():
    print("\n=== [5] Spike de tráfico ===", flush=True)
    scale_consumers(2)
    cfg = start_traffic("spike", mode="kafka", duration=50,
                        rate=40, spike_at=20, spike_rate=250)
    wait_traffic_done()
    wait_backlog_drained(timeout=150)
    snapshot("spike", extra=cfg)
    scale_consumers(1)


SCENARIOS = {
    "base_sync": sc_base_sync,
    "kafka_1":   sc_kafka_1,
    "kafka_3":   sc_kafka_3,
    "failure":   sc_failure,
    "spike":     sc_spike,
}


# ================== main ===================== #
def main():
    p = argparse.ArgumentParser(description="Batería asíncrona (Tarea 2)")
    p.add_argument("--scenario", choices=["all"] + list(SCENARIOS),
                   default="all")
    args = p.parse_args()

    wait_for_services()
    set_fail(False)  # asegurar estado limpio

    t0 = time.time()
    if args.scenario == "all":
        for fn in SCENARIOS.values():
            fn()
    else:
        SCENARIOS[args.scenario]()
    print(f"\n✓ Listo en {(time.time()-t0)/60:.1f} min. Resultados en results/", flush=True)


if __name__ == "__main__":
    main()
