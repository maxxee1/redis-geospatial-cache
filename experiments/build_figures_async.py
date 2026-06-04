# ================== figuras Tarea 2 (arquitectura asíncrona) ============== #

import json
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np

matplotlib.rcParams.update({
    "font.size": 11, "axes.titlesize": 12, "axes.labelsize": 11,
    "legend.fontsize": 9, "figure.dpi": 110, "savefig.dpi": 180,
    "savefig.bbox": "tight", "axes.spines.top": False, "axes.spines.right": False,
})

ROOT = Path(__file__).parent.parent
RES = ROOT / "results"
FIG = ROOT / "informe" / "figs"
FIG.mkdir(parents=True, exist_ok=True)

C = {"base_sync": "#6b7280", "kafka_1c": "#1d4faa", "kafka_3c": "#0a8754",
     "failure": "#c1453b", "spike": "#b8860b"}
LABELS = {"base_sync": "Base\n(sync)", "kafka_1c": "Kafka\n1 cons.",
          "kafka_3c": "Kafka\n3 cons.", "failure": "Falla +\nrecovery", "spike": "Spike"}


def load(label):
    return json.load(open(RES / f"snap_{label}.json"))["summary"]


S = {k: load(k) for k in C if (RES / f"snap_{k}.json").exists()}


def save(fig, name):
    p = FIG / name
    fig.savefig(p.with_suffix(".pdf"))
    fig.savefig(p.with_suffix(".png"))
    plt.close(fig)
    print(f"  ✓ {p.name}.pdf")


# Fig 1 throughput procesado por escenario
def fig_throughput():
    fig, ax = plt.subplots(figsize=(7, 3.8))
    order = list(S)
    vals = [(S[k].get("async_kpis") or {}).get("throughput_processed_qps_total") or
            S[k].get("throughput_qps_total") or 0 for k in order]
    bars = ax.bar([LABELS[k] for k in order], vals, color=[C[k] for k in order])
    for b, v in zip(bars, vals):
        ax.text(b.get_x() + b.get_width() / 2, v + 1, f"{v:.1f}", ha="center", fontsize=10)
    ax.set_ylabel("Throughput (consultas/s)")
    ax.set_title("Throughput por escenario")
    ax.set_ylim(0, max(vals) * 1.18)
    save(fig, "fig_t3_throughput")


# Fig 2 latencia E2E p50/p95/p99 por escenario Kafka
def fig_latency():
    order = [k for k in S if (S[k].get("async_kpis") or {}).get("latency_e2e_ms", {}).get("p50") is not None]
    p50 = [S[k]["async_kpis"]["latency_e2e_ms"]["p50"] / 1000 for k in order]
    p95 = [S[k]["async_kpis"]["latency_e2e_ms"]["p95"] / 1000 for k in order]
    p99 = [S[k]["async_kpis"]["latency_e2e_ms"]["p99"] / 1000 for k in order]
    x = np.arange(len(order)); w = 0.26
    fig, ax = plt.subplots(figsize=(7.5, 3.8))
    ax.bar(x - w, p50, w, label="p50", color="#1d4faa")
    ax.bar(x, p95, w, label="p95", color="#0a8754")
    ax.bar(x + w, p99, w, label="p99", color="#c1453b")
    ax.set_xticks(x); ax.set_xticklabels([LABELS[k] for k in order])
    ax.set_ylabel("Latencia extremo a extremo (s)")
    ax.set_title("Latencia E2E (incluye espera en cola)")
    ax.legend()
    save(fig, "fig_t3_latency")


# Fig 3 escalamiento 1 vs 3 consumidores
def fig_scaling():
    a1, a3 = S["kafka_1c"], S["kafka_3c"]
    metrics = [
        ("Throughput\n(q/s)", a1["async_kpis"]["throughput_processed_qps_total"],
         a3["async_kpis"]["throughput_processed_qps_total"]),
        ("Latencia E2E\np50 (s)", a1["async_kpis"]["latency_e2e_ms"]["p50"] / 1000,
         a3["async_kpis"]["latency_e2e_ms"]["p50"] / 1000),
        ("Backlog\npico", a1["queue_health"]["peak_backlog"], a3["queue_health"]["peak_backlog"]),
        ("Recovery\ntime (s)", a1["queue_health"]["recovery_time_sec"],
         a3["queue_health"]["recovery_time_sec"]),
    ]
    fig, axes = plt.subplots(1, 4, figsize=(10, 3.4))
    for ax, (name, v1, v3) in zip(axes, metrics):
        b = ax.bar(["1", "3"], [v1, v3], color=["#1d4faa", "#0a8754"])
        for bb, v in zip(b, [v1, v3]):
            ax.text(bb.get_x() + bb.get_width() / 2, v, f"{v:.1f}",
                    ha="center", va="bottom", fontsize=9)
        ax.set_title(name, fontsize=10)
        ax.set_xlabel("nº consumidores")
        ax.margins(y=0.18)
    fig.suptitle("Escalamiento del grupo de consumidores (1 vs 3)", y=1.02)
    save(fig, "fig_t3_scaling")


# Fig 4 tolerancia a fallos (escenario failure)
def fig_faults():
    a = S["failure"]["async_kpis"]; q = S["failure"]["queue_health"]
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(9, 3.6))
    rates = [("Retry\nrate", a["retry_rate"]), ("Recovery\nrate", a["recovery_rate"]),
             ("DLQ\nrate", a["dlq_rate"])]
    b = ax1.bar([r[0] for r in rates], [r[1] for r in rates],
                color=["#b8860b", "#0a8754", "#c1453b"])
    for bb, (_, v) in zip(b, rates):
        ax1.text(bb.get_x() + bb.get_width() / 2, v, f"{v:.3f}", ha="center", va="bottom")
    ax1.set_ylim(0, 1.05); ax1.set_ylabel("tasa")
    ax1.set_title("Tasas de tolerancia a fallos")
    counts = [("retried", a["retried_total"]), ("recovered", a["recovered_total"]),
              ("DLQ", a["dlq_total"]), ("backlog\npico", q["peak_backlog"])]
    b2 = ax2.bar([c[0] for c in counts], [c[1] for c in counts],
                 color=["#b8860b", "#0a8754", "#c1453b", "#6b7280"])
    for bb, (_, v) in zip(b2, counts):
        ax2.text(bb.get_x() + bb.get_width() / 2, v, f"{v}", ha="center", va="bottom")
    ax2.set_yscale("log"); ax2.set_ylabel("mensajes (log)")
    ax2.set_title(f"Conteos (recovery time = {q['recovery_time_sec']} s)")
    save(fig, "fig_t3_faults")


# Fig 5 backlog pico y recovery time por escenario
def fig_backlog():
    order = [k for k in S if (S[k].get("queue_health") or {}).get("peak_backlog")]
    peak = [S[k]["queue_health"]["peak_backlog"] for k in order]
    rec = [S[k]["queue_health"]["recovery_time_sec"] or 0 for k in order]
    x = np.arange(len(order))
    fig, ax1 = plt.subplots(figsize=(7.5, 3.8))
    b = ax1.bar(x - 0.2, peak, 0.4, label="Backlog pico", color="#1d4faa")
    ax1.set_ylabel("Backlog pico (mensajes)", color="#1d4faa")
    ax2 = ax1.twinx()
    ax2.bar(x + 0.2, rec, 0.4, label="Recovery time", color="#c1453b")
    ax2.set_ylabel("Recovery time (s)", color="#c1453b")
    ax2.spines.right.set_visible(True)
    ax1.set_xticks(x); ax1.set_xticklabels([LABELS[k] for k in order])
    ax1.set_title("Salud de la cola: backlog pico y tiempo de recuperación")
    save(fig, "fig_t3_backlog")


if __name__ == "__main__":
    print("Generando figuras Tarea 3...")
    fig_throughput(); fig_latency(); fig_scaling(); fig_faults(); fig_backlog()
    print(f"Listo. Figuras en {FIG}")
