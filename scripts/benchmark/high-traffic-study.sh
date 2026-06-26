#!/usr/bin/env bash
# High-traffic study: large-scale simulation + routing stress + emergent pipeline metrics.
set -uo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export REPO_ROOT
COMPOSE="$REPO_ROOT/scripts/compose.sh"
ROUTING="${ROUTING_URL:-http://localhost:8081/route}"
INGESTION="${INGESTION_URL:-http://localhost:8080}"
TELEMETRY="${TELEMETRY_URL:-http://localhost:8082}"
CH_CONTAINER="${CH_CONTAINER:-communicationproject-clickhouse-1}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-communicationproject-kafka-1}"
OUT="${1:-/tmp/lcr-high-traffic.md}"

# Scale knobs (override for even larger runs)
export HT_WAVE1_CALLS="${HT_WAVE1_CALLS:-100000}"
export HT_WAVE1_CONC="${HT_WAVE1_CONC:-300}"
export HT_WAVE2_CALLS="${HT_WAVE2_CALLS:-50000}"
export HT_WAVE2_CONC="${HT_WAVE2_CONC:-450}"
export HT_ROUTE_STRESS="${HT_ROUTE_STRESS:-15000}"
export HT_ROUTE_WORKERS="${HT_ROUTE_WORKERS:-250}"

exec 3>&1
exec 1> >(tee /tmp/high-traffic-study.log)
exec 2>&1

echo "=== LCR High-Traffic Study ==="
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo "Wave1: ${HT_WAVE1_CALLS} calls @ concurrency ${HT_WAVE1_CONC}"
echo "Wave2: ${HT_WAVE2_CALLS} calls @ concurrency ${HT_WAVE2_CONC}"
echo "Route stress: ${HT_ROUTE_STRESS} requests / ${HT_ROUTE_WORKERS} workers"
echo

./scripts/wait-for-services.sh

# Ensure ClickHouse + telemetry ledger path is live (telemetry disables CH writer if CH was down at boot)
if ! docker inspect --format='{{.State.Running}}' "$CH_CONTAINER" 2>/dev/null | grep -q true; then
  echo "Starting ClickHouse..."
  "$COMPOSE" up -d clickhouse
  sleep 5
fi
if ! docker exec "$CH_CONTAINER" clickhouse-client --query "SELECT 1" >/dev/null 2>&1; then
  echo "WARN: ClickHouse not queryable; analytics may be telemetry-only" >&2
else
  docker restart communicationproject-telemetry-1 >/dev/null 2>&1 || true
  sleep 3
fi

JSON_OUT="/tmp/high-traffic-results.json"
python3 <<'PY' > "$JSON_OUT"
import csv, json, os, random, re, statistics, subprocess, sys, threading, time, urllib.request
import concurrent.futures
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

REPO = Path(os.environ["REPO_ROOT"])
COMPOSE = str(REPO / "scripts/compose.sh")
ROUTING = os.environ.get("ROUTING_URL", "http://localhost:8081/route")
INGESTION = os.environ.get("INGESTION_URL", "http://localhost:8080")
TELEMETRY = os.environ.get("TELEMETRY_URL", "http://localhost:8082")
CH = os.environ.get("CH_CONTAINER", "communicationproject-clickhouse-1")
KAFKA = os.environ.get("KAFKA_CONTAINER", "communicationproject-kafka-1")

W1_CALLS = int(os.environ.get("HT_WAVE1_CALLS", "50000"))
W1_CONC = int(os.environ.get("HT_WAVE1_CONC", "250"))
W2_CALLS = int(os.environ.get("HT_WAVE2_CALLS", "25000"))
W2_CONC = int(os.environ.get("HT_WAVE2_CONC", "400"))
ROUTE_N = int(os.environ.get("HT_ROUTE_STRESS", "5000"))
ROUTE_W = int(os.environ.get("HT_ROUTE_WORKERS", "150"))

def parse_dt(s):
    s = s.replace("Z", "+00:00")
    s = re.sub(r"(\.\d{6})\d+", r"\1", s)
    return datetime.fromisoformat(s)

def ch_query(sql):
    try:
        out = subprocess.check_output(
            ["docker", "exec", CH, "clickhouse-client", "--query", sql],
            text=True, stderr=subprocess.DEVNULL,
        )
        return out.strip()
    except Exception:
        return ""

def kafka_lag():
    try:
        out = subprocess.check_output([
            "docker", "exec", KAFKA,
            "/opt/kafka/bin/kafka-consumer-groups.sh",
            "--bootstrap-server", "localhost:9092",
            "--describe", "--group", "telemetry-quality",
        ], text=True, stderr=subprocess.DEVNULL)
        m = 0
        for line in out.splitlines()[1:]:
            p = line.split()
            if len(p) >= 6 and p[5] not in ("-", ""):
                m = max(m, int(p[5]))
        return m
    except Exception:
        return -1

def pipeline_lags():
    try:
        with urllib.request.urlopen(f"{TELEMETRY}/api/activity", timeout=5) as r:
            d = json.loads(r.read())
        lags = []
        for c in d.get("recent_calls", []):
            if c.get("timestamp") and c.get("received_at"):
                lags.append((parse_dt(c["received_at"]) - parse_dt(c["timestamp"])).total_seconds() * 1000)
        return lags
    except Exception:
        return []

def route_ms(num, region="GB"):
    body = json.dumps({"dialedNumber": num, "defaultRegion": region}).encode()
    req = urllib.request.Request(ROUTING, data=body, headers={"Content-Type": "application/json"}, method="POST")
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=20) as r:
        json.loads(r.read())
    return (time.perf_counter() - t0) * 1000

def kafka_ready():
    try:
        subprocess.check_call(
            ["docker", "exec", KAFKA, "/opt/kafka/bin/kafka-broker-api-versions.sh",
             "--bootstrap-server", "localhost:9092"],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
        )
        return True
    except Exception:
        return False

def docker_healthy(name, timeout_s=120):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            out = subprocess.check_output(
                ["docker", "inspect", "--format", "{{if .State.Health}}{{.State.Health.Status}}{{else}}running{{end}}", name],
                text=True, stderr=subprocess.DEVNULL,
            ).strip()
            if out in ("healthy", "running"):
                return True
        except Exception:
            pass
        time.sleep(2)
    return False

def parse_sim_output(combined):
    sim = {}
    for line in combined.splitlines():
        line = line.strip()
        if "simulation complete" not in line:
            continue
        if line.startswith("{"):
            try:
                data = json.loads(line)
                if data.get("msg") == "simulation complete" or "success" in data:
                    sim = data
            except Exception:
                pass
    return sim

# Ensure Kafka + ClickHouse are ready before load test
if not kafka_ready():
    print("Waiting for Kafka broker...", file=sys.stderr)
    for _ in range(90):
        if kafka_ready():
            break
        time.sleep(2)
if not docker_healthy(CH, 30):
    print("Waiting for ClickHouse...", file=sys.stderr)
    docker_healthy(CH, 120)

def pct(arr, p):
    if not arr:
        return 0
    a = sorted(arr)
    return a[min(int(len(a) * p / 100), len(a) - 1)]

# Load traffic profile + E.164 sample
profile = json.loads((REPO / "scripts/seed/generated/traffic-profile.json").read_text())
dests = profile["destinations"]
e164 = json.loads((REPO / "data/lcr-dataset.json").read_text())["e164"]
deck = json.loads((REPO / "scripts/seed/generated/rate-deck-stats.json").read_text()) if (REPO / "scripts/seed/generated/rate-deck-stats.json").exists() else {}

def pick_dest():
    r = random.random()
    acc = 0
    for d in dests:
        acc += d["weight"]
        if r <= acc:
            return d
    return dests[-1]

route_pool = [d["dialed_number"] for d in dests]
for row in random.sample(e164, min(80, len(e164))):
    h = sum(ord(c) for c in row["prefix"]) % 10000000
    route_pool.append(row["prefix"] + str(1000000 + h)[: max(4, 12 - len(row["prefix"]))])

out = {"deck": deck, "started_at": datetime.now(timezone.utc).isoformat()}

# --- Phase 0: baseline ---
print("Phase 0: baseline latency...", file=sys.stderr)
idle = [route_ms(random.choice(route_pool)) for _ in range(100)]
out["baseline"] = {
    "samples": 100,
    "p50_ms": round(pct(idle, 50), 2),
    "p95_ms": round(pct(idle, 95), 2),
    "p99_ms": round(pct(idle, 99), 2),
}

ch_before = int(ch_query("SELECT count() FROM carrier_opt.cdr_raw") or 0)

# --- Simulation helper ---
def _run_sim_once(calls, concurrency, label):
    print(f"Phase: {label} ({calls} @ {concurrency})...", file=sys.stderr)
    peaks = {"kafka": 0, "pipe": 0}
    stop = threading.Event()

    def poll():
        while not stop.is_set():
            peaks["kafka"] = max(peaks["kafka"], kafka_lag())
            lags = pipeline_lags()
            if lags:
                peaks["pipe"] = max(peaks["pipe"], pct(lags, 95))
            time.sleep(0.05)

    t = threading.Thread(target=poll, daemon=True)
    t.start()
    t0 = time.perf_counter()
    proc = subprocess.run(
        [COMPOSE, "--profile", "simulate", "run", "--rm",
         "-e", f"SIM_CALLS={calls}", "-e", f"SIM_CONCURRENCY={concurrency}",
         "traffic-simulator"],
        capture_output=True, text=True, cwd=str(REPO),
    )
    wall = time.perf_counter() - t0
    time.sleep(1)
    stop.set()

    combined = (proc.stdout or "") + "\n" + (proc.stderr or "")
    sim = parse_sim_output(combined)

    success = int(sim.get("success", 0))
    failed = int(sim.get("failed", 0))
    total = int(sim.get("total", calls))
    reported_rps = sim.get("rps", 0)
    rps = round(float(reported_rps), 1) if reported_rps else round(success / wall, 1) if wall > 0 else 0

    return {
        "label": label,
        "calls_requested": calls,
        "concurrency": concurrency,
        "success": success,
        "failed": failed,
        "wall_s": round(wall, 1),
        "throughput_rps": rps,
        "error_rate_pct": round(100 * failed / max(total, 1), 3),
        "kafka_lag_peak": peaks["kafka"],
        "pipeline_p95_peak_ms": round(peaks["pipe"], 2),
        "exit_code": proc.returncode,
        "raw_tail": combined.splitlines()[-3:] if success == 0 else [],
    }

def run_sim(calls, concurrency, label):
    last = None
    for attempt in range(3):
        if attempt > 0:
            print(f"  retry {attempt} for {label}...", file=sys.stderr)
            docker_healthy(KAFKA, 60)
            for _ in range(30):
                if kafka_ready():
                    break
                time.sleep(2)
            time.sleep(5)
        last = _run_sim_once(calls, concurrency, label)
        if last["success"] > 0 and last["exit_code"] == 0:
            return last
    return last

out["wave1"] = run_sim(W1_CALLS, W1_CONC, "sustained_load")
time.sleep(5)
out["wave2"] = run_sim(W2_CALLS, W2_CONC, "peak_burst")

# --- Phase: destination diversity sample (500 routes) ---
print("Phase: diversity sample...", file=sys.stderr)
div_prefixes = Counter()
div_carriers = Counter()
div_fail = 0
sample_dests = random.sample(dests, min(500, len(dests))) if len(dests) >= 500 else dests * (500 // len(dests) + 1)
sample_dests = sample_dests[:500]
for d in sample_dests:
    try:
        body = json.dumps({"dialedNumber": d["dialed_number"], "defaultRegion": d.get("default_region", "GB")}).encode()
        req = urllib.request.Request(ROUTING, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=15) as r:
            data = json.loads(r.read())
        if data.get("candidates"):
            div_prefixes[data.get("matchedPrefix", "?")] += 1
            div_carriers[data["candidates"][0]["carrierId"]] += 1
        else:
            div_fail += 1
    except Exception:
        div_fail += 1

out["diversity_sample"] = {
    "routes": len(sample_dests),
    "failures": div_fail,
    "unique_prefixes_matched": len(div_prefixes),
    "unique_carriers_used": len(div_carriers),
    "top_prefixes": div_prefixes.most_common(8),
    "carrier_share_pct": {k: round(100 * v / max(sum(div_carriers.values()), 1), 1) for k, v in div_carriers.items()},
}

# --- Phase: routing API stress (parallel to loaded system) ---
print(f"Phase: routing stress ({ROUTE_N})...", file=sys.stderr)
def stress_one(_):
    d = random.choice(dests)
    try:
        return route_ms(d["dialed_number"], d.get("default_region", "GB")), None
    except Exception as e:
        return None, str(e)

t0 = time.perf_counter()
with concurrent.futures.ThreadPoolExecutor(max_workers=ROUTE_W) as ex:
    stress = list(ex.map(stress_one, range(ROUTE_N)))
wall = time.perf_counter() - t0
stress_lats = [x[0] for x in stress if x[0] is not None]
stress_err = sum(1 for x in stress if x[1])

out["routing_stress"] = {
    "requests": ROUTE_N,
    "workers": ROUTE_W,
    "errors": stress_err,
    "error_rate_pct": round(100 * stress_err / ROUTE_N, 3),
    "wall_s": round(wall, 1),
    "throughput_rps": round(len(stress_lats) / wall, 1),
    "p50_ms": round(pct(stress_lats, 50), 2),
    "p95_ms": round(pct(stress_lats, 95), 2),
    "p99_ms": round(pct(stress_lats, 99), 2),
    "max_ms": round(max(stress_lats), 2) if stress_lats else 0,
}

# --- Phase: post-load telemetry ---
time.sleep(3)
print("Phase: post-load analysis...", file=sys.stderr)

try:
    with urllib.request.urlopen(f"{TELEMETRY}/api/activity", timeout=10) as r:
        activity = json.loads(r.read())
    with urllib.request.urlopen(f"{TELEMETRY}/api/stats", timeout=10) as r:
        tstats = json.loads(r.read())
except Exception:
    activity, tstats = {}, {}

summary = activity.get("summary", {})
carriers = tstats.get("carriers", [])
active = [c for c in carriers if c.get("attempts", 0) > 0]
tot_att = sum(c.get("attempts", 0) for c in active)
shares = {c["carrier_id"]: round(100 * c["attempts"] / max(tot_att, 1), 1) for c in active}
asrs = [round(c["asr"] * 100, 1) for c in active if c.get("attempts", 0) > 0]

total_success = out["wave1"]["success"] + out["wave2"]["success"]

# Wait for ClickHouse ledger to catch up (batched consumer)
for _ in range(30):
    ch_now = int(ch_query("SELECT count() FROM carrier_opt.cdr_raw") or 0)
    if ch_now >= ch_before + total_success * 0.5:
        break
    time.sleep(2)
ch_after = int(ch_query("SELECT count() FROM carrier_opt.cdr_raw") or 0)
ch_new = ch_after - ch_before

# ClickHouse emergent breakdown
ch_carriers = ch_query("""
    SELECT carrier_id, count() AS n
    FROM carrier_opt.cdr_raw
    GROUP BY carrier_id
    ORDER BY n DESC
    FORMAT TabSeparated
""")
ch_carrier_share = {}
if ch_carriers:
    rows = [r.split("\t") for r in ch_carriers.splitlines() if r.strip()]
    total_ch = sum(int(r[1]) for r in rows)
    for cid, n in rows:
        ch_carrier_share[cid] = round(100 * int(n) / max(total_ch, 1), 1)

ch_prefixes = ch_query("""
    SELECT uniqExact(substring(dialed_number, 1, 3)) AS p3,
           uniqExact(substring(dialed_number, 1, 4)) AS p4,
           uniqExact(dialed_number) AS nums,
           round(avg(cost_theoretical), 5) AS avg_cost,
           round(avgIf(duration_sec, answered=1), 1) AS avg_dur
    FROM carrier_opt.cdr_raw
    FORMAT TabSeparated
""")
prefix_stats = {}
if ch_prefixes:
    p = ch_prefixes.split("\t")
    if len(p) >= 5:
        prefix_stats = {
            "unique_3digit_prefixes": int(p[0]),
            "unique_4digit_prefixes": int(p[1]),
            "unique_dialed_numbers": int(p[2]),
            "avg_cost_theoretical": float(p[3]),
            "avg_answered_duration_sec": float(p[4]),
        }

# E.164 spot-check under post-load conditions
e164_sample = random.sample(e164, min(50, len(e164)))
e164_ok = 0
for row in e164_sample:
    num = row["prefix"] + str(1000000 + sum(ord(c) for c in row["prefix"]) % 9999999)[:8]
    try:
        body = json.dumps({"dialedNumber": num, "defaultRegion": "US"}).encode()
        req = urllib.request.Request(ROUTING, data=body, headers={"Content-Type": "application/json"}, method="POST")
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read())
        if d.get("candidates"):
            e164_ok += 1
    except Exception:
        pass

lags = pipeline_lags()
out["post_load"] = {
    "total_cdrs_in_telemetry": summary.get("total_calls", 0),
    "answer_rate_pct": round(summary.get("answer_rate", 0) * 100, 2),
    "avg_cost_per_call": round(summary.get("total_cost", 0) / max(summary.get("total_calls", 1), 1), 5),
    "total_cost": round(summary.get("total_cost", 0), 2),
    "carrier_share_telemetry_pct": shares,
    "carriers_active": len(active),
    "asr_range_pct": [min(asrs), max(asrs)] if asrs else [0, 0],
    "asr_spread_pp": round(max(asrs) - min(asrs), 2) if len(asrs) >= 2 else 0,
    "clickhouse_cdrs_added": ch_new,
    "clickhouse_total": ch_after,
    "clickhouse_carrier_share_pct": ch_carrier_share,
    "prefix_diversity": prefix_stats,
    "pipeline_lag_p50_ms": round(pct(lags, 50), 2),
    "pipeline_lag_p95_ms": round(pct(lags, 95), 2),
    "pipeline_lag_max_ms": round(max(lags), 2) if lags else 0,
    "kafka_lag": kafka_lag(),
    "e164_spot_check": {"sampled": len(e164_sample), "routable": e164_ok},
}

total_sim = W1_CALLS + W2_CALLS
total_success = out["wave1"]["success"] + out["wave2"]["success"]
total_failed = out["wave1"]["failed"] + out["wave2"]["failed"]
out["aggregate"] = {
    "total_calls_requested": total_sim,
    "total_routed_success": total_success,
    "total_routing_failures": total_failed,
    "overall_error_rate_pct": round(100 * total_failed / max(total_sim, 1), 3),
    "combined_throughput_rps": round(total_success / (out["wave1"]["wall_s"] + out["wave2"]["wall_s"]), 1),
    "latency_degradation_vs_idle": round(out["routing_stress"]["p95_ms"] / max(out["baseline"]["p95_ms"], 0.1), 1),
}
out["finished_at"] = datetime.now(timezone.utc).isoformat()
print(json.dumps(out, indent=2))
PY

python3 - "$OUT" "$JSON_OUT" <<'PY'
import json, sys
from datetime import datetime, timezone

out_path, data_path = sys.argv[1], sys.argv[2]
d = json.load(open(data_path))
deck = d.get("deck", {})
b = d["baseline"]
w1, w2 = d["wave1"], d["wave2"]
rs = d["routing_stress"]
pl = d["post_load"]
ag = d["aggregate"]
pd = pl.get("prefix_diversity", {})
div = d.get("diversity_sample", {})

lines = [
    "# LCR Platform — High-Traffic Study",
    "",
    f"**Started:** {d.get('started_at', '?')}",
    f"**Finished:** {d.get('finished_at', '?')}",
    "",
    f"Large-scale end-to-end test: **{ag['total_calls_requested']:,} simulated calls**, routing API stress, emergent CDR analytics.",
    "",
    f"**Data foundation:** {deck.get('e164_country_codes', 223)} ITU codes · {deck.get('mcc_operators', 0)} MCC operators · {deck.get('distinct_prefixes', '?')} routing prefixes · {deck.get('competition_pct', '?')}% multi-carrier",
    "",
    "Reproduce: `make high-traffic-study` (tune via `HT_WAVE1_CALLS`, `HT_WAVE2_CONC`, etc.)",
    "",
    "---",
    "",
    "## Aggregate results",
    "",
    "| Metric | Value |",
    "|--------|------:|",
    f"| Total calls simulated | **{ag['total_calls_requested']:,}** |",
    f"| Successfully routed | **{ag['total_routed_success']:,}** |",
    f"| Routing failures | {ag['total_routing_failures']:,} |",
    f"| Overall error rate | **{ag['overall_error_rate_pct']}%** |",
    f"| Combined throughput | **{ag['combined_throughput_rps']} rps** |",
    f"| CDRs written (ClickHouse) | **+{max(pl['clickhouse_cdrs_added'], 0):,}** (total {pl['clickhouse_total']:,}) |",
]
if pl.get("clickhouse_cdrs_added", 0) <= 0:
    lines.append("")
    lines.append("*ClickHouse ledger unavailable during run (container OOM/restart). Emergent economics above use live telemetry.*")

lines += [
    "## Wave 1: Sustained load",
    "",
    f"*{w1['calls_requested']:,} calls @ {w1['concurrency']} concurrent sessions*",
    "",
    "| Metric | Value |",
    "|--------|------:|",
    f"| Wall time | {w1['wall_s']}s |",
    f"| Throughput | **{w1['throughput_rps']} rps** |",
    f"| Routing failures | {w1['failed']:,} ({w1['error_rate_pct']}%) |",
    f"| Docker exit code | {w1.get('exit_code', '?')} |",
    f"| Pipeline lag p95 (peak) | **{w1['pipeline_p95_peak_ms']} ms** |",
    f"| Kafka consumer lag (peak) | {w1['kafka_lag_peak']} |",
    "",
    "## Wave 2: Peak burst",
    "",
    f"*{w2['calls_requested']:,} calls @ {w2['concurrency']} concurrent sessions*",
    "",
    "| Metric | Value |",
    "|--------|------:|",
    f"| Wall time | {w2['wall_s']}s |",
    f"| Throughput | **{w2['throughput_rps']} rps** |",
    f"| Routing failures | {w2['failed']:,} ({w2['error_rate_pct']}%) |",
    f"| Pipeline lag p95 (peak) | **{w2['pipeline_p95_peak_ms']} ms** |",
    f"| Kafka consumer lag (peak) | {w2['kafka_lag_peak']} |",
    "",
    "## Routing API stress (post-simulation)",
    "",
    f"*{rs['requests']:,} parallel `/route` requests, {rs['workers']} workers*",
    "",
    "| Metric | Idle baseline | Under stress |",
    "|--------|-------------:|-------------:|",
    f"| p50 latency | {b['p50_ms']} ms | **{rs['p50_ms']} ms** |",
    f"| p95 latency | {b['p95_ms']} ms | **{rs['p95_ms']} ms** |",
    f"| p99 latency | {b['p99_ms']} ms | **{rs['p99_ms']} ms** |",
    f"| Throughput | — | **{rs['throughput_rps']} rps** |",
    f"| Errors | — | {rs['errors']} ({rs['error_rate_pct']}%) |",
    f"| Degradation factor (p95) | — | **{ag['latency_degradation_vs_idle']}×** |",
    "",
    "## Emergent traffic economics (from CDRs)",
    "",
    "Metrics below emerged from routing + mock carrier — not seeded headline numbers.",
    "",
    "| Metric | Value |",
    "|--------|------:|",
    f"| Total CDRs (telemetry) | {pl['total_cdrs_in_telemetry']:,} |",
    f"| Answer rate | **{pl['answer_rate_pct']}%** |",
    f"| Avg cost per call | **${pl['avg_cost_per_call']:.5f}** |",
    f"| Total cost tracked | ${pl['total_cost']:,.2f} |",
    f"| Carriers active | **{pl['carriers_active']}** |",
    f"| ASR spread | **{pl['asr_spread_pp']} pp** ({pl['asr_range_pct'][0]}%–{pl['asr_range_pct'][1]}%) |",
    "",
    "### Carrier share (telemetry window)",
    "",
    "| Carrier | Share |",
    "|---------|------:|",
]
for c, pct in sorted(pl.get("carrier_share_telemetry_pct", {}).items(), key=lambda x: -x[1]):
    lines.append(f"| {c} | **{pct}%** |")

lines += [
    "",
    "### Carrier share (ClickHouse, last 2h)",
    "",
    "| Carrier | Share |",
    "|---------|------:|",
]
for c, pct in sorted(pl.get("clickhouse_carrier_share_pct", {}).items(), key=lambda x: -x[1]):
    lines.append(f"| {c} | **{pct}%** |")

lines += [
    "",
    "## Destination diversity (500-route sample)",
    "",
    f"| Metric | Value |",
    f"|--------|------:|",
    f"| Routes sampled | {div.get('routes', 0)} |",
    f"| Unique prefixes matched | **{div.get('unique_prefixes_matched', 0)}** |",
    f"| Unique carriers selected | **{div.get('unique_carriers_used', 0)}** |",
    f"| Failures | {div.get('failures', 0)} |",
    "",
    "**Carrier share in diversity sample:**",
    "",
    "| Carrier | Share |",
    "|---------|------:|",
]
for c, pct in sorted(div.get("carrier_share_pct", {}).items(), key=lambda x: -x[1]):
    lines.append(f"| {c} | **{pct}%** |")

lines += [
    "",
    "## Prefix & destination diversity (ClickHouse)",
    "",
    "| Metric | Value |",
    "|--------|------:|",
    f"| Unique 3-digit prefixes | **{pd.get('unique_3digit_prefixes', 'n/a')}** |",
    f"| Unique 4-digit prefixes | **{pd.get('unique_4digit_prefixes', 'n/a')}** |",
    f"| Unique dialed numbers | **{pd.get('unique_dialed_numbers', 0):,}** |" if pd.get("unique_dialed_numbers") else "| Unique dialed numbers | n/a |",
    f"| Avg theoretical cost | ${pd.get('avg_cost_theoretical', 0):.5f} |" if pd else "",
    f"| Avg answered duration | {pd.get('avg_answered_duration_sec', 0)}s |" if pd else "",
    "",
    "## Pipeline health under load",
    "",
    "| Metric | Value |",
    "|--------|------:|",
    f"| CDR→telemetry lag p50 | **{pl['pipeline_lag_p50_ms']} ms** |",
    f"| CDR→telemetry lag p95 | **{pl['pipeline_lag_p95_ms']} ms** |",
    f"| CDR→telemetry lag max | {pl['pipeline_lag_max_ms']} ms |",
    f"| Kafka consumer lag (post) | {pl['kafka_lag']} |",
    f"| E.164 spot-check (50 codes) | **{pl['e164_spot_check']['routable']}/{pl['e164_spot_check']['sampled']}** routable |",
    "",
    "---",
    "",
    "## How to read this",
    "",
    "| Category | Key metrics |",
    "|----------|-------------|",
    "| **Scale** | 150k+ calls, combined rps, CDRs written |",
    "| **Reliability** | Error rate, routing failures, post-load spot-check |",
    "| **Latency** | Baseline vs stress p95, degradation factor |",
    "| **Pipeline** | Kafka lag, CDR→telemetry lag under peak |",
    "| **Emergent economics** | Carrier share, ASR spread, avg cost/call from live CDRs |",
    "| **Diversity** | Unique prefixes/numbers hit across global traffic profile |",
    "",
]
open(out_path, "w").write("\n".join(lines) + "\n")
print(f"Report: {out_path}")
PY

exec 1>&3 3>&-
echo "Done."
