#!/usr/bin/env bash
# Capacity study: concurrency sweep, pipeline lag, resource usage, diminishing-returns knee.
set -euo pipefail

COMPOSE="./scripts/compose.sh"
ROUTING_URL="${ROUTING_URL:-http://localhost:8081/route}"
TELEMETRY_URL="${TELEMETRY_URL:-http://localhost:8082}"
KAFKA_CONTAINER="${KAFKA_CONTAINER:-communicationproject-kafka-1}"
CALLS_PER_LEVEL="${CALLS_PER_LEVEL:-3000}"
LAG_SLO_MS="${LAG_SLO_MS:-100}"
OUT="${1:-/tmp/lcr-capacity.md}"
CSV="/tmp/lcr-capacity-study.csv"

CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-25 50 75 100 150 200 300 500 750 1000}"

measure_kafka_lag() {
  docker exec "$KAFKA_CONTAINER" /opt/kafka/bin/kafka-consumer-groups.sh \
    --bootstrap-server localhost:9092 --describe --group telemetry-quality 2>/dev/null \
    | awk 'NR>1 && $6!="-" && $6!="" {if($6+0>m)m=$6+0} END {print m+0}' || echo 0
}

docker_cpu_pct() {
  local name=$1
  docker stats --no-stream --format '{{.CPUPerc}}' "$name" 2>/dev/null | tr -d '%' || echo 0
}

echo "concurrency,calls,success,failed,rps,wall_s,pipeline_p95_ms,kafka_lag_max,route_p95_ms,cpu_routing,cpu_telemetry,cpu_kafka" > "$CSV"

python3 - "$COMPOSE" "$ROUTING_URL" "$TELEMETRY_URL" "$CALLS_PER_LEVEL" "$LAG_SLO_MS" "$CSV" <<'PY' > /tmp/capacity-study-body.txt
import json, subprocess, sys, time, threading, urllib.request, concurrent.futures, statistics, re
from datetime import datetime

COMPOSE, ROUTING, TELEMETRY, CALLS, LAG_SLO, CSV = sys.argv[1:7]
LEVELS = [int(x) for x in __import__('os').environ.get('CONCURRENCY_LEVELS', '25 50 75 100 150 200 300 500 750 1000').split()]

def parse_dt(s):
    s = s.replace('Z', '+00:00')
    s = re.sub(r'(\.\d{6})\d+', r'\1', s)
    return datetime.fromisoformat(s)

def pipeline_p95():
    try:
        with urllib.request.urlopen(f"{TELEMETRY}/api/activity", timeout=3) as r:
            d = json.load(r)
        lags = [(parse_dt(c['received_at']) - parse_dt(c['timestamp'])).total_seconds() * 1000
                for c in d.get('recent_calls', []) if c.get('timestamp') and c.get('received_at')]
        if not lags: return 0
        lags.sort()
        return lags[int(len(lags) * 0.95)]
    except Exception:
        return -1

def route_latency_p95(samples=200, workers=50):
    nums = ["447700900123", "44207123456", "33123456789", "4915123456789"]
    def one(_):
        body = json.dumps({"dialedNumber": nums[int(time.time()*1e6) % 4], "defaultRegion": "GB"}).encode()
        req = urllib.request.Request(ROUTING, data=body, headers={"Content-Type": "application/json"}, method="POST")
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=10) as r:
            json.loads(r.read())
        return (time.perf_counter() - t0) * 1000
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as ex:
            lats = list(ex.map(one, range(samples)))
        lats.sort()
        return lats[int(len(lats) * 0.95)]
    except Exception:
        return -1

def run_sim(concurrency):
    peaks = {'kafka': 0, 'pipe': 0}
    stop = threading.Event()
    def poll():
        while not stop.is_set():
            try:
                out = subprocess.check_output([
                    'docker', 'exec', __import__('os').environ.get('KAFKA_CONTAINER', 'communicationproject-kafka-1'),
                    '/opt/kafka/bin/kafka-consumer-groups.sh', '--bootstrap-server', 'localhost:9092',
                    '--describe', '--group', 'telemetry-quality'
                ], stderr=subprocess.DEVNULL, text=True)
                for line in out.splitlines()[1:]:
                    p = line.split()
                    if len(p) >= 6 and p[5] not in ('-', ''):
                        peaks['kafka'] = max(peaks['kafka'], int(p[5]))
            except Exception:
                pass
            peaks['pipe'] = max(peaks['pipe'], pipeline_p95())
            time.sleep(0.03)
    t = threading.Thread(target=poll, daemon=True)
    t.start()
    t0 = time.perf_counter()
    out = subprocess.check_output([
        COMPOSE, '--profile', 'simulate', 'run', '--rm',
        '-e', f'SIM_CALLS={CALLS}', '-e', f'SIM_CONCURRENCY={concurrency}',
        'traffic-simulator'
    ], stderr=subprocess.STDOUT, text=True)
    wall = time.perf_counter() - t0
    time.sleep(0.5)
    stop.set()
    data = {}
    for line in out.splitlines():
        if 'simulation complete' in line:
            data = json.loads(line)
    return data, wall, peaks['kafka'], peaks['pipe']

def docker_cpu(name):
    try:
        out = subprocess.check_output(['docker', 'stats', '--no-stream', '--format', '{{.CPUPerc}}', name],
                                      stderr=subprocess.DEVNULL, text=True).strip().replace('%','')
        return float(out)
    except Exception:
        return 0.0

rows = []
print("Running capacity sweep...")
for c in LEVELS:
    print(f"  concurrency={c} ...", flush=True)
    data, wall, klag, plag = run_sim(c)
    route_p95 = route_latency_p95(min(200, c * 2), min(50, c))
    cpu_r = docker_cpu('communicationproject-routing-engine-1')
    cpu_t = docker_cpu('communicationproject-telemetry-1')
    cpu_k = docker_cpu('communicationproject-kafka-1')
    success = data.get('success', 0)
    failed = data.get('failed', 0)
    rps = success / wall if wall > 0 else 0
    rows.append({
        'concurrency': c, 'calls': data.get('total', CALLS), 'success': success, 'failed': failed,
        'rps': rps, 'wall_s': wall, 'pipeline_p95_ms': plag, 'kafka_lag_max': klag,
        'route_p95_ms': route_p95, 'cpu_routing': cpu_r, 'cpu_telemetry': cpu_t, 'cpu_kafka': cpu_k,
    })
    with open(CSV, 'a') as f:
        f.write(f"{c},{data.get('total',CALLS)},{success},{failed},{rps:.1f},{wall:.2f},{plag:.0f},{klag},{route_p95:.1f},{cpu_r:.1f},{cpu_t:.1f},{cpu_k:.1f}\n")
    time.sleep(2)

# Find knees
peak_rps = max(r['rps'] for r in rows)
sat_conc = rows[-1]['concurrency']
marginal_knee = None
lag_knee = None
for i in range(1, len(rows)):
    prev, cur = rows[i-1], rows[i]
    drps = cur['rps'] - prev['rps']
    dconc = cur['concurrency'] - prev['concurrency']
    marginal = drps / dconc if dconc else 0
    prev_marginal = (prev['rps'] - rows[i-2]['rps']) / (prev['concurrency'] - rows[i-2]['concurrency']) if i >= 2 else marginal
    # ROI bad: <5% rps gain when increasing concurrency 2x
    if prev['rps'] > 0 and cur['rps'] / prev['rps'] < 1.05 and cur['concurrency'] >= prev['concurrency'] * 1.5 and marginal_knee is None:
        marginal_knee = cur['concurrency']
    if cur['pipeline_p95_ms'] > float(LAG_SLO) and lag_knee is None:
        lag_knee = cur['concurrency']
    if cur['kafka_lag_max'] > 100 and lag_knee is None:
        lag_knee = cur['concurrency']

# Print markdown tables
print()
print("## Concurrency sweep")
print()
print(f"Calls per level: **{CALLS}** | Lag SLO tested: **{LAG_SLO} ms** (pipeline p95)")
print()
print("| Concurrent sessions | Throughput (rps) | Pipeline lag p95 | Route latency p95 | Kafka lag | Routing CPU% |")
print("|--------------------:|-----------------:|-----------------:|------------------:|----------:|-------------:|")
for r in rows:
    print(f"| {r['concurrency']} | {r['rps']:.0f} | {r['pipeline_p95_ms']:.0f} ms | {r['route_p95_ms']:.0f} ms | {r['kafka_lag_max']} | {r['cpu_routing']:.0f}% |")

print()
print("## Saturation analysis")
print()
print(f"| Metric | Value |")
print(f"|--------|-------|")
print(f"| Peak throughput | **{peak_rps:.0f} rps** |")
if marginal_knee:
    print(f"| Throughput plateau begins | ~**{marginal_knee}** concurrent sessions (<5% gain per step) |")
else:
    best = max(rows, key=lambda r: r['rps'])
    print(f"| Highest throughput observed | **{best['rps']:.0f} rps** at **{best['concurrency']}** concurrent sessions |")
if lag_knee:
    print(f"| Pipeline lag exceeds {LAG_SLO}ms SLO | ~**{lag_knee}** concurrent sessions |")
else:
    max_lag = max(r['pipeline_p95_ms'] for r in rows)
    print(f"| Pipeline lag SLO ({LAG_SLO}ms) | Not exceeded (max p95: **{max_lag:.0f} ms**) |")

# Marginal efficiency table
print()
print("## Marginal throughput (ROI per concurrency step)")
print()
print("| From → To | Δ concurrency | Δ rps | Marginal rps per slot | % gain |")
print("|-----------|--------------:|------:|----------------------:|-------:|")
for i in range(1, len(rows)):
    a, b = rows[i-1], rows[i]
    dr = b['rps'] - a['rps']
    dc = b['concurrency'] - a['concurrency']
    pct = (dr / a['rps'] * 100) if a['rps'] else 0
    m = dr / dc if dc else 0
    flag = " ⚠️ diminishing" if pct < 10 and dc >= 50 else ""
    print(f"| {a['concurrency']} → {b['concurrency']} | {dc} | {dr:+.0f} | {m:.2f} | {pct:+.1f}%{flag}")

print()
print("## Interpretation")
print()
print("- **Concurrent sessions** = parallel call flows in the simulator (route → carrier → CDR).")
print("- **Pipeline lag** = CDR publish → telemetry ingest. Stays low while consumers keep up.")
print("- **Route latency p95** = routing API under load; rises when routing-engine saturates.")
print("- **Diminishing ROI**: when % gain column drops below ~10%, more concurrency (or hardware) buys little throughput.")
print("- **Provider count (3 carriers)** limits routing showcase metrics; capacity metrics still valid for pipeline design.")
PY

# CPU limit comparison (optional second phase)
CPU_PHASE="${RUN_CPU_PHASE:-1}"
CPU_BODY=""
if [[ "$CPU_PHASE" == "1" ]]; then
  CPU_BODY=$(python3 <<'PY'
import subprocess, json, time, os

COMPOSE = "./scripts/compose.sh"
CALLS, CONC = 3000, 200
container = "communicationproject-routing-engine-1"

def run_sim():
    out = subprocess.check_output([
        COMPOSE, '--profile', 'simulate', 'run', '--rm',
        '-e', f'SIM_CALLS={CALLS}', '-e', f'SIM_CONCURRENCY={CONC}',
        'traffic-simulator'
    ], stderr=subprocess.STDOUT, text=True)
    for line in out.splitlines():
        if 'simulation complete' in line:
            d = json.loads(line)
            return d.get('success', 0), float(d.get('rps', 0))
    return 0, 0.0

def route_p95():
    import urllib.request, concurrent.futures, statistics
    ROUTING = "http://localhost:8081/route"
    nums = ["447700900123", "44207123456"]
    def one(_):
        import json
        body = json.dumps({"dialedNumber": nums[0], "defaultRegion": "GB"}).encode()
        req = urllib.request.Request(ROUTING, data=body, headers={"Content-Type":"application/json"}, method="POST")
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=10) as r:
            json.loads(r.read())
        return (time.perf_counter()-t0)*1000
    with concurrent.futures.ThreadPoolExecutor(50) as ex:
        lats = list(ex.map(one, range(200)))
    lats.sort()
    return lats[int(len(lats)*0.95)]

results = []
for cpus in [0.25, 0.5, 1.0, 2.0, 0]:
    label = "unlimited" if cpus == 0 else f"{cpus} CPU"
    try:
        if cpus > 0:
            subprocess.check_call(['docker', 'update', '--cpus', str(cpus), container], stderr=subprocess.DEVNULL)
        else:
            subprocess.check_call(['docker', 'update', '--cpus', '0', container], stderr=subprocess.DEVNULL)
        time.sleep(3)
        ok, rps = run_sim()
        rp95 = route_p95()
        results.append((label, cpus, rps, rp95))
        print(f"  {label}: {rps:.0f} rps, route p95 {rp95:.0f}ms", flush=True)
    except Exception as e:
        results.append((label, cpus, 0, 0))
    time.sleep(2)

# restore
subprocess.call(['docker', 'update', '--cpus', '0', container], stderr=subprocess.DEVNULL)

print()
print("## Hardware ROI — routing-engine CPU limits")
print()
print(f"Fixed load: **{CALLS}** calls @ **{CONC}** concurrent sessions")
print()
print("| CPU limit (routing-engine) | Throughput (rps) | Route latency p95 |")
print("|----------------------------|-----------------:|------------------:|")
base_rps = results[-1][2] if results else 1
for label, cpus, rps, rp95 in results:
    gain = ((rps/base_rps)-1)*100 if base_rps else 0
    note = ""
    if cpus > 0 and results:
        prev = [r for r in results if r[1] < cpus and r[1] > 0]
        if prev:
            pg = (rps - prev[-1][2]) / prev[-1][2] * 100 if prev[-1][2] else 0
            if pg < 15 and cpus >= 1:
                note = " ← diminishing"
    print(f"| {label} | {rps:.0f} | {rp95:.0f} ms |")

if len(results) >= 2:
    unlimited = results[-1][2]
    half = next((r[2] for r in results if r[1] == 0.5), 0)
    if unlimited > 0 and half > 0:
        print()
        print(f"Halving CPU (0.5 vs unlimited): throughput **{half/unlimited*100:.0f}%** of unlimited.")
    # find knee: doubling CPU adds <15%
    for i in range(1, len(results)-1):
        a, b = results[i-1], results[i]
        if a[2] > 0 and (b[2]-a[2])/a[2] < 0.15 and b[1] >= 1.0:
            print(f"CPU ROI diminishes above **{b[0]}** — doubling resources adds <15% throughput.")
            break
PY
)
fi

{
  echo "# LCR Platform — Capacity Study"
  echo ""
  echo "**Generated:** $(date -u +%Y-%m-%d)"
  echo "**Environment:** Docker Compose (Colima)"
  echo ""
  echo "Finds where more concurrency or CPU stops improving throughput (diminishing ROI)."
  echo ""
  echo "Reproduce: \`./scripts/benchmark/capacity-study.sh\`"
  echo ""
  cat /tmp/capacity-study-body.txt
  if [[ -n "$CPU_BODY" ]]; then
    echo ""
    echo "$CPU_BODY"
  fi
} > "$OUT"

echo "Wrote $OUT"
echo "CSV: $CSV"
