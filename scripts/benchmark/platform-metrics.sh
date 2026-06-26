#!/usr/bin/env bash
# Measures non-arbitrary platform metrics: invariants, latency, throughput, pipeline timing.
# Does NOT report seed-file dollar amounts as business outcomes.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
export REPO_ROOT
export API_KEY REDIS_CONTAINER CH_CONTAINER
COMPOSE="./scripts/compose.sh"
API_KEY="${API_KEY:-local-upload-key}"
INGESTION="${INGESTION_URL:-http://localhost:8080}"
ROUTING="${ROUTING_URL:-http://localhost:8081/route}"
ROUTING_BASE="${ROUTING%/route}"
TELEMETRY="${TELEMETRY_URL:-http://localhost:8082}"
REDIS_CONTAINER="${REDIS_CONTAINER:-communicationproject-redis-1}"
CH_CONTAINER="${CH_CONTAINER:-communicationproject-clickhouse-1}"
OUT="${1:-/tmp/lcr-platform.md}"

LATENCY_SAMPLES="${LATENCY_SAMPLES:-500}"
CONCURRENT_WORKERS="${CONCURRENT_WORKERS:-50}"
CONCURRENT_REQUESTS="${CONCURRENT_REQUESTS:-1000}"
LOAD_REQUESTS="${LOAD_REQUESTS:-400}"

exec 3>&1
exec 1> >(tee /tmp/platform-metrics-run.log)
exec 2>&1

echo "=== LCR Platform Metrics (measured) ==="
echo "Started: $(date -u +%Y-%m-%dT%H:%M:%SZ)"
echo

RESULTS=$(python3 <<'PY'
import json, os, statistics, subprocess, tempfile, time, urllib.request, concurrent.futures

INGESTION = os.environ.get("INGESTION_URL", "http://localhost:8080")
ROUTING = os.environ.get("ROUTING_URL", "http://localhost:8081/route")
TELEMETRY = os.environ.get("TELEMETRY_URL", "http://localhost:8082")
API_KEY = os.environ.get("API_KEY", "local-upload-key")
LATENCY_SAMPLES = int(os.environ.get("LATENCY_SAMPLES", "500"))
CONCURRENT_WORKERS = int(os.environ.get("CONCURRENT_WORKERS", "50"))
CONCURRENT_REQUESTS = int(os.environ.get("CONCURRENT_REQUESTS", "1000"))
LOAD_REQUESTS = int(os.environ.get("LOAD_REQUESTS", "400"))

def post_json(url, body, headers=None):
    h = {"Content-Type": "application/json"}
    if headers:
        h.update(headers)
    req = urllib.request.Request(url, data=json.dumps(body).encode(), headers=h, method="POST")
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status, json.loads(r.read())

def route(num, region="GB"):
    _, d = post_json(ROUTING, {"dialedNumber": num, "defaultRegion": region})
    return d

def route_ms(num):
    body = json.dumps({"dialedNumber": num, "defaultRegion": "GB"}).encode()
    req = urllib.request.Request(ROUTING, data=body, headers={"Content-Type": "application/json"}, method="POST")
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=15) as r:
        json.loads(r.read())
    return (time.perf_counter() - t0) * 1000

out = {}

# --- 1. Routing invariants (property tests, not seed dollar amounts) ---
numbers = [
    "447700900123", "44207123456", "33123456789", "4915123456789",
    "5511987654321", "81312345678", "34612345678", "393331234567",
    "12025550100", "61891234567",
]
trials = max(LATENCY_SAMPLES, 200)

prefix_subset_ok = 0
ordering_ok = 0
penalty_formula_ok = 0
failover_ok = 0
errors = 0

for i in range(trials):
    num = numbers[i % len(numbers)]
    try:
        d = route(num)
    except Exception:
        errors += 1
        continue
    dialed = d.get("dialedNumber", "")
    prefix = d.get("matchedPrefix", "")
    if dialed.startswith(prefix):
        prefix_subset_ok += 1
    cands = d.get("candidates", [])
    costs = [c.get("effectiveCost", 0) for c in cands]
    if all(costs[j] <= costs[j + 1] for j in range(len(costs) - 1)):
        ordering_ok += 1
    if len(cands) >= 2:
        failover_ok += 1

# Health penalty formula: set known penalty, verify effective = base * (1+p)
penalty_trials = 50
penalty_ok = 0
for p in [0.0, 0.1, 0.25, 0.5]:
    subprocess.run(
        ["docker", "exec", os.environ.get("REDIS_CONTAINER", "communicationproject-redis-1"),
         "redis-cli", "SET", "health:clearpath", str(p)],
        capture_output=True,
    )
    time.sleep(0.05)
    for _ in range(penalty_trials // 4):
        try:
            d = route("44207123456")
            for c in d.get("candidates", []):
                if c.get("carrierId") == "clearpath":
                    base = c.get("costPerMin", 0)
                    eff = c.get("effectiveCost", 0)
                    pen = c.get("healthPenalty", 0)
                    if base > 0 and abs(eff - base * (1 + pen)) < 1e-9 and abs(pen - p) < 1e-6:
                        penalty_ok += 1
                    break
        except Exception:
            pass
subprocess.run(
    ["docker", "exec", os.environ.get("REDIS_CONTAINER", "communicationproject-redis-1"),
     "redis-cli", "DEL", "health:clearpath"],
    capture_output=True,
)

# Blocklist: blocked carrier never appears
subprocess.run(
    ["docker", "exec", os.environ.get("REDIS_CONTAINER", "communicationproject-redis-1"),
     "redis-cli", "SET", "blocklist:clearpath", "1", "EX", "120"],
    capture_output=True,
)
time.sleep(0.1)
block_trials = 100
block_ok = 0
for _ in range(block_trials):
    try:
        d = route("44207123456")
        ids = [c["carrierId"] for c in d.get("candidates", [])]
        if "clearpath" not in ids:
            block_ok += 1
    except Exception:
        pass
subprocess.run(
    ["docker", "exec", os.environ.get("REDIS_CONTAINER", "communicationproject-redis-1"),
     "redis-cli", "DEL", "blocklist:clearpath"],
    capture_output=True,
)

# Determinism
det_trials = 50
d0 = route("44207123456")
key0 = (d0.get("matchedPrefix"), tuple((c["carrierId"], c["effectiveCost"]) for c in d0.get("candidates", [])))
det_ok = 0
for _ in range(det_trials):
    d = route("44207123456")
    key = (d.get("matchedPrefix"), tuple((c["carrierId"], c["effectiveCost"]) for c in d.get("candidates", [])))
    if key == key0:
        det_ok += 1

# LPM longest-prefix: verify more-specific prefix wins over shorter (existing rate table)
lpm_cases = [
    ("447700900123", "4477"),  # must not stop at 447 or 44
    ("44207123456", "442"),    # must not stop at 44
]
lpm_pass = 0
lpm_detail_parts = []
for num, expect in lpm_cases:
    try:
        d = route(num, "GB")
        got = d.get("matchedPrefix", "")
        ok = got == expect
        if ok:
            lpm_pass += 1
        lpm_detail_parts.append(f"{num}→{got}")
    except Exception as e:
        lpm_detail_parts.append(f"{num}:error")
lpm_ok = lpm_pass == len(lpm_cases)
lpm_detail = "; ".join(lpm_detail_parts)

valid = trials - errors
out["invariants"] = {
    "requests": trials,
    "errors": errors,
    "prefix_is_subset_of_number_pct": round(100 * prefix_subset_ok / max(valid, 1), 2),
    "cost_ordering_pct": round(100 * ordering_ok / max(valid, 1), 2),
    "failover_depth_pct": round(100 * failover_ok / max(valid, 1), 2),
    "health_penalty_formula_pct": round(100 * penalty_ok / max(penalty_trials, 1), 2),
    "blocklist_exclusion_pct": round(100 * block_ok / block_trials, 2),
    "determinism_pct": round(100 * det_ok / det_trials, 2),
    "lpm_longest_prefix": lpm_ok,
    "lpm_pass": lpm_pass,
    "lpm_cases": len(lpm_cases),
    "lpm_detail": lpm_detail,
}

# --- 2. Latency (HTTP measured) ---
serial = [route_ms(numbers[i % len(numbers)]) for i in range(LATENCY_SAMPLES)]
serial.sort()

def pct(arr, p):
    return arr[min(int(len(arr) * p / 100), len(arr) - 1)]

def route_one(_):
    return route_ms(numbers[int(time.time() * 1e6) % len(numbers)])

with concurrent.futures.ThreadPoolExecutor(max_workers=CONCURRENT_WORKERS) as ex:
    concurrent_lats = sorted(ex.map(route_one, range(CONCURRENT_REQUESTS)))

out["latency_ms"] = {
    "serial_samples": LATENCY_SAMPLES,
    "serial_p50": round(pct(serial, 50), 2),
    "serial_p95": round(pct(serial, 95), 2),
    "serial_p99": round(pct(serial, 99), 2),
    "serial_max": round(max(serial), 2),
    "concurrent_samples": CONCURRENT_REQUESTS,
    "concurrent_workers": CONCURRENT_WORKERS,
    "concurrent_p50": round(pct(concurrent_lats, 50), 2),
    "concurrent_p95": round(pct(concurrent_lats, 95), 2),
    "concurrent_p99": round(pct(concurrent_lats, 99), 2),
    "concurrent_max": round(max(concurrent_lats), 2),
    "serial_throughput_rps": round(LATENCY_SAMPLES / (sum(serial) / 1000), 0),
}

# --- 3. Concurrent mixed load (error rate) ---
def mixed_route(i):
    nums = ["447700900123", "44207123456", "33123456789", "4915123456789", "5511987654321"]
    try:
        return route_ms(nums[i % len(nums)]), None
    except Exception as e:
        return None, str(e)

with concurrent.futures.ThreadPoolExecutor(max_workers=40) as ex:
    mixed = list(ex.map(mixed_route, range(LOAD_REQUESTS)))
mixed_errors = sum(1 for _, e in mixed if e)
mixed_lats = sorted(x for x, e in mixed if x is not None)

out["concurrent_load"] = {
    "requests": LOAD_REQUESTS,
    "errors": mixed_errors,
    "error_rate_pct": round(100 * mixed_errors / LOAD_REQUESTS, 2),
    "p95_ms": round(pct(mixed_lats, 95), 2) if mixed_lats else 0,
}

# --- 4. Trie rebuild duration ---
rebuild_ms = -1
try:
    t0 = time.perf_counter()
    req = urllib.request.Request(
        f"{INGESTION}/admin/trie/rebuild",
        headers={"X-API-Key": API_KEY},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=60):
        pass
    rebuild_ms = round((time.perf_counter() - t0) * 1000, 1)
except Exception:
    pass
out["trie_rebuild_ms"] = rebuild_ms

# --- 5. Pipeline lag (CDR timestamp → telemetry received) ---
import re
from datetime import datetime

def parse_dt(s):
    s = s.replace("Z", "+00:00")
    s = re.sub(r"(\.\d{6})\d+", r"\1", s)
    return datetime.fromisoformat(s)

lags = []
try:
    with urllib.request.urlopen(f"{TELEMETRY}/api/activity", timeout=5) as r:
        activity = json.loads(r.read())
    for c in activity.get("recent_calls", []):
        if c.get("timestamp") and c.get("received_at"):
            lags.append((parse_dt(c["received_at"]) - parse_dt(c["timestamp"])).total_seconds() * 1000)
except Exception:
    pass
lags.sort()
out["pipeline_lag_ms"] = {
    "samples": len(lags),
    "p50": round(pct(lags, 50), 2) if lags else 0,
    "p95": round(pct(lags, 95), 2) if lags else 0,
    "max": round(max(lags), 2) if lags else 0,
}

# --- 6. Platform structure (counted, not invented) ---
overview = {}
try:
    with urllib.request.urlopen(f"{INGESTION}/api/overview", timeout=5) as r:
        overview = json.loads(r.read())
except Exception:
    pass
out["structure"] = {
    "active_rate_rows": overview.get("active_rates", 0),
    "carriers_in_rate_table": len(overview.get("carriers", [])),
    "trie_buffer": overview.get("trie_active_buffer", "?"),
}

# --- 7. Security behavior (HTTP codes) ---
def http_code(url, method="GET", data=None, headers=None):
    h = headers or {}
    req = urllib.request.Request(url, data=data, headers=h, method=method)
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            return r.status
    except urllib.error.HTTPError as e:
        return e.code

out["security"] = {
    "rebuild_without_key": http_code(f"{INGESTION}/admin/trie/rebuild", method="POST"),
    "invalid_number": http_code(ROUTING, method="POST", data=json.dumps({"dialedNumber": "not-a-phone", "defaultRegion": "GB"}).encode(), headers={"Content-Type": "application/json"}),
}

# --- 8. Invoice auditor detection (binary: does it flag known mismatch?) ---
inv_name = f"probe-{int(time.time())}.csv"
invoice_path = os.path.join(os.environ["REPO_ROOT"], "scripts/seed/invoice-overcharge.csv")
try:
    with open(invoice_path, "rb") as f:
        invoice_data = f.read()
    req = urllib.request.Request(
        f"{INGESTION}/invoices/upload?carrier_id=zenith&file_name={inv_name}",
        data=invoice_data,
        headers={"X-API-Key": API_KEY, "Content-Type": "text/csv"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=15):
        pass
    audit = subprocess.run(
        [os.path.join(os.environ["REPO_ROOT"], "scripts/compose.sh"), "--profile", "audit", "run", "--rm", "invoice-auditor"],
        capture_output=True,
        text=True,
        cwd=os.environ["REPO_ROOT"],
    )
    audit_text = audit.stdout + audit.stderr
    flags = -1
    start = audit_text.find("{")
    if start >= 0:
        depth = 0
        for i in range(start, len(audit_text)):
            if audit_text[i] == "{":
                depth += 1
            elif audit_text[i] == "}":
                depth -= 1
                if depth == 0:
                    try:
                        flags = json.loads(audit_text[start : i + 1]).get("flags", -1)
                    except json.JSONDecodeError:
                        pass
                    break
    out["invoice_audit"] = {
        "flags_on_probe": flags,
        "detected_mismatch": flags > 0,
        "auditor_exit": audit.returncode,
    }
except Exception as e:
    out["invoice_audit"] = {"flags_on_probe": -1, "detected_mismatch": False, "error": str(e)}

print(json.dumps(out, indent=2))
PY
)

echo "$RESULTS" | python3 -c "import json,sys; json.load(sys.stdin)" >/dev/null

# Write markdown report
python3 - "$OUT" "$RESULTS" <<'PY'
import json, sys
from datetime import datetime, timezone

out_path, data = sys.argv[1], json.loads(sys.argv[2])
inv = data["invariants"]
lat = data["latency_ms"]
load = data["concurrent_load"]
lag = data["pipeline_lag_ms"]
struct = data["structure"]
sec = data["security"]
audit = data["invoice_audit"]
now = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

lines = [
    "# LCR Platform — Measured Metrics",
    "",
    f"**Generated:** {now}",
    "",
    "These numbers are **measured from system behavior** (latency, pass rates, error rates, timing).",
    "They do **not** claim production cost savings — seed rate files are only used as test inputs where noted.",
    "",
    "Reproduce: `./scripts/benchmark/platform-metrics.sh`",
    "",
    "---",
    "",
    "## 1. Routing rule compliance (invariant pass rates)",
    "",
    "Property tests over live `/route` responses. A pass rate of **100%** means the rule holds on every trial.",
    "",
    "| Rule | Trials | Pass rate |",
    "|------|-------:|----------:|",
    f"| Matched prefix is a prefix of the dialed number | {inv['requests'] - inv['errors']} | **{inv['prefix_is_subset_of_number_pct']}%** |",
    f"| Candidates sorted by ascending effective cost | {inv['requests'] - inv['errors']} | **{inv['cost_ordering_pct']}%** |",
    f"| Routes with ≥2 failover candidates | {inv['requests'] - inv['errors']} | **{inv['failover_depth_pct']}%** |",
    f"| `effectiveCost = costPerMin × (1 + healthPenalty)` | 50 | **{inv['health_penalty_formula_pct']}%** |",
    f"| Blocklisted carrier absent from all candidates | 100 | **{inv['blocklist_exclusion_pct']}%** |",
    f"| Identical input → identical output | 50 | **{inv['determinism_pct']}%** |",
    f"| Longest-prefix match (specificity over shorter prefixes) | {inv.get('lpm_cases', 2)} | **{round(100 * inv.get('lpm_pass', 0) / max(inv.get('lpm_cases', 2), 1))}%** ({inv['lpm_detail']}) |",
    "",
    f"Routing errors during invariant sweep: **{inv['errors']}** / {inv['requests']}",
    "",
    "## 2. Routing latency (HTTP-measured)",
    "",
    f"Serial: **{lat['serial_samples']}** requests, one at a time.",
    "",
    "| Percentile | Latency |",
    "|------------|--------:|",
    f"| p50 | **{lat['serial_p50']} ms** |",
    f"| p95 | **{lat['serial_p95']} ms** |",
    f"| p99 | **{lat['serial_p99']} ms** |",
    f"| max | {lat['serial_max']} ms |",
    f"| Serial throughput | **{lat['serial_throughput_rps']:.0f} req/s** |",
    "",
    f"Concurrent: **{lat['concurrent_samples']}** requests, **{lat['concurrent_workers']}** workers.",
    "",
    "| Percentile | Latency |",
    "|------------|--------:|",
    f"| p50 | **{lat['concurrent_p50']} ms** |",
    f"| p95 | **{lat['concurrent_p95']} ms** |",
    f"| p99 | **{lat['concurrent_p99']} ms** |",
    f"| max | {lat['concurrent_max']} ms |",
    "",
    "## 3. Concurrent routing reliability",
    "",
    f"| Metric | Value |",
    f"|--------|------:|",
    f"| Parallel requests | {load['requests']} |",
    f"| Errors | **{load['errors']}** |",
    f"| Error rate | **{load['error_rate_pct']}%** |",
    f"| p95 latency | **{load['p95_ms']} ms** |",
    "",
    "## 4. Pipeline timing",
    "",
    "| Metric | Value |",
    "|--------|------:|",
    f"| Trie rebuild (API round-trip) | **{data['trie_rebuild_ms']} ms** |",
    f"| CDR → telemetry lag p50 ({lag['samples']} samples) | **{lag['p50']} ms** |",
    f"| CDR → telemetry lag p95 | **{lag['p95']} ms** |",
    f"| CDR → telemetry lag max | {lag['max']} ms |",
    "",
    "Lag = `received_at − call_timestamp` from telemetry activity feed.",
    "",
    "## 5. Platform structure (counted from live state)",
    "",
    "| Metric | Value |",
    "|--------|------:|",
    f"| Active rate rows loaded | {struct['active_rate_rows']} |",
    f"| Carriers in rate table | {struct['carriers_in_rate_table']} |",
    f"| Trie active buffer | {struct['trie_buffer']} |",
    "",
    "## 6. Security & validation (HTTP behavior)",
    "",
    "| Check | HTTP status |",
    "|-------|------------:|",
    f"| Trie rebuild without API key | **{sec['rebuild_without_key']}** |",
    f"| Route with invalid number | **{sec['invalid_number']}** |",
    "",
    "## 7. Invoice audit (detection, not dollar claims)",
    "",
    "A probe invoice with deliberately wrong costs was uploaded. Metric: did the auditor flag it?",
    "",
    f"| Metric | Value |",
    f"|--------|-------|",
    f"| Mismatch lines flagged | **{audit.get('flags_on_probe', 'n/a')}** |",
    f"| Detection worked | **{'yes' if audit.get('detected_mismatch') else 'no'}** |",
    "",
    "---",
    "",
    "## What these metrics mean",
    "",
    "| Type | Examples above | Tied to seed CSV prices? |",
    "|------|----------------|-------------------------|",
    "| **Invariant pass rates** | 100% cost ordering, blocklist exclusion | No — tests rules |",
    "| **Latency / throughput** | p95 ms, req/s, rebuild ms | No — measured |",
    "| **Pipeline lag** | CDR → telemetry ms | No — measured |",
    "| **Error rate** | 0% under parallel load | No — measured |",
    "| **Structure counts** | rate rows, carriers | No — counted |",
    "| **Invoice detection** | flags on probe | No — binary proof |",
    "",
    "Reproduce: `make platform-metrics` or `make report` for full metrics.",
    "",
]
open(out_path, "w").write("\n".join(lines) + "\n")
print(f"Report written: {out_path}")
PY

exec 1>&3 3>&-
echo "Done."
