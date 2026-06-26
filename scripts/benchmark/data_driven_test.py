#!/usr/bin/env python3
"""Data-driven tests: E.164 coverage, traffic profile, simulation metrics."""

from __future__ import annotations

import argparse
import json
import os
import random
import statistics
import subprocess
import sys
import time
import urllib.request
from collections import Counter
from pathlib import Path

REPO = Path(__file__).resolve().parents[2]


def route(routing_url: str, num: str, region: str = "GB") -> tuple[int, dict]:
    body = json.dumps({"dialedNumber": num, "defaultRegion": region}).encode()
    req = urllib.request.Request(
        routing_url, data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=15) as r:
        return r.status, json.loads(r.read())


def test_number(prefix: str) -> str:
    h = sum(ord(c) for c in prefix) % 10000000
    return prefix + str(1000000 + h)[: max(4, 12 - len(prefix))]


def run_tests() -> dict:
    routing = os.environ.get("ROUTING_URL", "http://localhost:8081/route")
    ingestion = os.environ.get("INGESTION_URL", "http://localhost:8080")
    telemetry = os.environ.get("TELEMETRY_URL", "http://localhost:8082")
    compose = str(REPO / "scripts/compose.sh")
    sim_calls = int(os.environ.get("SIM_CALLS", "3000"))

    stats_path = REPO / "scripts/seed/generated/rate-deck-stats.json"
    deck = json.loads(stats_path.read_text()) if stats_path.exists() else {}

    e164_rows = json.loads((REPO / "data/lcr-dataset.json").read_text())["e164"]
    random.seed(42)

    coverage_ok = coverage_fail = 0
    fail_zones: list[str] = []
    for row in e164_rows:
        num = test_number(row["prefix"])
        region = "GB" if row["prefix"].startswith("44") else "US"
        try:
            st, d = route(routing, num, region)
            if st == 200 and d.get("candidates"):
                coverage_ok += 1
            else:
                coverage_fail += 1
                fail_zones.append(row["prefix"])
        except Exception:
            coverage_fail += 1
            fail_zones.append(row["prefix"])

    profile_path = REPO / "scripts/seed/generated/traffic-profile.json"
    dests = json.loads(profile_path.read_text()).get("destinations", [])

    prof_ok = prof_fail = 0
    prefix_lens: list[int] = []
    carrier_counts: Counter = Counter()
    failover_gaps: list[float] = []
    ordering_bad = 0

    for d in dests:
        try:
            st, r = route(routing, d["dialed_number"], d.get("default_region", "GB"))
            if st != 200 or not r.get("candidates"):
                prof_fail += 1
                continue
            prof_ok += 1
            prefix_lens.append(len(r.get("matchedPrefix", "")))
            carrier_counts[r["candidates"][0]["carrierId"]] += 1
            costs = [c["effectiveCost"] for c in r["candidates"]]
            if not all(costs[i] <= costs[i + 1] for i in range(len(costs) - 1)):
                ordering_bad += 1
            if len(costs) >= 2 and costs[0] > 0:
                failover_gaps.append((costs[1] - costs[0]) / costs[0] * 100)
        except Exception:
            prof_fail += 1

    lpm_cases = [
        ("447700900123", "4477"),
        ("44207123456", "442"),
        ("440207123456", "442"),
    ]
    lpm_pass = sum(1 for num, exp in lpm_cases if route(routing, num)[1].get("matchedPrefix") == exp)

    try:
        with urllib.request.urlopen(f"{ingestion}/api/overview", timeout=5) as r:
            overview = json.loads(r.read())
    except Exception:
        overview = {}

    subprocess.run(
        [
            compose, "--profile", "simulate", "run", "--rm",
            "-e", f"SIM_CALLS={sim_calls}", "-e", "SIM_CONCURRENCY=50",
            "traffic-simulator",
        ],
        capture_output=True,
        cwd=str(REPO),
    )
    time.sleep(2)

    try:
        with urllib.request.urlopen(f"{telemetry}/api/activity", timeout=5) as r:
            activity = json.loads(r.read())
        with urllib.request.urlopen(f"{telemetry}/api/stats", timeout=5) as r:
            tstats = json.loads(r.read())
    except Exception:
        activity, tstats = {}, {}

    summary = activity.get("summary", {})
    carriers = tstats.get("carriers", [])
    active = [c for c in carriers if c.get("attempts", 0) > 0]
    asrs = [round(c["asr"] * 100, 1) for c in active]
    sim_shares = {}
    tot = sum(c.get("attempts", 0) for c in active)
    for c in active:
        sim_shares[c["carrier_id"]] = round(100 * c["attempts"] / max(tot, 1), 1)

    return {
        "deck": deck,
        "e164_coverage": {
            "country_codes_tested": len(e164_rows),
            "routable": coverage_ok,
            "failed": coverage_fail,
            "coverage_pct": round(100 * coverage_ok / max(len(e164_rows), 1), 1),
            "sample_failures": fail_zones[:8],
        },
        "traffic_profile": {
            "destinations": len(dests),
            "routable": prof_ok,
            "failed": prof_fail,
            "success_pct": round(100 * prof_ok / max(len(dests), 1), 1),
            "avg_prefix_digits": round(statistics.mean(prefix_lens), 2) if prefix_lens else 0,
            "carrier_share_pct": dict(carrier_counts),
            "cost_ordering_violations": ordering_bad,
            "median_failover_gap_pct": round(statistics.median(failover_gaps), 1) if failover_gaps else 0,
            "destinations_with_backup_pct": round(100 * len(failover_gaps) / max(prof_ok, 1), 1),
        },
        "lpm": {"cases": len(lpm_cases), "passed": lpm_pass},
        "overview": {
            "active_rates": overview.get("active_rates", 0),
            "carriers": overview.get("carriers", []),
            "trie_buffer": overview.get("trie_active_buffer", "?"),
        },
        "simulation": {
            "calls": sim_calls,
            "total_cdrs": summary.get("total_calls", 0),
            "answer_rate_pct": round(summary.get("answer_rate", 0) * 100, 1),
            "avg_cost_per_call": round(
                summary.get("total_cost", 0) / max(summary.get("total_calls", 1), 1), 4
            ),
            "carrier_share_pct": sim_shares,
            "asr_spread_pp": round(max(asrs) - min(asrs), 1) if len(asrs) >= 2 else 0,
            "carriers_observed": len(active),
        },
    }


def write_report(data: dict, out_path: Path) -> None:
    deck = data.get("deck", {})
    e164 = data["e164_coverage"]
    tp = data["traffic_profile"]
    lpm = data["lpm"]
    ov = data["overview"]
    sim = data["simulation"]

    lines = [
        "# LCR Platform — Data-Driven Test Report",
        "",
        f"**Sources:** ITU E.164 ({deck.get('e164_country_codes', 223)} codes) + "
        f"**{deck.get('mcc_operators', 0)}** MCC/MNC operators "
        f"({deck.get('mcc_countries', 0)} countries) + 5-carrier rate deck",
        "",
        "| Rate rows | Prefixes | Multi-carrier | MCC→E.164 mapped | Competition |",
        "|----------:|---------:|--------------:|-------------------:|------------:|",
        f"| {deck.get('rate_rows', '?')} | {deck.get('distinct_prefixes', '?')} | "
        f"{deck.get('prefixes_with_2plus_carriers', '?')} | {deck.get('mcc_mapped_to_e164', '?')} | "
        f"**{deck.get('competition_pct', '?')}%** |",
        "",
        "## 1. E.164 country code coverage",
        "",
        f"Routed one test number per ITU country code ({e164['country_codes_tested']} destinations).",
        "",
        "| Metric | Value |",
        "|--------|------:|",
        f"| Routable | **{e164['routable']}** / {e164['country_codes_tested']} |",
        f"| Coverage | **{e164['coverage_pct']}%** |",
        f"| Failures | {e164['failed']} |",
        "",
        f"## 2. Traffic profile ({tp['destinations']} weighted destinations)",
        "",
        "| Metric | Value |",
        "|--------|------:|",
        f"| Routing success | **{tp['success_pct']}%** ({tp['routable']}/{tp['destinations']}) |",
        f"| Avg prefix digits matched | **{tp['avg_prefix_digits']}** |",
        f"| Destinations with backup carrier | **{tp['destinations_with_backup_pct']}%** |",
        f"| Median failover cost gap | **{tp['median_failover_gap_pct']}%** |",
        f"| Cost ordering violations | {tp['cost_ordering_violations']} |",
        "",
        "**Carrier share (profile sample):**",
        "",
        "| Carrier | Share |",
        "|---------|------:|",
    ]
    for c, pct in sorted(tp.get("carrier_share_pct", {}).items(), key=lambda x: -x[1]):
        lines.append(f"| {c} | **{pct}** calls |")

    lines += [
        "",
        "## 3. LPM nested prefixes",
        "",
        f"**{lpm['passed']}** / {lpm['cases']} UK specificity cases passed",
        "",
        "## 4. Live platform state",
        "",
        "| Active rates | Carriers | Trie buffer |",
        "|-------------:|----------|-------------|",
        f"| {ov.get('active_rates', '?')} | {len(ov.get('carriers', []))} | {ov.get('trie_buffer', '?')} |",
        "",
        f"## 5. Simulation ({sim['calls']} calls, global traffic profile)",
        "",
        "| Metric | Value |",
        "|--------|------:|",
        f"| Answer rate | **{sim['answer_rate_pct']}%** |",
        f"| Avg cost / call | **${sim['avg_cost_per_call']:.4f}** |",
        f"| Carriers observed | **{sim['carriers_observed']}** |",
        f"| ASR spread | **{sim['asr_spread_pp']} pp** |",
        "",
        "**Carrier attempt share (emergent):**",
        "",
        "| Carrier | Share |",
        "|---------|------:|",
    ]
    for c, pct in sorted(sim.get("carrier_share_pct", {}).items(), key=lambda x: -x[1]):
        lines.append(f"| {c} | **{pct}%** |")

    lines += [
        "",
        "Reproduce:",
        "```bash",
        "make seed",
        "make data-driven-test",
        "```",
        "",
        "Data: `data/lcr-dataset.json` (ITU E.164 + MCC/MNC operators)",
        "",
    ]
    out_path.write_text("\n".join(lines) + "\n")


def main() -> int:
    parser = argparse.ArgumentParser(description="Run data-driven LCR tests")
    parser.add_argument("--report", type=Path, help="Write markdown report to this path")
    parser.add_argument("--json", type=Path, help="Write JSON results to this path")
    args = parser.parse_args()

    data = run_tests()
    if args.json:
        args.json.write_text(json.dumps(data, indent=2) + "\n")
    if args.report:
        write_report(data, args.report)
        print(f"Report: {args.report}")
    else:
        print(json.dumps(data, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
