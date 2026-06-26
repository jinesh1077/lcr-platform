#!/usr/bin/env python3
"""Routing and platform checks for thorough-test.sh (prints status|name|detail lines)."""

from __future__ import annotations

import argparse
import collections
import concurrent.futures
import json
import os
import sys
import time
import urllib.request


def routing_url() -> str:
    return os.environ.get("ROUTING_URL", "http://localhost:8081/route")


def route(num: str, region: str = "GB") -> dict:
    body = json.dumps({"dialedNumber": num, "defaultRegion": region}).encode()
    req = urllib.request.Request(
        routing_url(), data=body, headers={"Content-Type": "application/json"}, method="POST"
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.loads(r.read())


def emit(status: str, name: str, detail: str) -> None:
    print(f"{status}|{name}|{detail}")


def routing_behavior() -> None:
    r0 = route("44207123456")
    stable = all(
        route("44207123456").get("matchedPrefix") == r0.get("matchedPrefix")
        and (route("44207123456").get("candidates") or [{}])[0].get("carrierId")
        == (r0.get("candidates") or [{}])[0].get("carrierId")
        for _ in range(30)
    )
    emit(
        "PASS" if stable else "FAIL",
        "Routing determinism (30 calls)",
        f"prefix={r0.get('matchedPrefix')} carrier={(r0.get('candidates') or [{}])[0].get('carrierId')}",
    )

    cases = [
        ("44207123456", "442", "clearpath"),
        ("447700900123", "4477", "zenith"),
        ("33123456789", "331", None),
        ("5511987654321", "551", "nexatel"),
    ]
    ok = 0
    for num, exp_prefix, exp_carrier in cases:
        r = route(num)
        p_ok = r.get("matchedPrefix") == exp_prefix
        c = (r.get("candidates") or [{}])[0].get("carrierId")
        if p_ok and (exp_carrier is None or c == exp_carrier):
            ok += 1
    emit("PASS" if ok == len(cases) else "FAIL", "LPM correctness battery", f"{ok}/{len(cases)} cases")

    r = route("44207123456")
    costs = [c["effectiveCost"] for c in r.get("candidates", [])]
    ordered = all(costs[i] <= costs[i + 1] for i in range(len(costs) - 1))
    emit(
        "PASS" if ordered and len(costs) >= 2 else "FAIL",
        "Candidate cost ordering",
        str(costs),
    )

    n = len(r.get("candidates", []))
    emit("PASS" if n >= 2 else "FAIL", "Multi-carrier failover depth", f"{n} candidates")


def concurrent_mixed() -> None:
    nums = ["447700900123", "44207123456", "33123456789", "4915123456789", "5511987654321"]

    def route_one(num: str) -> tuple[str | None, str | None, float]:
        body = json.dumps({"dialedNumber": num, "defaultRegion": "GB"}).encode()
        req = urllib.request.Request(
            routing_url(), data=body, headers={"Content-Type": "application/json"}, method="POST"
        )
        t0 = time.perf_counter()
        with urllib.request.urlopen(req, timeout=10) as r:
            d = json.loads(r.read())
        lat = (time.perf_counter() - t0) * 1000
        prefix = d.get("matchedPrefix")
        carrier = (d.get("candidates") or [{}])[0].get("carrierId")
        return prefix, carrier, lat

    with concurrent.futures.ThreadPoolExecutor(40) as ex:
        results = list(ex.map(lambda i: route_one(nums[i % len(nums)]), range(400)))

    errors = sum(1 for r in results if r[0] is None)
    prefixes = collections.Counter(r[0] for r in results)
    lats = sorted(r[2] for r in results)
    p95 = lats[int(len(lats) * 0.95)]
    unique = len(prefixes)
    print(f"errors={errors}|unique_prefixes={unique}|p95_ms={p95:.0f}|top={prefixes.most_common(3)}")


def platform_health() -> None:
    checks = [
        ("http://localhost:8080/health", "Ingestion"),
        ("http://localhost:8081/health", "Routing"),
        ("http://localhost:8082/health", "Telemetry"),
        ("http://localhost:8083/health", "Mock carrier"),
    ]
    up = 0
    for url, _ in checks:
        try:
            with urllib.request.urlopen(url, timeout=5) as r:
                if r.status == 200:
                    up += 1
        except Exception:
            pass
    emit(
        "PASS" if up == len(checks) else "FAIL",
        "Service health endpoints",
        f"{up}/{len(checks)} services up",
    )

    try:
        with urllib.request.urlopen("http://localhost:8080/api/overview", timeout=5) as r:
            ov = json.loads(r.read())
        buf = ov.get("trie_active_buffer", "?")
        rates = ov.get("active_rates", 0)
        carriers = ov.get("carriers", [])
        ok = buf in ("A", "B") and rates > 0 and len(carriers) >= 3
        emit(
            "PASS" if ok else "FAIL",
            "Ingestion overview",
            f"buffer={buf}, {rates} rates, {len(carriers)} carriers",
        )
    except Exception as e:
        emit("FAIL", "Ingestion overview", str(e))


def routing_economics() -> None:
    r_mobile = route("447700900123")
    r_london = route("44207123456")
    m_cost = (r_mobile.get("candidates") or [{}])[0].get("costPerMin", 0)
    l_cost = (r_london.get("candidates") or [{}])[0].get("costPerMin", 0)
    savings = round((l_cost - m_cost) / l_cost * 100, 1) if l_cost > m_cost else 0
    lpm_ok = m_cost < l_cost and r_mobile.get("matchedPrefix") == "4477"
    emit(
        "PASS" if lpm_ok else "FAIL",
        "UK LPM specificity",
        f"4477 ${m_cost:.4f} vs 442 ${l_cost:.4f} ({savings}% cheaper)",
    )

    cands = r_london.get("candidates", [])
    if len(cands) >= 2:
        p, b = cands[0]["costPerMin"], cands[1]["costPerMin"]
        prem = round((b - p) / p * 100, 1) if p > 0 else 0
        emit(
            "PASS" if prem >= 40 else "FAIL",
            "Failover cost premium",
            f"backup {prem}% more than primary (${p:.4f} → ${b:.4f})",
        )
    else:
        emit("FAIL", "Failover cost premium", "fewer than 2 candidates")

    regions = [
        ("5511987654321", "BR"),
        ("4915123456789", "DE"),
        ("33123456789", "FR"),
    ]
    costs = [(route(num, reg).get("candidates") or [{}])[0].get("costPerMin", 0) for num, reg in regions]
    lo, hi = min(costs), max(costs)
    ratio = round(hi / lo, 2) if lo > 0 else 0
    emit("PASS" if ratio >= 1.5 else "FAIL", "Regional rate spread", f"${lo:.4f} – ${hi:.4f}/min ({ratio}×)")

    try:
        r_local = route("02071234567", "GB")
        norm_ok = r_local.get("dialedNumber", "").startswith("442")
        emit(
            "PASS" if norm_ok else "FAIL",
            "E164 normalization",
            f"02071234567 → {r_local.get('dialedNumber', '?')}",
        )
    except Exception as e:
        emit("FAIL", "E164 normalization", str(e))


def post_rebuild_determinism() -> None:
    r0 = (route("44207123456").get("matchedPrefix"), (route("44207123456").get("candidates") or [{}])[0].get("carrierId"))
    ok = all(
        (
            route("44207123456").get("matchedPrefix"),
            (route("44207123456").get("candidates") or [{}])[0].get("carrierId"),
        )
        == r0
        for _ in range(10)
    )
    print("ok" if ok else "fail")


COMMANDS = {
    "routing-behavior": routing_behavior,
    "concurrent-mixed": concurrent_mixed,
    "platform-health": platform_health,
    "routing-economics": routing_economics,
    "post-rebuild-determinism": post_rebuild_determinism,
}


def main() -> int:
    parser = argparse.ArgumentParser(description="Thorough test routing/platform checks")
    parser.add_argument("command", choices=sorted(COMMANDS.keys()))
    args = parser.parse_args()
    COMMANDS[args.command]()
    return 0


if __name__ == "__main__":
    sys.exit(main())
