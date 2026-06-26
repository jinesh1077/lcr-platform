#!/usr/bin/env python3
"""Build weighted traffic profile from E.164 destinations (global wholesaler mix)."""

import hashlib
import json
import random
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "scripts" / "seed" / "generated"

sys.path.insert(0, str(ROOT / "scripts" / "data"))
from dataset import e164_countries

# Plausible international wholesale traffic weights by zone (not uniform)
ZONE_WEIGHT = {
    "uk": 0.22,
    "eu": 0.28,
    "nam": 0.14,
    "apac": 0.16,
    "latam": 0.08,
    "africa": 0.06,
    "mena": 0.04,
    "oceania": 0.02,
    "default": 0.05,
}


def zone_for_prefix(prefix: str) -> str:
    if prefix.startswith("44"):
        return "uk"
    if prefix.startswith("1"):
        return "nam"
    if prefix.startswith(("55", "52", "54", "56", "57", "58")):
        return "latam"
    if prefix.startswith(("27", "20")) or (len(prefix) >= 2 and prefix[:2] in tuple(f"{x}" for x in range(20, 30))):
        return "africa"
    if prefix.startswith(("81", "82", "84", "86", "91", "65", "60", "66")):
        return "apac"
    if prefix.startswith(("61", "64")):
        return "oceania"
    if prefix.startswith(("971", "966", "972", "90", "98")):
        return "mena"
    if prefix.startswith(("33", "34", "39", "41", "43", "45", "46", "47", "48", "49", "31", "32", "30", "36")):
        return "eu"
    return "default"


def sample_national_number(prefix: str) -> str:
    """E.164-like test number: prefix + deterministic subscriber digits."""
    h = hashlib.md5(prefix.encode()).hexdigest()
    extra_len = 10 - len(prefix) if len(prefix) < 10 else 4
    extra_len = max(4, min(8, extra_len))
    digits = "".join(str(int(h[i], 16) % 10) for i in range(extra_len))
    return prefix + digits


def main() -> None:
    by_zone: dict[str, list] = {z: [] for z in ZONE_WEIGHT}

    for row in e164_countries():
        p, country = row["prefix"], row["country"]
        z = zone_for_prefix(p)
        by_zone.setdefault(z, []).append({
            "dialed_number": sample_national_number(p),
            "default_region": "GB" if p.startswith("44") else "US",
            "prefix": p,
            "country": country,
            "zone": z,
        })

    # Nested high-traffic prefixes
    for sub, country, zone in [
        ("447700900123", "United Kingdom", "uk"),
        ("44207123456", "United Kingdom", "uk"),
        ("33123456789", "France", "eu"),
        ("4915123456789", "Germany", "eu"),
        ("12025550100", "United States", "nam"),
        ("5511987654321", "Brazil", "latam"),
        ("81312345678", "Japan", "apac"),
        ("27123456789", "South Africa", "africa"),
    ]:
        by_zone[zone].insert(0, {
            "dialed_number": sub,
            "default_region": "GB" if sub.startswith("44") else "US",
            "prefix": sub[:3],
            "country": country,
            "zone": zone,
        })

    profile = []
    for zone, weight in ZONE_WEIGHT.items():
        pool = by_zone.get(zone, [])
        if not pool:
            continue
        # Include up to 25 destinations per zone, weighted
        n = min(len(pool), 25)
        chosen = pool[:n] if len(pool) <= n else random.Random(42).sample(pool, n)
        per = weight / len(chosen)
        for dest in chosen:
            profile.append({**dest, "weight": round(per, 6)})

    # Normalize weights
    total = sum(d["weight"] for d in profile)
    for d in profile:
        d["weight"] = round(d["weight"] / total, 6)

    OUT.mkdir(parents=True, exist_ok=True)
    out_path = OUT / "traffic-profile.json"
    with open(out_path, "w") as f:
        json.dump({"destinations": profile, "total": len(profile)}, f, indent=2)

    print(f"Traffic profile: {len(profile)} destinations across {len(ZONE_WEIGHT)} zones")
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
