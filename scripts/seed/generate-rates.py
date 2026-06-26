#!/usr/bin/env python3
"""Build rate decks from ITU E.164 country codes + MCC operator data."""

import csv
import hashlib
import json
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
DATA = ROOT / "data"
OUT = ROOT / "scripts" / "seed" / "generated"

sys.path.insert(0, str(ROOT / "scripts" / "data"))
from dataset import e164_countries, iso3_to_e164, operators, stats as dataset_stats

# Wholesale termination partners (LCR carriers, not mobile operators)
CARRIERS = ["nexatel", "clearpath", "zenith", "horizon", "meridian"]

# Regional base $/min by first-digit / zone heuristics (plausible wholesale tiers)
ZONE_BASE = {
    "nam": 0.006,   # NANP +1
    "eu": 0.012,
    "uk": 0.010,
    "apac": 0.009,
    "latam": 0.007,
    "africa": 0.011,
    "mena": 0.013,
    "oceania": 0.010,
    "default": 0.014,
}

# Carrier regional strength: lower multiplier = more likely to quote + cheaper
CARRIER_ZONE_BIAS = {
    "nexatel": {"nam": 0.92, "latam": 0.88, "africa": 0.95, "eu": 1.08, "uk": 1.05, "apac": 1.12, "mena": 1.10, "oceania": 1.06, "default": 1.05},
    "clearpath": {"eu": 0.90, "uk": 0.88, "mena": 0.96, "nam": 1.10, "apac": 1.08, "latam": 1.05, "africa": 1.02, "oceania": 1.04, "default": 1.02},
    "zenith": {"apac": 0.87, "oceania": 0.90, "mena": 0.94, "eu": 1.06, "uk": 1.04, "nam": 1.08, "latam": 1.06, "africa": 1.05, "default": 1.04},
    "horizon": {"default": 1.15, "eu": 1.12, "uk": 1.10, "nam": 1.08, "apac": 1.10, "latam": 1.09, "africa": 1.11, "mena": 1.12, "oceania": 1.10},  # premium backup
    "meridian": {"africa": 0.93, "latam": 0.94, "mena": 0.97, "eu": 1.04, "uk": 1.05, "nam": 1.02, "apac": 1.03, "oceania": 1.02, "default": 0.98},  # budget
}

# Extra LPM sub-prefixes for major markets (country + mobile/area)
NESTED_PREFIXES = {
    "44": [("442", "uk"), ("447", "uk"), ("4477", "uk")],
    "1": [("1212", "nam"), ("1310", "nam"), ("1416", "nam")],
    "49": [("4915", "eu"), ("4917", "eu")],
    "33": [("336", "eu")],
    "91": [("919", "apac")],
    "55": [("5511", "latam")],
    "61": [("614", "oceania")],
    "81": [("813", "apac")],
}


def zone_for_prefix(prefix: str) -> str:
    if prefix.startswith("44"):
        return "uk"
    if prefix.startswith("1"):
        return "nam"
    if prefix in ("55",) or prefix.startswith("55"):
        return "latam"
    if prefix in ("27", "20", "21", "22", "23", "24", "25", "26", "28", "29") or (
        len(prefix) >= 2 and prefix[:2] in ("20", "21", "22", "23", "24", "25", "26", "27", "28", "29")
    ):
        return "africa"
    if prefix.startswith(("81", "82", "84", "86", "91", "65", "60", "66")):
        return "apac"
    if prefix.startswith(("61", "64")):
        return "oceania"
    if prefix.startswith(("971", "966", "972", "90", "98")):
        return "mena"
    if prefix.startswith(("33", "34", "39", "41", "43", "45", "46", "47", "48", "49", "31", "32", "30", "36", "351", "352", "353", "354", "358", "359")):
        return "eu"
    if prefix.startswith(("52", "54", "56", "57", "58")):
        return "latam"
    return "default"


def det_noise(prefix: str, carrier: str) -> float:
    h = hashlib.md5(f"{prefix}:{carrier}".encode()).hexdigest()
    n = int(h[:8], 16) / 0xFFFFFFFF
    return 0.94 + n * 0.12  # 0.94 – 1.06


def quotes_prefix(carrier: str, prefix: str, zone: str) -> bool:
    bias = CARRIER_ZONE_BIAS[carrier].get(zone, CARRIER_ZONE_BIAS[carrier]["default"])
    h = int(hashlib.md5(f"q:{carrier}:{prefix}".encode()).hexdigest()[:6], 16)
    threshold = 0.72 + (bias - 0.9) * 0.35
    return (h / 0xFFFFFF) < min(0.98, max(0.55, threshold))


def rate_for(prefix: str, carrier: str, zone: str | None = None) -> float:
    zone = zone or zone_for_prefix(prefix)
    base = ZONE_BASE.get(zone, ZONE_BASE["default"])
    bias = CARRIER_ZONE_BIAS[carrier].get(zone, CARRIER_ZONE_BIAS[carrier]["default"])
    return round(base * bias * det_noise(prefix, carrier), 4)


def load_e164() -> list[tuple[str, str]]:
    return [(r["prefix"], r["country"]) for r in e164_countries()]


def load_iso3_e164_map() -> dict[str, str]:
    return iso3_to_e164()


def mcc_sub_prefixes(iso: str, cc: str, mccs: set[str]) -> list[str]:
    """Country-specific mobile/LCR sub-prefixes from MCC operator data."""
    out: list[str] = []
    if iso == "GBR" or cc == "44":
        out.extend(["447", "4477", "442"])
    elif iso == "USA" or cc == "1":
        out.extend(["1212", "1310", "1404", "1718", "1305", "1416"])
    elif iso == "IND" or cc == "91":
        for mcc in sorted(mccs)[:20]:
            out.append(f"91{mcc}")
    elif iso == "DEU" or cc == "49":
        out.extend(["4915", "4917", "49176", "49151"])
    elif iso == "FRA" or cc == "33":
        out.extend(["336", "337"])
    elif iso == "BRA" or cc == "55":
        out.extend(["5511", "5521", "5531"])
    elif iso == "AUS" or cc == "61":
        out.extend(["614", "6140", "6143"])
    elif iso == "JPN" or cc == "81":
        out.extend(["813", "8190"])
    elif iso == "CHN" or cc == "86":
        out.extend(["8613", "8615", "8618"])
    else:
        for mcc in sorted(mccs)[:8]:
            p = f"{cc}{mcc}"
            if 3 <= len(p) <= 6:
                out.append(p)
        if len(mccs) >= 4:
            out.append(f"{cc}7")
    return list(dict.fromkeys(out))


def load_mcc_extra_prefixes() -> list[tuple[str, str]]:
    """Derive mobile sub-prefixes from MCC/MNC operator database."""
    iso_map = load_iso3_e164_map()
    by_iso: dict[str, set[str]] = {}
    for row in operators():
        iso = row.get("iso3", "")
        mcc = row.get("mcc", "").strip()
        if iso and mcc:
            by_iso.setdefault(iso, set()).add(mcc)

    extras: list[tuple[str, str]] = []
    seen: set[str] = set()
    for iso, mccs in by_iso.items():
        cc = iso_map.get(iso)
        if not cc:
            continue
        for sub in mcc_sub_prefixes(iso, cc, mccs):
            if sub in seen or len(sub) > 8:
                continue
            seen.add(sub)
            extras.append((sub, zone_for_prefix(cc)))
    return extras


def build_rows() -> list[dict]:
    rows = []
    e164 = load_e164()
    all_prefixes: list[tuple[str, str | None]] = [(p, None) for p, _ in e164]

    for parent, nested in NESTED_PREFIXES.items():
        for sub, zone in nested:
            all_prefixes.append((sub, zone))

    for sub, zone in load_mcc_extra_prefixes():
        all_prefixes.append((sub, zone))

    for prefix, zone_override in all_prefixes:
        zone = zone_override or zone_for_prefix(prefix)
        for carrier in CARRIERS:
            if not quotes_prefix(carrier, prefix, zone):
                continue
            rows.append({
                "prefix": prefix,
                "carrier_id": carrier,
                "cost_per_min": rate_for(prefix, carrier, zone),
            })
    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["prefix", "carrier_id", "cost_per_min"])
        w.writeheader()
        w.writerows(rows)


def competitive_subset(rows: list[dict], fraction: float = 0.12) -> list[dict]:
    """Vendor B deck: undercut ~12% of prefixes on one carrier."""
    out = []
    for r in rows:
        h = int(hashlib.md5(r["prefix"].encode()).hexdigest()[:8], 16)
        if (h % 100) / 100.0 > fraction:
            continue
        cheaper = round(r["cost_per_min"] * 0.93, 4)
        alt = CARRIERS[(CARRIERS.index(r["carrier_id"]) + 1) % len(CARRIERS)]
        out.append({"prefix": r["prefix"], "carrier_id": alt, "cost_per_min": cheaper})
    return out


def stats(rows: list[dict], e164_count: int) -> dict:
    prefixes = {r["prefix"] for r in rows}
    by_prefix: dict[str, set] = {}
    for r in rows:
        by_prefix.setdefault(r["prefix"], set()).add(r["carrier_id"])
    multi = sum(1 for c in by_prefix.values() if len(c) >= 2)

    mcc_stats = dataset_stats()
    return {
        "e164_country_codes": e164_count,
        "mcc_operators": mcc_stats.get("operators_active", 0),
        "mcc_countries": mcc_stats.get("countries_iso3", 0),
        "mcc_mapped_to_e164": mcc_stats.get("iso3_mapped_to_e164", 0),
        "rate_rows": len(rows),
        "distinct_prefixes": len(prefixes),
        "prefixes_with_2plus_carriers": multi,
        "carriers": CARRIERS,
        "competition_pct": round(100 * multi / max(len(by_prefix), 1), 1),
    }


def main() -> None:
    e164 = load_e164()
    global_rows = build_rows()
    comp_rows = competitive_subset(global_rows)

    write_csv(OUT / "rates-global.csv", global_rows)
    write_csv(OUT / "rates-competitive.csv", comp_rows)

    st = stats(global_rows, len(e164))
    with open(OUT / "rate-deck-stats.json", "w") as f:
        json.dump(st, f, indent=2)

    print(json.dumps(st, indent=2))
    print(f"Wrote {OUT / 'rates-global.csv'} ({len(global_rows)} rows)")
    print(f"Wrote {OUT / 'rates-competitive.csv'} ({len(comp_rows)} rows)")


if __name__ == "__main__":
    main()
