#!/usr/bin/env python3
"""Validate and refresh data/lcr-dataset.json stats/timestamp."""

from __future__ import annotations

import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
OUT = ROOT / "data" / "lcr-dataset.json"


def refresh(dataset: dict) -> dict:
    e164 = dataset.get("e164", [])
    ops = dataset.get("operators", [])
    iso_map = dataset.get("iso3_to_e164", {})
    countries = Counter(row.get("iso3", "") for row in ops if row.get("iso3"))

    dataset["generated_at"] = datetime.now(timezone.utc).isoformat()
    dataset["stats"] = {
        "e164_count": len(e164),
        "operators_active": len(ops),
        "countries_iso3": len(countries),
        "iso3_mapped_to_e164": len(iso_map),
        "top_countries_by_operators": countries.most_common(10),
    }
    return dataset


def main() -> None:
    if not OUT.exists():
        raise SystemExit(f"Missing {OUT}")

    dataset = json.loads(OUT.read_text(encoding="utf-8"))
    dataset = refresh(dataset)
    OUT.write_text(json.dumps(dataset, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps(dataset["stats"], indent=2))
    print(f"Wrote {OUT} ({OUT.stat().st_size // 1024} KB)")


if __name__ == "__main__":
    main()
