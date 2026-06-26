"""Load the canonical LCR platform dataset (data/lcr-dataset.json)."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
DATASET_PATH = ROOT / "data" / "lcr-dataset.json"


def load(path: Path | None = None) -> dict[str, Any]:
    p = path or DATASET_PATH
    with open(p, encoding="utf-8") as f:
        return json.load(f)


def e164_countries(ds: dict[str, Any] | None = None) -> list[dict[str, str]]:
    return (ds or load())["e164"]


def operators(ds: dict[str, Any] | None = None) -> list[dict[str, str]]:
    return (ds or load())["operators"]


def iso3_to_e164(ds: dict[str, Any] | None = None) -> dict[str, str]:
    return (ds or load())["iso3_to_e164"]


def stats(ds: dict[str, Any] | None = None) -> dict[str, Any]:
    return (ds or load()).get("stats", {})
