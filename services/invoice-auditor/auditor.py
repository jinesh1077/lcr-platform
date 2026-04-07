#!/usr/bin/env python3
"""Invoice auditor: compares carrier invoices against ClickHouse CDR aggregates."""

import csv
import io
import json
import os
import sys
from datetime import datetime

import clickhouse_connect
import polars as pl
import psycopg2


def env(key: str, default: str) -> str:
    return os.environ.get(key, default)


def main() -> int:
    threshold_pct = float(env("AUDIT_THRESHOLD_PCT", "2.0"))
    pg_dsn = env(
        "POSTGRES_DSN",
        "postgresql://carrier:carrier_secret@localhost:5432/carrier_opt",
    )
    ch_host = env("CLICKHOUSE_HOST", "localhost")
    ch_port = int(env("CLICKHOUSE_PORT", "8123"))
    ch_db = env("CLICKHOUSE_DB", "carrier_opt")
    ch_pass = env("CLICKHOUSE_PASSWORD", "")

    pg = psycopg2.connect(pg_dsn)
    ch = clickhouse_connect.get_client(
        host=ch_host, port=ch_port, database=ch_db, password=ch_pass
    )

    cur = pg.cursor()
    cur.execute(
        "SELECT id, carrier_id, file_content FROM invoice_uploads WHERE audited = false"
    )
    rows = cur.fetchall()

    if not rows:
        print(json.dumps({"status": "no_invoices", "flags": 0}))
        return 0

    total_flags = 0
    reports = []

    for invoice_id, carrier_id, content in rows:
        invoice_df = parse_invoice(content)
        expected_df = query_expected_costs(ch, carrier_id)

        if expected_df.is_empty() or invoice_df.is_empty():
            mark_audited(cur, pg, invoice_id)
            continue

        joined = invoice_df.join(
            expected_df, on=["prefix"], how="left"
        ).with_columns(
            pl.col("expected_cost").fill_null(0.0),
            (
                (pl.col("invoiced_cost") - pl.col("expected_cost")).abs()
                / pl.when(pl.col("expected_cost") > 0)
                .then(pl.col("expected_cost"))
                .otherwise(1.0)
                * 100
            ).alias("discrepancy_pct"),
        )

        flagged = joined.filter(pl.col("discrepancy_pct") > threshold_pct)
        for row in flagged.iter_rows(named=True):
            insert_flag(cur, invoice_id, carrier_id, row)
            total_flags += 1
            reports.append(
                {
                    "carrier_id": carrier_id,
                    "prefix": row["prefix"],
                    "expected": row["expected_cost"],
                    "invoiced": row["invoiced_cost"],
                    "discrepancy_pct": round(row["discrepancy_pct"], 2),
                }
            )

        mark_audited(cur, pg, invoice_id)

    pg.commit()
    output = {
        "status": "complete",
        "audited_invoices": len(rows),
        "flags": total_flags,
        "report": reports,
        "timestamp": datetime.utcnow().isoformat(),
    }
    print(json.dumps(output, indent=2))

    report_path = env("AUDIT_REPORT_PATH", "/tmp/audit_report.json")
    with open(report_path, "w") as f:
        json.dump(output, f, indent=2)

    return 0 if total_flags >= 0 else 1


def parse_invoice(content: str) -> pl.DataFrame:
    reader = csv.DictReader(io.StringIO(content))
    rows = []
    for row in reader:
        prefix = row.get("prefix") or row.get("destination") or row.get("dest")
        cost = row.get("cost") or row.get("amount") or row.get("total")
        if prefix and cost:
            rows.append({"prefix": prefix.strip().lstrip("+"), "invoiced_cost": float(cost)})
    return pl.DataFrame(rows) if rows else pl.DataFrame({"prefix": [], "invoiced_cost": []})


def query_expected_costs(ch, carrier_id: str) -> pl.DataFrame:
    result = ch.query(
        """
        SELECT
            substring(dialed_number, 1, 3) AS prefix,
            sum(cost_theoretical) AS expected_cost
        FROM cdr_raw
        WHERE carrier_id = {carrier_id:String}
        GROUP BY prefix
        """,
        parameters={"carrier_id": carrier_id},
    )
    if not result.result_rows:
        return pl.DataFrame({"prefix": [], "expected_cost": []})
    return pl.DataFrame(
        {"prefix": [r[0] for r in result.result_rows], "expected_cost": [r[1] for r in result.result_rows]}
    )


def insert_flag(cur, invoice_id, carrier_id, row):
    cur.execute(
        """
        INSERT INTO audit_flags (invoice_upload_id, carrier_id, prefix, expected_cost, invoiced_cost, discrepancy_pct)
        VALUES (%s, %s, %s, %s, %s, %s)
        """,
        (
            invoice_id,
            carrier_id,
            row["prefix"],
            row["expected_cost"],
            row["invoiced_cost"],
            row["discrepancy_pct"],
        ),
    )


def mark_audited(cur, pg, invoice_id):
    cur.execute("UPDATE invoice_uploads SET audited = true WHERE id = %s", (invoice_id,))
    pg.commit()


if __name__ == "__main__":
    sys.exit(main())
