CREATE DATABASE IF NOT EXISTS carrier_opt;

CREATE TABLE IF NOT EXISTS carrier_opt.cdr_raw (
    call_id UUID,
    dialed_number String,
    carrier_id String,
    duration_sec UInt32,
    answered UInt8,
    disconnect_reason String,
    timestamp DateTime,
    cost_theoretical Float64,
    ingested_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (carrier_id, timestamp)
TTL timestamp + INTERVAL 7 DAY;
