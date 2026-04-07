-- Carriers registry
CREATE EXTENSION IF NOT EXISTS pgcrypto;
CREATE TABLE IF NOT EXISTS carriers (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    priority INT NOT NULL DEFAULT 0,
    metadata JSONB DEFAULT '{}',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Vendors
CREATE TABLE IF NOT EXISTS vendors (
    id TEXT PRIMARY KEY,
    name TEXT NOT NULL,
    adapter_type TEXT NOT NULL DEFAULT 'default',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Rate sheets (idempotent uploads)
CREATE TABLE IF NOT EXISTS rate_sheets (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    vendor_id TEXT NOT NULL REFERENCES vendors(id),
    sheet_hash TEXT NOT NULL,
    effective_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(vendor_id, sheet_hash, effective_at)
);

-- Normalized rates
CREATE TABLE IF NOT EXISTS rates (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rate_sheet_id UUID NOT NULL REFERENCES rate_sheets(id) ON DELETE CASCADE,
    prefix TEXT NOT NULL,
    carrier_id TEXT NOT NULL REFERENCES carriers(id),
    cost_per_min NUMERIC(12, 6) NOT NULL CHECK (cost_per_min >= 0),
    effective_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ,
    active BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rates_prefix ON rates(prefix);
CREATE INDEX IF NOT EXISTS idx_rates_active ON rates(active, effective_at);

-- Scheduled activations
CREATE TABLE IF NOT EXISTS scheduled_activations (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    rate_sheet_id UUID NOT NULL REFERENCES rate_sheets(id),
    effective_at TIMESTAMPTZ NOT NULL,
    status TEXT NOT NULL DEFAULT 'pending',
    processed_at TIMESTAMPTZ,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_scheduled_pending ON scheduled_activations(status, effective_at)
    WHERE status = 'pending';

-- Invoice uploads for auditor
CREATE TABLE IF NOT EXISTS invoice_uploads (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    carrier_id TEXT NOT NULL REFERENCES carriers(id),
    file_name TEXT NOT NULL,
    file_content TEXT NOT NULL,
    uploaded_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    audited BOOLEAN NOT NULL DEFAULT FALSE
);

-- Audit flags
CREATE TABLE IF NOT EXISTS audit_flags (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    invoice_upload_id UUID NOT NULL REFERENCES invoice_uploads(id),
    carrier_id TEXT NOT NULL,
    prefix TEXT,
    expected_cost NUMERIC(12, 6),
    invoiced_cost NUMERIC(12, 6),
    discrepancy_pct NUMERIC(8, 4),
    flagged_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Seed carriers
INSERT INTO carriers (id, name, priority) VALUES
    ('nexatel', 'Nexatel International', 1),
    ('clearpath', 'Clearpath Wholesale', 2),
    ('zenith', 'Zenith Transit', 3)
ON CONFLICT (id) DO NOTHING;

INSERT INTO vendors (id, name, adapter_type) VALUES
    ('vendor-a', 'Vendor A (CSV)', 'vendor_a'),
    ('vendor-b', 'Vendor B (JSON)', 'vendor_b'),
    ('vendor-default', 'Default Vendor', 'default'),
    ('vendor-lpm-demo', 'LPM Demo Rates', 'default')
ON CONFLICT (id) DO NOTHING;
