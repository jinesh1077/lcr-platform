INSERT INTO vendors (id, name, adapter_type) VALUES
    ('vendor-lpm-demo', 'LPM Demo Rates', 'default')
ON CONFLICT (id) DO NOTHING;
