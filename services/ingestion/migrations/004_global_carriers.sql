INSERT INTO carriers (id, name, priority) VALUES
    ('horizon', 'Horizon Global Voice', 4),
    ('meridian', 'Meridian Telecom', 5)
ON CONFLICT (id) DO NOTHING;

INSERT INTO vendors (id, name, adapter_type) VALUES
    ('vendor-global', 'ITU E.164 Global Deck', 'default'),
    ('vendor-competitive', 'Competitive Overrides', 'default')
ON CONFLICT (id) DO NOTHING;
