-- Rename legacy carrier IDs (safe to re-run)
UPDATE rates SET carrier_id = 'nexatel' WHERE carrier_id = 'carrier-alpha';
UPDATE rates SET carrier_id = 'clearpath' WHERE carrier_id = 'carrier-beta';
UPDATE rates SET carrier_id = 'zenith' WHERE carrier_id = 'carrier-gamma';
UPDATE invoice_uploads SET carrier_id = 'nexatel' WHERE carrier_id = 'carrier-alpha';
UPDATE invoice_uploads SET carrier_id = 'clearpath' WHERE carrier_id = 'carrier-beta';
UPDATE invoice_uploads SET carrier_id = 'zenith' WHERE carrier_id = 'carrier-gamma';
UPDATE audit_flags SET carrier_id = 'nexatel' WHERE carrier_id = 'carrier-alpha';
UPDATE audit_flags SET carrier_id = 'clearpath' WHERE carrier_id = 'carrier-beta';
UPDATE audit_flags SET carrier_id = 'zenith' WHERE carrier_id = 'carrier-gamma';

DELETE FROM carriers WHERE id IN ('carrier-alpha', 'carrier-beta', 'carrier-gamma');

INSERT INTO carriers (id, name, priority) VALUES
    ('nexatel', 'Nexatel International', 1),
    ('clearpath', 'Clearpath Wholesale', 2),
    ('zenith', 'Zenith Transit', 3)
ON CONFLICT (id) DO UPDATE SET name = EXCLUDED.name;
