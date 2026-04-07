package db

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/carrier-opt/ingestion/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Store struct {
	pool *pgxpool.Pool
}

func Connect(ctx context.Context, dsn string) (*Store, error) {
	pool, err := pgxpool.New(ctx, dsn)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		return nil, err
	}
	return &Store{pool: pool}, nil
}

func (s *Store) Close() {
	s.pool.Close()
}

func (s *Store) RunMigrations(ctx context.Context, dir string) error {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	var files []string
	for _, e := range entries {
		if !e.IsDir() && strings.HasSuffix(e.Name(), ".sql") {
			files = append(files, e.Name())
		}
	}
	sort.Strings(files)
	for _, f := range files {
		body, err := os.ReadFile(filepath.Join(dir, f))
		if err != nil {
			return err
		}
		if _, err := s.pool.Exec(ctx, string(body)); err != nil {
			return fmt.Errorf("migration %s: %w", f, err)
		}
	}
	return nil
}

func (s *Store) CarrierExists(ctx context.Context, id string) (bool, error) {
	var exists bool
	err := s.pool.QueryRow(ctx, `SELECT EXISTS(SELECT 1 FROM carriers WHERE id=$1)`, id).Scan(&exists)
	return exists, err
}

func (s *Store) InsertRateSheet(ctx context.Context, vendorID, sheetHash string, effectiveAt time.Time, rates []models.RateRow) (uuid.UUID, error) {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, err
	}
	defer tx.Rollback(ctx)

	var sheetID uuid.UUID
	err = tx.QueryRow(ctx, `
		INSERT INTO rate_sheets (vendor_id, sheet_hash, effective_at, status)
		VALUES ($1, $2, $3, 'pending')
		ON CONFLICT (vendor_id, sheet_hash, effective_at) DO UPDATE SET status = EXCLUDED.status
		RETURNING id`, vendorID, sheetHash, effectiveAt).Scan(&sheetID)
	if err != nil {
		return uuid.Nil, err
	}

	for _, r := range rates {
		exists, err := s.CarrierExists(ctx, r.CarrierID)
		if err != nil {
			return uuid.Nil, err
		}
		if !exists {
			return uuid.Nil, fmt.Errorf("unknown carrier: %s", r.CarrierID)
		}
		if r.CostPerMin < 0 {
			return uuid.Nil, fmt.Errorf("negative cost for prefix %s", r.Prefix)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO rates (rate_sheet_id, prefix, carrier_id, cost_per_min, effective_at, expires_at)
			VALUES ($1, $2, $3, $4, $5, $6)`,
			sheetID, r.Prefix, r.CarrierID, r.CostPerMin, effectiveAt, r.ExpiresAt)
		if err != nil {
			return uuid.Nil, err
		}
	}

	_, err = tx.Exec(ctx, `
		INSERT INTO scheduled_activations (rate_sheet_id, effective_at, status)
		VALUES ($1, $2, 'pending')`, sheetID, effectiveAt)
	if err != nil {
		return uuid.Nil, err
	}

	if !effectiveAt.After(time.Now().UTC()) {
		if _, err := tx.Exec(ctx, `UPDATE rate_sheets SET status='active' WHERE id=$1`, sheetID); err != nil {
			return uuid.Nil, err
		}
		if _, err := tx.Exec(ctx, `UPDATE rates SET active=true WHERE rate_sheet_id=$1`, sheetID); err != nil {
			return uuid.Nil, err
		}
		if _, err := tx.Exec(ctx, `
			UPDATE scheduled_activations SET status='done', processed_at=NOW()
			WHERE rate_sheet_id=$1 AND status='pending'`, sheetID); err != nil {
			return uuid.Nil, err
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, err
	}
	return sheetID, nil
}

func HashContent(data []byte) string {
	h := sha256.Sum256(data)
	return hex.EncodeToString(h[:])
}

func (s *Store) GetRatesByVendor(ctx context.Context, vendorID string) ([]models.RateRow, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT r.prefix, r.carrier_id, r.cost_per_min, r.expires_at
		FROM rates r
		JOIN rate_sheets rs ON rs.id = r.rate_sheet_id
		WHERE rs.vendor_id = $1 AND r.active = true
		ORDER BY r.prefix`, vendorID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rates []models.RateRow
	for rows.Next() {
		var r models.RateRow
		if err := rows.Scan(&r.Prefix, &r.CarrierID, &r.CostPerMin, &r.ExpiresAt); err != nil {
			return nil, err
		}
		rates = append(rates, r)
	}
	return rates, rows.Err()
}

func (s *Store) GetAllActiveRates(ctx context.Context) ([]models.CarrierRate, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT prefix, carrier_id, cost_per_min
		FROM rates WHERE active = true`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rates []models.CarrierRate
	for rows.Next() {
		var r models.CarrierRate
		if err := rows.Scan(&r.Prefix, &r.CarrierID, &r.CostPerMin); err != nil {
			return nil, err
		}
		rates = append(rates, r)
	}
	return rates, rows.Err()
}

func (s *Store) PendingActivations(ctx context.Context) ([]models.RatesActivatedEvent, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT sa.rate_sheet_id, sa.effective_at,
			(SELECT COUNT(*) FROM rates r WHERE r.rate_sheet_id = sa.rate_sheet_id)
		FROM scheduled_activations sa
		WHERE sa.status = 'pending' AND sa.effective_at <= NOW()
		ORDER BY sa.effective_at`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.RatesActivatedEvent
	for rows.Next() {
		var e models.RatesActivatedEvent
		if err := rows.Scan(&e.RateSheetID, &e.EffectiveAt, &e.RateCount); err != nil {
			return nil, err
		}
		events = append(events, e)
	}
	return events, rows.Err()
}

func (s *Store) MarkActivationDone(ctx context.Context, sheetID uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `
		UPDATE scheduled_activations SET status='done', processed_at=NOW()
		WHERE rate_sheet_id=$1`, sheetID)
	return err
}

func (s *Store) ActivateRateSheet(ctx context.Context, sheetID uuid.UUID) error {
	tx, err := s.pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(ctx, `UPDATE rate_sheets SET status='active' WHERE id=$1`, sheetID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `UPDATE rates SET active=true WHERE rate_sheet_id=$1`, sheetID); err != nil {
		return err
	}
	if _, err := tx.Exec(ctx, `
		UPDATE scheduled_activations SET status='done', processed_at=NOW()
		WHERE rate_sheet_id=$1`, sheetID); err != nil {
		return err
	}
	return tx.Commit(ctx)
}

func (s *Store) InsertInvoice(ctx context.Context, carrierID, fileName, content string) (uuid.UUID, error) {
	var id uuid.UUID
	err := s.pool.QueryRow(ctx, `
		INSERT INTO invoice_uploads (carrier_id, file_name, file_content)
		VALUES ($1, $2, $3) RETURNING id`, carrierID, fileName, content).Scan(&id)
	return id, err
}

func (s *Store) GetUnauditedInvoices(ctx context.Context) ([]struct {
	ID        uuid.UUID
	CarrierID string
	Content   string
}, error) {
	rows, err := s.pool.Query(ctx, `
		SELECT id, carrier_id, file_content FROM invoice_uploads WHERE audited = false`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var result []struct {
		ID        uuid.UUID
		CarrierID string
		Content   string
	}
	for rows.Next() {
		var r struct {
			ID        uuid.UUID
			CarrierID string
			Content   string
		}
		if err := rows.Scan(&r.ID, &r.CarrierID, &r.Content); err != nil {
			return nil, err
		}
		result = append(result, r)
	}
	return result, rows.Err()
}

func (s *Store) MarkInvoiceAudited(ctx context.Context, id uuid.UUID) error {
	_, err := s.pool.Exec(ctx, `UPDATE invoice_uploads SET audited=true WHERE id=$1`, id)
	return err
}

func (s *Store) InsertAuditFlag(ctx context.Context, invoiceID uuid.UUID, carrierID, prefix string, expected, invoiced, pct float64) error {
	_, err := s.pool.Exec(ctx, `
		INSERT INTO audit_flags (invoice_upload_id, carrier_id, prefix, expected_cost, invoiced_cost, discrepancy_pct)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		invoiceID, carrierID, prefix, expected, invoiced, pct)
	return err
}

func (s *Store) GetVendorAdapterType(ctx context.Context, vendorID string) (string, error) {
	var adapterType string
	err := s.pool.QueryRow(ctx, `SELECT adapter_type FROM vendors WHERE id=$1`, vendorID).Scan(&adapterType)
	return adapterType, err
}

func (s *Store) ListCarrierIDs(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, `SELECT id FROM carriers ORDER BY priority, id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		ids = append(ids, id)
	}
	return ids, rows.Err()
}

func (s *Store) CountActiveRates(ctx context.Context) (int, error) {
	var n int
	err := s.pool.QueryRow(ctx, `SELECT COUNT(*) FROM rates WHERE active = true`).Scan(&n)
	return n, err
}
