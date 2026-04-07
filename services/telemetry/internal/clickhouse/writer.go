package clickhouse

import (
	"context"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/carrier-opt/telemetry/internal/models"
)

type Writer struct {
	conn driver.Conn
}

func Connect(addr, db, password string) (*Writer, error) {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{addr},
		Auth: clickhouse.Auth{
			Database: db,
			Username: "default",
			Password: password,
		},
	})
	if err != nil {
		return nil, err
	}
	if err := conn.Ping(context.Background()); err != nil {
		return nil, err
	}
	return &Writer{conn: conn}, nil
}

func (w *Writer) InsertBatch(ctx context.Context, cdrs []models.CDREvent) error {
	if len(cdrs) == 0 {
		return nil
	}
	batch, err := w.conn.PrepareBatch(ctx, `
		INSERT INTO cdr_raw (call_id, dialed_number, carrier_id, duration_sec, answered, disconnect_reason, timestamp, cost_theoretical)`)
	if err != nil {
		return err
	}
	for _, c := range cdrs {
		answered := uint8(0)
		if c.Answered {
			answered = 1
		}
		if err := batch.Append(c.CallID, c.DialedNumber, c.CarrierID, c.DurationSec, answered, c.DisconnectReason, c.Timestamp, c.CostTheoretical); err != nil {
			return err
		}
	}
	return batch.Send()
}

func (w *Writer) Close() error {
	return w.conn.Close()
}

func Now() time.Time { return time.Now().UTC() }
