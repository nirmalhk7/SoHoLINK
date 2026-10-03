package store_test

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/NetworkTheoryAppliedResearchInstitute/soholink/internal/store"
)

func TestRunUptimeScorer(t *testing.T) {
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		t.Skip("TEST_DATABASE_URL not set — skipping integration test (see docs/test-database.md)")
	}

	ctx := context.Background()

	db, err := store.Connect(ctx, dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Pool.Close()

	var dbName string
	if err := db.Pool.QueryRow(ctx, `SELECT current_database()`).Scan(&dbName); err != nil {
		t.Fatalf("current_database: %v", err)
	}
	if !strings.Contains(dbName, "test") {
		t.Fatalf("refusing to run destructive integration test: connected database %q does not contain \"test\" in its name; set TEST_DATABASE_URL to a dedicated test database", dbName)
	}

	// Seed: create a participant and a node.
	var participantID, nodeID string
	err = db.Pool.QueryRow(ctx, `
		INSERT INTO participants (email, password_hash, display_name)
		VALUES ('uptime-test@example.com', 'x', 'Uptime Test')
		RETURNING id`,
	).Scan(&participantID)
	if err != nil {
		t.Fatalf("insert participant: %v", err)
	}
	t.Cleanup(func() {
		db.Pool.Exec(ctx, `DELETE FROM participants WHERE id = $1`, participantID)
	})

	err = db.Pool.QueryRow(ctx, `
		INSERT INTO nodes (participant_id, hostname, country_code, node_class)
		VALUES ($1, 'uptime-test-host', 'US', 'C'::node_class)
		RETURNING id`,
		participantID,
	).Scan(&nodeID)
	if err != nil {
		t.Fatalf("insert node: %v", err)
	}

	// Seed heartbeats: 20160 expected over 7 days (one per 30s).
	// Insert 19152 heartbeats → 19152/20160 ≈ 94.97% → below Class A (95%), above Class B (85%).
	heartbeatCount := 19152
	_, err = db.Pool.Exec(ctx, `
		INSERT INTO node_heartbeat_events (node_id, recorded_at)
		SELECT $1, NOW() - n * INTERVAL '30 seconds'
		FROM generate_series(0, $2 - 1) AS n`, nodeID, heartbeatCount)
	if err != nil {
		t.Fatalf("insert heartbeats: %v", err)
	}

	// Run the scorer via RunUptimeScorer with a short interval.
	scorerCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- store.RunUptimeScorer(scorerCtx, db, 10*time.Millisecond)
	}()

	// Give the scorer time to fire at least once, then cancel.
	time.Sleep(100 * time.Millisecond)
	cancel()
	<-errCh

	// Verify uptime_pct was updated.
	var uptimePct float64
	err = db.Pool.QueryRow(ctx,
		`SELECT uptime_pct FROM nodes WHERE id = $1`, nodeID,
	).Scan(&uptimePct)
	if err != nil {
		t.Fatalf("read uptime_pct: %v", err)
	}

	// 19152/20160 = 94.97% — should be ≥ 85 (Class B threshold) and < 95.
	if uptimePct < 85.0 {
		t.Errorf("expected uptime_pct ≥ 85, got %.2f", uptimePct)
	}
	if uptimePct > 100.0 {
		t.Errorf("uptime_pct capped at 100, got %.2f", uptimePct)
	}
}
