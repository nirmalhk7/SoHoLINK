package store

import (
	"context"
	"log/slog"
	"time"
)

// RunUptimeScorer runs in a goroutine and updates nodes.uptime_pct every
// interval based on heartbeat events over the past 7 days.
// Expected heartbeats = 7 * 24 * 60 * 2 (one per 30s).
func RunUptimeScorer(ctx context.Context, db *DB, interval time.Duration) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := scoreUptime(ctx, db); err != nil {
				// Non-fatal — log and continue on next tick.
				slog.Warn("uptime scorer error", "error", err)
			}
		}
	}
}

func scoreUptime(ctx context.Context, db *DB) error {
	_, err := db.Pool.Exec(ctx, `
		WITH heartbeat_counts AS (
			SELECT node_id, COUNT(*) AS count
			FROM node_heartbeat_events
			WHERE recorded_at > NOW() - INTERVAL '7 days'
			GROUP BY node_id
		), scores AS (
			SELECT n.id,
			       LEAST(COALESCE(h.count, 0)::numeric / 20160 * 100, 100) AS uptime_pct
			FROM nodes n
			LEFT JOIN heartbeat_counts h ON h.node_id = n.id
		)
		UPDATE nodes n
		SET uptime_pct = s.uptime_pct
		FROM scores s
		WHERE n.id = s.id`,
	)
	return err
}
