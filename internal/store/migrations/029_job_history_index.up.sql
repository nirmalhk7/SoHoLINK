-- Owner-scoped keyset pagination, including a deterministic timestamp tie-breaker.
CREATE INDEX idx_jobs_participant_history
    ON jobs (participant_id, created_at DESC, id DESC);
