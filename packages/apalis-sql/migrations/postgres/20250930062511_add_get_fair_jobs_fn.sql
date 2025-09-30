--
-- apalis.get_fair_jobs
--
-- Purpose
--   Fairly select up to `v_job_count` jobs for a worker, ensuring at most
--   one job per user in a single batch, while still respecting job `priority`.
--
-- Approach (3-step CTE pipeline):
--   1) locked_jobs:   Build a small, ordered frontier of eligible jobs and lock them
--                     (FOR UPDATE SKIP LOCKED) to avoid race conditions across workers.
--   2) ranked_jobs:   Use ROW_NUMBER() partitioned by user to pick the best job per user
--                     (highest priority, then oldest) within that frontier.
--   3) fair_jobs:     Take the first (rn = 1) per user and cap to `v_job_count`.
--
-- Notes
--   • We read user identity from JSON at path job -> 'identity' ->> 'user_id'.
--     If missing, we fall back to id::text so such jobs are treated as their own "user".
--   • We include Failed jobs that still have remaining attempts, matching the behavior
--     of the non-fair get_jobs function.
--   • Ordering by priority DESC, run_at ASC, id ASC produces stable and predictable results.
--   • The heuristic LIMIT `v_job_count * 10` on the lock frontier keeps locking cheap but
--     provides sufficient variety to find up to `v_job_count` distinct users.
--     Adjust this multiplier if you have many users relative to `v_job_count`.
--
CREATE OR REPLACE FUNCTION apalis.get_fair_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    RETURN QUERY
    -- 1) Build a frontier of candidates and lock them to avoid concurrent selection.
    WITH locked_jobs AS (
        SELECT
            id,
            run_at,
            job,
            priority
        FROM apalis.jobs
        WHERE
            -- Eligible jobs: Pending, or Failed with retries left.
            (status = 'Pending' OR (status = 'Failed' AND attempts < max_attempts))
            AND run_at <= NOW()
            AND job_type = v_job_type
        -- Respect priority first, then oldest by run_at, with id as a stable tie-breaker.
        ORDER BY priority DESC, run_at ASC, id ASC
        -- Heuristic: lock a small superset to ensure we likely get distinct users
        -- while avoiding locking the entire table.
        LIMIT v_job_count * 10 FOR
        UPDATE SKIP LOCKED
    ),
    -- 2) Rank the locked jobs per user, so we can pick at most one per user.
    ranked_jobs AS (
        SELECT
            id,
            run_at,
            ROW_NUMBER() OVER (
                -- The id::text fallback guarantees every job has a partition key. 
                -- If a job lacks user_id, we treat that job as its own “user”, 
                -- preventing multiple unknown-user jobs from crowding the batch.
                PARTITION BY COALESCE(
                    job -> 'identity' ->> 'user_id',  -- case 1: 'user_id' is nested in 'identity'
                    job ->> 'user_id',                -- case 2: 'user_id' is at top-level
                    id::text                          -- fallback
                )
                ORDER BY priority DESC, run_at ASC, id ASC
            ) AS rn
        FROM locked_jobs
    ),
    -- 3) Take the top-ranked job per user, up to the requested batch size.
    fair_jobs AS (
        SELECT id
        FROM ranked_jobs
        WHERE rn = 1
        ORDER BY run_at ASC, id ASC
        LIMIT v_job_count
    )
    -- Atomically mark the chosen jobs as Running for this worker and return them.
    UPDATE apalis.jobs
    SET
        status = 'Running',
        lock_by = worker_id,
        lock_at = now()
    WHERE
        id IN (SELECT id FROM fair_jobs)
    RETURNING *;
END;
$$ LANGUAGE plpgsql VOLATILE;
