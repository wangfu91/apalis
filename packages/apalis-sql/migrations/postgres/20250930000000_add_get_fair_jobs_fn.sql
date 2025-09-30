CREATE OR REPLACE FUNCTION apalis.get_fair_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    RETURN QUERY
    WITH ranked_jobs AS (
        SELECT
            id,
            ROW_NUMBER() OVER(PARTITION BY job -> 'identity' ->> 'user_id' ORDER BY run_at ASC, id ASC) as rn
        FROM apalis.jobs
        WHERE
            status = 'Pending'
            AND run_at <= NOW()
            AND job_type = v_job_type
    ),
    fair_jobs AS (
        SELECT id
        FROM ranked_jobs
        WHERE rn = 1
        ORDER BY run_at ASC, id ASC
        LIMIT v_job_count
        FOR UPDATE SKIP LOCKED
    )
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
