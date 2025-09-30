CREATE OR REPLACE FUNCTION apalis.get_fair_jobs(
    worker_id TEXT,
    v_job_type TEXT,
    v_job_count INTEGER DEFAULT 5
) RETURNS SETOF apalis.jobs AS $$
BEGIN
    RETURN QUERY
    WITH locked_jobs AS (
        SELECT
            id,
            run_at,
            job
        FROM apalis.jobs
        WHERE
            status = 'Pending'
            AND run_at <= NOW()
            AND job_type = v_job_type
        ORDER BY run_at ASC, id ASC
        FOR UPDATE SKIP LOCKED
    ),
    ranked_jobs AS (
        SELECT
            id,
            run_at,
            ROW_NUMBER() OVER (
                PARTITION BY COALESCE(job -> 'identity' ->> 'user_id', id::text)
                ORDER BY run_at ASC, id ASC
            ) AS rn
        FROM locked_jobs
    ),
    fair_jobs AS (
        SELECT id
        FROM ranked_jobs
        WHERE rn = 1
        ORDER BY run_at ASC, id ASC
        LIMIT v_job_count
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
