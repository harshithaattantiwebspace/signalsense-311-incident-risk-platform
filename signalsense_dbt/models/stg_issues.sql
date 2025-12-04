{{ config(materialized='view') }}

WITH base AS (
    SELECT
        issue_id,
        created_at,
        resolved_at,
        region,
        channel,
        raw_category,
        topic_id,
        topic_keywords,
        sla_breached,
        predicted_label,

        -- risk_score is a STRUCT<label, prob>, so extract the numeric probability
        SAFE_CAST(risk_score.prob AS FLOAT64) AS risk_prob,

        resolution_hours
    FROM {{ source('raw', 'issues_scored') }}
)

SELECT
    issue_id,

    created_at,
    DATE(created_at) AS issue_date,
    EXTRACT(HOUR FROM created_at) AS issue_hour,
    EXTRACT(DAYOFWEEK FROM created_at) AS issue_dow,

    region,
    channel,
    raw_category,
    topic_id,
    topic_keywords,

    sla_breached,
    predicted_label,

    -- expose the numeric probability as risk_score for downstream
    risk_prob AS risk_score,
    resolution_hours,

    CASE
        WHEN risk_prob IS NULL THEN 'unknown'
        WHEN risk_prob < 0.2 THEN 'very_low'
        WHEN risk_prob < 0.4 THEN 'low'
        WHEN risk_prob < 0.6 THEN 'medium'
        WHEN risk_prob < 0.8 THEN 'high'
        ELSE 'very_high'
    END AS risk_bucket
FROM base
