{{ config(materialized='table') }}

WITH s AS (
    SELECT *
    FROM {{ ref('stg_issues') }}
)

SELECT
    issue_date,
    region,

    COUNT(*) AS total_issues,

    -- actual SLA breach rate from truth labels
    AVG(CASE WHEN sla_breached THEN 1 ELSE 0 END) AS sla_breach_rate,

    -- average predicted risk from the DNN model
    AVG(risk_score) AS avg_risk_score,

    -- share of issues the model thinks are high-risk
    AVG(CASE WHEN risk_bucket IN ('high', 'very_high') THEN 1 ELSE 0 END) AS high_risk_share,

    -- counts by risk bucket for stacked bar charts
    COUNTIF(risk_bucket = 'very_low')  AS cnt_very_low,
    COUNTIF(risk_bucket = 'low')       AS cnt_low,
    COUNTIF(risk_bucket = 'medium')    AS cnt_medium,
    COUNTIF(risk_bucket = 'high')      AS cnt_high,
    COUNTIF(risk_bucket = 'very_high') AS cnt_very_high
FROM s
GROUP BY
    issue_date,
    region
ORDER BY
    issue_date,
    region
