import os
import time
from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st
from google.cloud import bigquery
from dotenv import load_dotenv

# Optional LLM (OpenAI)
try:
    from openai import OpenAI
except ImportError:
    OpenAI = None


# -------------------------
# Config & setup
# -------------------------

load_dotenv()

PROJECT_ID = "signalsense-project"
MART_TABLE = "signalsense_dbt.mart_issues_by_region_day"  # dataset.table
FACT_TABLE = "signalsense.issues_scored"                  # dataset.table

DAG_ID = "signalsense_daily_pipeline"

AIRFLOW_BASE_URL = os.getenv("AIRFLOW_BASE_URL", "http://localhost:8080")
AIRFLOW_USERNAME = os.getenv("AIRFLOW_USERNAME", "airflow")
AIRFLOW_PASSWORD = os.getenv("AIRFLOW_PASSWORD", "airflow")

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")

llm_client = OpenAI(api_key=OPENAI_API_KEY) if (OpenAI and OPENAI_API_KEY) else None


@st.cache_resource
def get_bq_client():
    return bigquery.Client(project=PROJECT_ID)


# -------------------------
# BigQuery data loaders
# -------------------------

@st.cache_data(ttl=300)
def load_mart(start_date: date, end_date: date) -> pd.DataFrame:
    """
    Load the dbt mart from BigQuery.

    Uses the schema:
      issue_date, region, total_issues,
      sla_breach_rate, avg_risk_score, high_risk_share,
      cnt_very_low, cnt_low, cnt_medium, ...
    """
    client = get_bq_client()

    query = f"""
        SELECT
          issue_date,
          region,
          total_issues,
          sla_breach_rate,
          avg_risk_score,
          high_risk_share,
          cnt_very_low,
          cnt_low,
          cnt_medium
        FROM `{PROJECT_ID}.{MART_TABLE}`
        WHERE issue_date BETWEEN @start_date AND @end_date
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
        ]
    )

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(
            "BigQuery error while loading mart_issues_by_region_day.\n\n"
            f"Check that the table `{PROJECT_ID}.{MART_TABLE}` exists and "
            "has the columns: issue_date, region, total_issues, "
            "sla_breach_rate, avg_risk_score, high_risk_share, "
            "cnt_very_low, cnt_low, cnt_medium.\n\n"
            f"Raw error: {e}"
        )
        return pd.DataFrame()


@st.cache_data(ttl=300)
def load_issues_sample(start_date: date, end_date: date, max_rows: int = 500) -> pd.DataFrame:
    """
    Load a sample of row-level scored issues for the detail table.

    We don't assume specific column names here – just select *.
    """
    client = get_bq_client()

    query = f"""
        SELECT
          *
        FROM `{PROJECT_ID}.{FACT_TABLE}`
        WHERE DATE(created_at) BETWEEN @start_date AND @end_date
        ORDER BY created_at DESC
        LIMIT @max_rows
    """

    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ScalarQueryParameter("start_date", "DATE", start_date),
            bigquery.ScalarQueryParameter("end_date", "DATE", end_date),
            bigquery.ScalarQueryParameter("max_rows", "INT64", max_rows),
        ]
    )

    try:
        df = client.query(query, job_config=job_config).to_dataframe()
        return df
    except Exception as e:
        st.error(
            "BigQuery error while loading issues_scored.\n\n"
            f"Check that the table `{PROJECT_ID}.{FACT_TABLE}` exists "
            "and that it has a created_at column.\n\n"
            f"Raw error: {e}"
        )
        return pd.DataFrame()


# -------------------------
# Airflow integration
# -------------------------

def trigger_airflow_dag_and_wait() -> tuple[bool, str]:
    """
    Trigger the Airflow DAG via REST API and wait until it finishes
    (success or failed), then return (success_flag, final_state).
    """
    dag_run_id = f"streamlit-trigger-{int(time.time())}"

    trigger_url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns"
    payload = {"dag_run_id": dag_run_id}
    auth = (AIRFLOW_USERNAME, AIRFLOW_PASSWORD)

    try:
        r = requests.post(trigger_url, auth=auth, json=payload, timeout=10)
    except Exception as e:
        return False, f"Error connecting to Airflow: {e}"

    if r.status_code not in (200, 201):
        # Pass back the full error so Streamlit shows it
        return False, f"Failed to trigger DAG: {r.status_code} {r.text}"

    # Poll DAG run status
    status_url = f"{AIRFLOW_BASE_URL}/api/v1/dags/{DAG_ID}/dagRuns/{dag_run_id}"
    max_wait_seconds = 180  # 3 minutes
    poll_interval = 5
    waited = 0

    while waited < max_wait_seconds:
        time.sleep(poll_interval)
        waited += poll_interval

        try:
            resp = requests.get(status_url, auth=auth, timeout=10)
        except Exception as e:
            return False, f"Error polling DAG run: {e}"

        if resp.status_code != 200:
            continue

        state = resp.json().get("state", "").lower()
        if state in ("success", "failed"):
            return (state == "success"), state

    return False, "timeout"


# -------------------------
# LLM insight generator
# -------------------------

def generate_llm_insights(mart_df: pd.DataFrame, start_date: date, end_date: date) -> str:
    """
    Use an LLM to summarize patterns in the mart:
    region, total_issues, sla_breach_rate, avg_risk_score, high_risk_share, etc.
    """
    if llm_client is None:
        return "LLM is not configured. Set OPENAI_API_KEY to enable insights."

    if mart_df.empty:
        return "No data in the selected range. Nothing to analyze."

    # Aggregate summary by region
    if "region" in mart_df.columns:
        summary_df = (
            mart_df.groupby("region", dropna=False)
            .agg(
                total_issues=("total_issues", "sum"),
                avg_sla_breach_rate=("sla_breach_rate", "mean"),
                avg_risk_score=("avg_risk_score", "mean"),
                avg_high_risk_share=("high_risk_share", "mean"),
            )
            .reset_index()
            .sort_values("total_issues", ascending=False)
            .head(30)
        )
    else:
        summary_df = (
            mart_df.agg(
                total_issues=("total_issues", "sum"),
                avg_sla_breach_rate=("sla_breach_rate", "mean"),
                avg_risk_score=("avg_risk_score", "mean"),
                avg_high_risk_share=("high_risk_share", "mean"),
            )
            .to_frame()
            .T
        )

    summary_text = summary_df.to_csv(index=False)

    prompt = f"""
You are an incident reliability analyst for a city 311 operations team.

You are given aggregated data for service requests between {start_date} and {end_date}.
Each row has: {', '.join(summary_df.columns)}.

Here is the data (CSV):
{summary_text}

In 4–6 bullet points, explain:
- Which regions show the highest risk (based on sla_breach_rate, avg_risk_score, and high_risk_share)
- Any patterns between total_issues and sla_breach_rate
- Operational recommendations (what the city operations team should prioritize)

Write in clear, non-technical language suitable for a city operations manager.
"""

    try:
        completion = llm_client.chat.completions.create(
            model="gpt-5-mini",
            messages=[
                {"role": "system", "content": "You are an expert reliability and operations analyst."},
                {"role": "user", "content": prompt},
            ],
        )
        return completion.choices[0].message.content.strip()
    except Exception as e:
        return f"LLM call failed: {e}"


# -------------------------
# Streamlit layout
# -------------------------

def main():
    st.set_page_config(
        page_title="SignalSense – 311 Incident Intelligence",
        layout="wide",
    )

    st.title("📡 SignalSense – 311 SLA Risk & Incident Intelligence")

    st.markdown(
        """
        
### What is SignalSense?

SignalSense is a city-incident monitoring tool built on top of open 311 service-request data.

Each row is a citizen complaint (noise, sanitation, streetlight, etc.) with timestamps, location, agency and a free-text description.

The goal is simple:

“Which incidents are most likely to miss their SLA, and where are we seeing risk build up across the city?”

---

### 1. Data & cleaning

We started from a ~1M row NYC 311 dataset and created a clean, unified incident table:

* Parsed **created** and **closed** timestamps and computed
  **`resolution_hours` = time to close**.
* Marked each ticket as **`sla_breached`** (yes/no) based on a target SLA window.
* Standardized location fields into a **region** (borough) column.
* Dropped obviously broken rows and handled missing values so that models and dashboards see consistent, reliable data.

This cleaned table is stored in **BigQuery** as `issues_enriched` and acts as the single source of truth for the rest of the project.

---

### 2. NLP signal from complaint text

The 311 data also contains free-text complaint descriptions.
We used light-weight NLP to turn that into usable signal:

* Cleaned the text (lower-case, removed stopwords, etc.).
* Extracted **key complaint terms / phrases** (e.g. “loud music”, “pothole”, “tree down”) and mapped them into a compact set of **issue keywords**.
* Used these keywords both as **features for the ML model** and as filters in the UI (“show me high-risk noise complaints in Brooklyn”).

This gives the model and the user some understanding of *what* the complaint is about, not just when and where it happened.

---

### 3. Streaming into BigQuery (Kafka → BigQuery)

To simulate near real-time operations, new incidents are streamed through **Kafka**:

* A **producer** sends new 311-style events into a Kafka topic.
* A **consumer** reads those events, applies the same transforms as batch processing, and inserts them into **BigQuery** (`issues_enriched_stream`), which is then merged into `issues_enriched`.

This shows how the system can keep up with ongoing tickets instead of only working on historical snapshots.

---

### 4. Predicting SLA risk with BigQuery ML

On top of the enriched table we built an ML model in **BigQuery ML** to predict whether a new incident will miss its SLA:

* Label: **`sla_breached`** (1 = breach, 0 = met).
* Features: region, time of day, complaint type, issue keywords, etc.
* Tried three model families:

  * **Logistic regression**
  * **Boosted tree classifier**
  * **DNN classifier**
* Evaluated them with `ML.EVALUATE` (accuracy, ROC AUC, precision/recall).

The **DNN classifier** performed best, so we selected it as the production model.

For every incident we store the prediction back in BigQuery:

* **`sla_breach_score`** – probability (0–1) that this ticket will miss SLA.
* **`risk_bucket`** – human-friendly band (very low / low / medium / high).

These are the core analysis results: they let operations rank tickets by risk and see where high-risk work is clustering.

---

### 5. Analytics mart with dbt

Next, we used **dbt** to turn the scored incident table into a business-friendly analytics mart:

* `stg_issues` – a cleaned staging view on top of the scored data.
* `mart_issues_by_region_day` – daily metrics per region, including:

  * total incidents,
  * SLA breach rate,
  * average risk score,
  * share of high-risk tickets,
  * counts per risk bucket.

This mart is what both the Streamlit app and Power BI report query; it answers “how are we doing over time and across regions?” in one place.

---

### 6. Orchestration with Airflow

All heavy lifting is automated with an **Airflow DAG**:

1. Load new streamed incidents into BigQuery.
2. Score them with the trained BigQuery ML model.
3. Append to the main scored table.
4. Run **dbt** to refresh the mart.

This gives you a repeatable **daily workflow** instead of a one-off notebook.

---

### 7. What you see in the Streamlit app

The Streamlit UI sits on top of those tables and exposes the analysis:

* **Overview cards** – total incidents, overall SLA breach rate, average risk score, and high-risk share for the selected date range.
* **Trends over time** – line charts of incidents and breach rate by day.
* **Region comparison** – which regions have higher breach rates and risk.
* **Recent high-risk incidents** – table of tickets the model thinks are most likely to breach.

On top of the charts, an **LLM section** generates narrative insights such as:

> “Over the last 7 days, Queens shows the highest SLA breach rate, mainly driven by noise complaints logged after 10pm. Manhattan has high volume but lower relative risk.”

This gives non-technical stakeholders a plain-language summary of what’s happening.

---

### 8. Power BI dashboard

Finally, a **Power BI report** built on `mart_issues_by_region_day` provides:

* KPI tiles (total incidents, breach rate, average risk).
* Time-series of incidents vs breach rate.
* Bar charts of risk by region.
* 100% stacked bars showing the mix of risk buckets per region.
* A detail table for export.

Power BI is used for interactive analysis and “slide-ready” visuals, while Streamlit focuses on the operational view and ML/LLM insights.



        """
    )

    # ------------- Sidebar: pipeline + filters -------------

    st.sidebar.header("Pipeline & Filters")

    if st.sidebar.button("▶ Run daily pipeline"):
        with st.spinner("Triggering Airflow pipeline and waiting for it to finish..."):
            success, state = trigger_airflow_dag_and_wait()
        if success:
            st.sidebar.success("Pipeline finished successfully.")
            st.cache_data.clear()  # refresh cached BigQuery data
        else:
            st.sidebar.error(f"Pipeline run did not succeed (state={state}).")

    today = date.today()
    default_start = today - timedelta(days=30)

    start_date = st.sidebar.date_input("Start date", default_start)
    end_date = st.sidebar.date_input("End date", today)

    if start_date > end_date:
        st.sidebar.error("Start date must be before end date.")
        return

    # ------------- Load data -------------

    mart_df = load_mart(start_date, end_date)
    issues_df = load_issues_sample(start_date, end_date)

    # ------------- Overview KPIs -------------

    st.subheader("Overview")

    if mart_df.empty:
        st.warning("No data for this date range.")
    else:
        total_issues = mart_df["total_issues"].sum()

        # Weighted overall SLA breach rate
        if total_issues > 0:
            overall_breach_rate = (
                (mart_df["sla_breach_rate"] * mart_df["total_issues"]).sum()
                / total_issues
            )
        else:
            overall_breach_rate = 0.0

        avg_risk_score = mart_df["avg_risk_score"].mean()
        avg_high_risk_share = mart_df["high_risk_share"].mean()

        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Total issues", f"{total_issues:,}")
        col2.metric("Overall SLA breach rate", f"{overall_breach_rate:.1%}")
        col3.metric("Avg risk score", f"{avg_risk_score:.2f}")
        col4.metric("High-risk share (avg)", f"{avg_high_risk_share:.1%}")

        # ------------- Trend chart -------------

        st.subheader("Trends over time")

        if "issue_date" in mart_df.columns:
            trend_df = (
                mart_df.groupby("issue_date", as_index=False)
                .agg(
                    total_issues=("total_issues", "sum"),
                    avg_sla_breach_rate=("sla_breach_rate", "mean"),
                )
                .sort_values("issue_date")
            )
            st.line_chart(
                trend_df.set_index("issue_date")[["total_issues", "avg_sla_breach_rate"]]
            )
        else:
            st.info("No issue_date column in mart for trend chart.")

        # ------------- Region chart -------------

        if "region" in mart_df.columns:
            st.subheader("SLA breach rate by region")
            region_df = (
                mart_df.groupby("region", as_index=False)
                .agg(
                    total_issues=("total_issues", "sum"),
                    avg_sla_breach_rate=("sla_breach_rate", "mean"),
                )
                .sort_values("avg_sla_breach_rate", ascending=False)
            )
            st.bar_chart(
                region_df.set_index("region")[["avg_sla_breach_rate"]]
            )

    # ------------- Detailed issues table -------------

    st.subheader("Recent issues (sample from results table)")

    if not issues_df.empty:
        st.dataframe(
            issues_df,
            use_container_width=True,
            hide_index=True,
        )
    else:
        st.info("No issues in this date range.")

    # ------------- LLM Insights -------------

    st.subheader("🧠 LLM Insights on Results")

    if st.button("Generate insights for current results"):
        with st.spinner("Asking the LLM to analyze the mart..."):
            insights = generate_llm_insights(mart_df, start_date, end_date)
        st.markdown(insights)

    # ------------- Power BI embed -------------

         # ------------- Power BI dashboard embed -------------

    st.subheader("📊 Power BI SLA Dashboard")

    st.markdown(
        "This dashboard is built in Power BI on top of "
        "`signalsense_dbt.mart_issues_by_region_day`."
    )

    PBI_EMBED_URL = os.getenv(
        "POWER_BI_EMBED_URL",
        "https://app.powerbi.com/view?r=YOUR_PUBLISH_TO_WEB_URL",
    )

    if PBI_EMBED_URL and "YOUR_PUBLISH_TO_WEB_URL" not in PBI_EMBED_URL:
       st.markdown(f"[Open Power BI dashboard in a new tab]({PBI_EMBED_URL})")

    else:
        st.info(
            "Set POWER_BI_EMBED_URL in your environment or .env file "
            "to embed the Power BI dashboard here."
        )




if __name__ == "__main__":
    main()
