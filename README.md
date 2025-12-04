# SignalSense – 311 Incident SLA Risk Platform

SignalSense is an end-to-end data engineering project that turns raw 311 service request data into real time SLA risk insights.

The platform:

- Cleans and enriches ~1M+ NYC style 311 incidents
- Streams new incidents through **Kafka** into **BigQuery**
- Predicts SLA breach risk with **BigQuery ML** (logistic regression, boosted trees, DNN)
- Builds analytics marts with **DBT**
- Orchestrates the daily workflow with **Airflow**
- Serves an operational UI with **Streamlit** and a BI view with **Power BI** and **LLM (GPT-5-mini) Insights**

---

## 1. Problem & Goal

City 311 systems receive thousands of complaints per day (noise, sanitation, streetlights, etc.).  
Operations teams care about two questions:

1. **Which tickets are most likely to miss their SLA?**  
2. **Where and when are SLA breaches clustering across the city?**

SignalSense answers these by assigning each incident a **predicted SLA breach risk** and aggregating that risk by **region** and **day** for monitoring.

---

## 2. Data & Pre-processing 

Source: open 311 dataset (~1M rows) with columns such as created date, closed date, agency, complaint type, description, and location.

Processing in `prep_data.py`:

- Parse and standardize timestamps (`created_date`, `closed_date`)
- Compute:

  - `resolution_hours` = time from creation to closure  
  - `sla_breached` = 1/0 label based on an SLA threshold

- Normalize location information into a `region` (borough) field
- Drop obviously broken rows and handle missing values

### NLP features on complaint text

The raw data contains free text fields such as **Complaint Type**, **Descriptor** and **Resolution Description**.  
In `prep_data.py` these are combined and turned into simple NLP features:

- Build `raw_text` by concatenating complaint-type, descriptor and resolution text
- Clean the text:
  - lower casing
  - removing punctuation, digits and extra whitespace
  - removing common stopwords (e.g. “the”, “and”, “please”, “address”)
- Tokenize into words and create:
  - `text_len` (number of characters)
  - `word_count` (number of tokens)
- Extract a lightweight **topic keyword** per incident:
  - compute term frequencies on the cleaned tokens
  - pick the most informative non-stopword token as `issue_keyword`  
    (e.g. “noise”, “pothole”, “heat”, “tree”, “garbage”)

This is intentionally a **simple NLP pipeline** (no deep models) that still captures *what the complaint is about* and can be used as a categorical feature in ML and for filtering in the UI.

The cleaned, enriched dataset is written to BigQuery as **`issues_enriched`** and used by all downstream components.

---

## 3. Streaming Ingestion (Kafka → BigQuery)

To simulate near real time operations:

- A **Kafka producer** writes new incident events to a topic.
- A **Kafka consumer** reads events, applies the same transforms as batch
  (including the simple NLP keyword step), and inserts into `issues_enriched_stream`
  in **BigQuery**.
- SQL merges the stream into `issues_enriched`.

This shows how the platform can keep up with ongoing tickets, not just historical snapshots.

---

## 4. SLA Risk Modelling with BigQuery ML

On top of `issues_enriched`, a supervised model predicts whether a ticket will breach its SLA.

- **Label:** `sla_breached`
- **Features:** region, time of day, complaint type, NLP `issue_keyword`, text length, etc.
- **Models tried:**
  - Logistic regression
  - Boosted tree classifier
  - DNN classifier

Models are trained and evaluated with `ML.TRAIN` / `ML.EVALUATE`.  
The **DNN classifier** achieved the best accuracy / ROC AUC, so it was selected as the production model.

Predictions are written back to BigQuery in `issues_scored`:

- `sla_breach_score` – probability of SLA breach (0–1)
- `risk_bucket` – human friendly risk band (very_low / low / medium / high)

These fields are the core analysis output used by the UI and dashboards.

---

## 5. Analytics Mart with DBT

DBT provides the semantic layer on top of the scored incidents:

- `stg_issues` – staging view over `issues_scored`
- `mart_issues_by_region_day` – daily grain mart per region with:
  - `issue_date`
  - `region`
  - `total_issues`
  - `sla_breach_rate`
  - `avg_risk_score`
  - `high_risk_share`
  - counts per risk bucket (`cnt_very_low`, `cnt_low`, `cnt_medium`, …)

Both Streamlit and Power BI query this mart to answer:
> “How are we doing over time and across regions?”

---

## 6. Orchestration with Airflow

A daily **Airflow DAG** wires everything together:

1. Load new streamed incidents into BigQuery.
2. Score new records with the DNN model using `ML.PREDICT`.
3. Append to `issues_scored`.
4. Run `dbt run` to refresh `mart_issues_by_region_day`.

This turns the project from a one off notebook into a repeatable, schedulable data pipeline.

---

## 7. Streamlit UI with LLM Insights

The `app/` folder contains a **Streamlit** app that:

- Lets the user select a date range.
- Shows overview KPIs:
  - total incidents
  - overall SLA breach rate
  - average risk score
  - share of high risk tickets
- Plots:
  - incidents vs breach rate over time
  - risk by region
  - risk-bucket distribution
- Lists recent **high risk** incidents.


LLM Insights (Narrative Analysis)
The UI includes an AI-powered narrative generator using an LLM (OpenAI GPT-5-mini).  
The model receives:
 - aggregated metrics from the dbt mart  
 - region-level risk trends  
 - risk-bucket distributions  
 - a snapshot of recent high-risk incidents  

The LLM produces a **human-readable summary** answering questions like:
 - *“Which regions show emerging SLA risk?”*  
 - *“What operational patterns stand out this week?”*  
 - *“Which issue types or keywords are driving high-risk incidents?”*  
 - *“How does this period compare to the previous one?”* 

This is the operational view meant for non technical stakeholders.

---

## 8. Power BI Dashboard

A separate **Power BI** report is built on `mart_issues_by_region_day` with:

- KPI cards (total incidents, breach rate, risk)
- Time series of incidents vs breach rate
- Region comparison charts
- 100% stacked bars for risk-bucket mix
- Detail table for export


---

## 9. Tech Stack

- **Language:** Python (ETL, Kafka producer/consumer, Streamlit)
- **Data warehouse:** Google BigQuery
- **ML:** BigQuery ML (logistic regression, boosted tree, DNN)
- **Stream processing:** Apache Kafka
- **Orchestration:** Apache Airflow (Docker)
- **Transformations / marts:** dbt (BigQuery adapter)
- **UI:** Streamlit
- **Dashboard:** Power BI
- **LLM:** GPT-5-mini

> **Note:** Service account keys, `.env` files and other credentials are **not** included in this repo.  
> To run the project yourself you will need your own GCP project, BigQuery dataset and service account with appropriate permissions.
