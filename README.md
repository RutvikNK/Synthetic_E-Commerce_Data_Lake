# Synthetic E-Commerce Data Lake & ELT Platform

A complete end-to-end data engineering platform that generates synthetic e-commerce traffic, ingests it into a serverless Data Lake, and performs daily ELT transformations using Modern Data Stack tools.

## 🏗 Architecture

**Event-Driven Pipeline:**
`Python Producer` ➔ `Google Pub/Sub` ➔ `Cloud Functions` ➔ `GCS (Data Lake)` ➔ `BigQuery` ➔ `dbt` ➔ `Looker Studio`

**Key Components:**
* **Ingestion:** Streaming ingestion using **Google Pub/Sub** and **Cloud Functions (Gen 2)**.
* **Storage:** Raw JSON logs stored in **Google Cloud Storage (GCS)**, partitioned by date (`year=YYYY/month=MM/day=DD`).
* **Transformation:** **dbt (data build tool)** models to clean, type-cast, and aggregate raw data into analytical Fact tables.
* **Orchestration:** **Apache Airflow** DAGs to schedule and monitor the daily transformation pipelines.
* **Resilience:** Implemented **Dead Letter Queue (DLQ)** pattern to automatically quarantine malformed JSON data without stopping the ingestion pipeline.
* **IaC:** Full infrastructure provisioning using **Terraform**.

## 🛠 Tech Stack

* **Language:** Python 3.12
* **Cloud:** Google Cloud Platform (GCP)
* **Infrastructure:** Terraform
* **Transformation:** dbt Core
* **Orchestration:** Apache Airflow
* **Package Manager:** uv

## 🚀 Getting Started

### Prerequisites
* [Google Cloud SDK (gcloud)](https://cloud.google.com/sdk/docs/install) installed and authenticated.
* [Terraform](https://developer.hashicorp.com/terraform/install) installed.
* [uv](https://github.com/astral-sh/uv) installed (Fast Python package manager).

### 1. Clone the Repository
```bash
git clone https://github.com/RutvikNK/Synthetic_E-Commerce_Data_Lake.git
cd synthetic-ecommerce-lake
```

### 2. Infrastructure Setup (Terraform)

Provision the GCP resources (Bucket, Pub/Sub Topic, BigQuery Dataset).

```bash
cd infra
# Initialize Terraform
terraform init

# Apply configuration (Type 'yes' to confirm)
terraform apply
```

**Important:** Copy the `bucket_name` from the Terraform output. You will need it for the next step.

### 3. Deploy Ingestion Function

Deploy the serverless function that moves data from Pub/Sub to GCS.

```bash
# Ensure you are in the root directory (go back up from infra/)
cd ..

# Replace YOUR_BUCKET_NAME with the actual name from Terraform
gcloud functions deploy ingest-ecommerce-events \
    --gen2 \
    --runtime=python311 \
    --region=us-central1 \
    --source=./src/ingestion \
    --entry-point=ingest_event \
    --trigger-topic=ecommerce-events \
    --set-env-vars=BUCKET_NAME=YOUR_BUCKET_NAME,QUARANTINE_BUCKET=YOUR_BUCKET_NAME-quarantine
```

## 🏃‍♂️ Running the Pipeline

### Step 1: Start the Data Generator

This script simulates user traffic (purchases, page views) and sends it to Pub/Sub.

```bash
# Install dependencies
uv sync

# Run the producer
uv run src/generator/producer.py
```

*Leave this terminal open. Data is now flowing.*

### Step 2: Configure dbt (Transformation)

Set up dbt to talk to your BigQuery dataset.

```bash
# Install dbt-bigquery as an isolated tool
uv tool install dbt-core --with dbt-bigquery

# Initialize profile (select 'bigquery', 'oauth', and region 'us-central1')
dbt init transformation
```

### Step 3: Start Airflow (Orchestration)

Launch the Airflow Scheduler and Webserver to automate the dbt runs.

```bash
cd orchestration

# Start Airflow in standalone mode
uv run airflow standalone
```

* **Login:** Open `http://localhost:8080`.
* **Credentials:** Check the terminal output for the `admin` password.
* **Action:** Unpause the `ecommerce_daily_transform` DAG to start the pipeline.

## 🧪 Testing & Quality Assurance

The project includes a suite of unit tests for the generator and ingestion logic, as well as resilience tests for the Dead Letter Queue.

```bash
# Run the test suite
uv run pytest
```

### Chaos Engineering (Simulate Errors)

The producer script intentionally sends "poison pill" (malformed) data 1% of the time.

1. Check the **Quarantine Bucket** in Google Cloud Console (`gs://YOUR_BUCKET-quarantine/failed/`).
2. You should see files appearing there. This proves the pipeline is resilient and does not crash on bad data.

## 🧹 Cleanup

To avoid incurring cloud costs, destroy the infrastructure when finished.

```bash
cd infra
terraform destroy
```

## 📂 Project Structure

```text
├── infra/                  # Terraform Infrastructure as Code
├── orchestration/          # Airflow DAGs and Config
│   └── dags/               # Pipeline definitions (Python)
├── src/
│   ├── generator/          # Python Data Producer (Fake traffic)
│   └── ingestion/          # Cloud Function source code
├── transformation/         # dbt Project (SQL Models)
│   ├── models/             # Staging and Mart tables
│   └── seeds/              # Static data
├── tests/                  # Pytest Unit Tests
└── pyproject.toml          # Python Dependencies
```
