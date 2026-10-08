# Azure Databricks + Kafka + Airflow Data Engineering Project

## Project: Real-Time Retail Orders Analytics

A production-style portfolio project using:

- Azure Event Hubs (Kafka-compatible endpoint)
- Apache Kafka protocol
- Azure Databricks / Spark Structured Streaming
- ADLS Gen2
- Delta Lake
- Unity Catalog
- Apache Airflow
- Databricks Asset Bundles
- Python
- SQL
- Optional ML/AI extension

## Architecture

```text
Retail Order Producer
        |
        v
Azure Event Hubs (Kafka endpoint)
        |
        v
Azure Databricks Structured Streaming
        |
        v
ADLS Gen2 / Delta Lake
   Bronze -> Silver -> Gold
                    |
                    +--> Daily Revenue
                    +--> Product Analytics
                    +--> Customer Analytics
                    +--> Fraud/Anomaly candidates
        ^
        |
 Apache Airflow
        |
        +--> Trigger Databricks jobs
        +--> Validate data quality
        +--> Run daily aggregates
        +--> Monitor/retry
```

Azure Event Hubs exposes a Kafka-compatible endpoint, so Kafka producers/consumers can be used without operating a separate Kafka cluster.

## Repository structure

```text
azure-databricks-kafka-airflow/
├── README.md
├── requirements.txt
├── .gitignore
├── databricks.yml
├── config/
│   └── dev.yml
├── data/
│   ├── customers.csv
│   └── products.csv
├── producer/
│   └── kafka_order_producer.py
├── src/
│   ├── 01_stream_bronze.py
│   ├── 02_stream_silver.py
│   ├── 03_gold_aggregations.py
│   └── 04_data_quality.py
├── sql/
│   └── analytics.sql
├── airflow/
│   ├── dags/
│   │   └── retail_databricks_pipeline.py
│   ├── Dockerfile
│   └── requirements.txt
└── tests/
    └── test_transformations.py
```

## Azure resources

Create:

1. Resource Group
2. Azure Databricks workspace
3. ADLS Gen2 storage account
4. Event Hubs namespace
5. Event Hub named `retail-orders`
6. Unity Catalog catalog/schema/volume or external location
7. Optional Key Vault
8. Airflow environment:
   - Azure Container Apps / AKS / VM / managed Airflow service
   - For local development, Docker Compose can be used.

## Kafka/Event Hubs design

Event Hubs namespace:

```text
<namespace>.servicebus.windows.net:9093
```

Topic/Event Hub:

```text
retail-orders
```

The Databricks streaming job consumes the Kafka-compatible endpoint.

Do not commit connection strings, client secrets, SAS keys, or PATs to GitHub.

## Databricks setup

Recommended production pattern:

- Unity Catalog for governance
- Managed identity/service principal for Azure access
- Secret scope or workload identity for Kafka authentication
- ADLS Gen2 external location/volume
- Delta tables for Bronze/Silver/Gold

For a demo, set these Spark configurations as job parameters or secrets:

```text
KAFKA_BOOTSTRAP_SERVERS
KAFKA_TOPIC
CHECKPOINT_PATH
BRONZE_PATH
SILVER_PATH
GOLD_PATH
```

## Deploy with Databricks Asset Bundles

Install Databricks CLI and authenticate to your Azure Databricks workspace.

Validate:

```bash
databricks bundle validate -t dev
```

Deploy:

```bash
databricks bundle deploy -t dev
```

Run:

```bash
databricks bundle run -t dev retail_streaming_job
```

## Airflow

Airflow is used as the orchestration layer.

Example DAG:

```text
start
  |
  v
trigger_databricks_stream
  |
  v
run_data_quality
  |
  v
run_gold_aggregation
  |
  v
end
```

Airflow does not need to process every Kafka message itself. Databricks Structured Streaming performs continuous ingestion; Airflow handles orchestration, dependencies, scheduled maintenance jobs, quality checks and retries.

## Local producer

Install:

```bash
pip install kafka-python
```

Set environment variables:

```bash
export KAFKA_BOOTSTRAP_SERVERS="<namespace>.servicebus.windows.net:9093"
export KAFKA_USERNAME="$ConnectionString"
export KAFKA_PASSWORD="<event-hubs-connection-string>"
export KAFKA_TOPIC="retail-orders"
```

Run:

```bash
python producer/kafka_order_producer.py
```

## Interview explanation

"I designed a real-time retail data platform on Azure. Orders are published through the Kafka-compatible endpoint of Azure Event Hubs. Databricks Structured Streaming consumes the events and writes raw data to the Bronze Delta layer in ADLS Gen2. Silver applies schema validation, deduplication and business transformations. Gold produces customer, product and revenue aggregates. Apache Airflow orchestrates Databricks jobs, data quality checks and scheduled batch workloads. Unity Catalog provides governance and access control. The solution uses checkpoints and Delta transactions to provide reliable processing and prevent duplicate results."

## Production improvements

- Use Event Hubs Capture for replay/backup if required.
- Use schema registry/schema evolution controls.
- Use Unity Catalog external locations/volumes.
- Use managed identity instead of storage keys.
- Store secrets in Key Vault/secret management.
- Add dead-letter/quarantine handling.
- Add monitoring with Azure Monitor and Databricks system tables.
- Add MLflow for an ML extension.
- Add CI/CD using GitHub Actions.
