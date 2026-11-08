# 🚀 End-to-End Data Engineering Pipeline Using Azure Databricks

## 📌 Project Overview

This project demonstrates the development of an end-to-end ETL pipeline using **Azure Databricks Lakeflow Spark Declarative Pipelines (formerly Delta Live Tables)**.

The pipeline follows the **Medallion Architecture (Bronze, Silver, and Gold)** to ingest, validate, transform, and aggregate data for analytical purposes.

It incorporates incremental data ingestion using Auto Loader, streaming tables, data quality expectations, Slowly Changing Dimensions (SCD Type 1 and Type 2), materialized views, and automated pipeline orchestration using Databricks Workflows.

The project also demonstrates event-driven processing through file arrival triggers configured on Unity Catalog Volumes.

---

## 🏗️ Architecture

The pipeline is organized into three layers:

### 1. Bronze Layer – Data Ingestion

The Bronze layer handles raw data ingestion from multiple sources.

**Implemented components:**
- Ingest order data from Delta tables.
- Ingest CSV files incrementally using Databricks Auto Loader.
- Create streaming tables for incoming data.
- Combine multiple order streams using append flows.
- Implement data quality expectations.
- Maintain customer records using SCD Type 1 and SCD Type 2.

**Main tables:**
- `orders_bronze`
- `orders_autoloader_bronze`
- `orders_union_bronze`
- `customers_bronze_scd1`
- `customers_bronze_scd2`

### 2. Silver Layer – Data Transformation

The Silver layer transforms and enriches the ingested data.

**Implemented components:**
- Create intermediate temporary views.
- Join orders and customer datasets.
- Maintain enriched order and customer information.
- Create materialized views for downstream processing.

**Main table:**
- `joined_silver`

### 3. Gold Layer – Business Aggregation

The Gold layer prepares aggregated datasets for analytical consumption.

**Implemented components:**
- Aggregate total orders.
- Calculate total revenue.
- Group results by customer market segment.
- Generate separate aggregations based on order status.

**Main tables:**
- `orders_agg_gold`
- `orders_agg_gold_F`
- `orders_agg_gold_O`
- `orders_agg_gold_P`

---

## ⚙️ Technologies Used

| Technology | Purpose |
|---|---|
| Azure Databricks | Data engineering platform |
| PySpark | Data processing and transformations |
| Lakeflow Spark Declarative Pipelines | Pipeline development |
| Delta Lake | Reliable data storage |
| Auto Loader | Incremental file ingestion |
| Unity Catalog | Data governance and Volumes |
| Streaming Tables | Incremental data processing |
| Materialized Views | Transformed and aggregated data |
| SCD Type 1 | Maintain latest customer records |
| SCD Type 2 | Maintain customer history |
| Databricks Workflows | Pipeline orchestration |
| File Arrival Trigger | Event-driven pipeline execution |

---

## 📂 Project Structure

```text
databricks-lakeflow-medallion-etl/
│
├── datasets/
│   ├── orders/
│   └── customers/
│
├── explorations/
│   └── main_dlt_exp.py
│
├── transformations/
│   └── my_transformation.py
│
├── screenshots/
│   ├── pipeline_architecture.png
│   └── workflow_execution.png
│
├── README.md
└── requirements.txt
```

*Note: Add the actual dataset files to the datasets directory before publishing. The current notebooks use Databricks sample datasets and a Databricks-provided retail CSV file.*

---

## 🔄 Data Pipeline Execution

The pipeline follows this execution sequence:

1. Create the source catalog and schema.
2. Load initial order and customer data.
3. Configure Auto Loader to monitor the landing directory.
4. Ingest data into Bronze streaming tables.
5. Apply data quality expectations.
6. Maintain customer records using SCD Type 1 and Type 2.
7. Combine and transform order and customer data in Silver.
8. Generate business aggregations in Gold.
9. Configure Databricks Workflows for orchestration.
10. Execute the pipeline automatically through a file arrival trigger.

---

## ⚡ Automated Pipeline Orchestration

Implemented event-driven execution using Databricks Workflows.

**Trigger configuration:**
- Trigger type: File Arrival
- Storage location: Unity Catalog Volume
- Monitored directory: `/Volumes/dlt/etl/landing/files/`
- Workflow: `dlt_pipeline_run`

Whenever a new file arrives in the monitored directory, the configured trigger can automatically initiate the workflow and execute the DLT pipeline.

This reduces manual intervention and supports automated incremental data ingestion.

---

## 🧪 Data Quality

Implemented data quality expectations to validate incoming data.

**Order validation rules:**
- Order status must be one of F, O, or P.
- Order total price must be greater than zero.

**Customer validation rules:**
- Customer market segment must not be NULL.

These expectations help maintain data quality during pipeline processing.

---

## 📊 Key Learning Outcomes

Through this project, I gained hands-on experience in:

- Designing an end-to-end ETL pipeline.
- Implementing the Medallion Architecture.
- Incremental data ingestion using Auto Loader.
- Building streaming tables and append flows.
- Implementing SCD Type 1 and SCD Type 2.
- Applying data quality expectations.
- Building materialized views.
- Performing joins and aggregations using PySpark.
- Automating pipelines using Databricks Workflows.
- Implementing event-driven processing using file arrival triggers.

---


## 👨‍💻 Author

**Data Engineering Project**

Built as a hands-on learning project to demonstrate practical data engineering skills using Azure Databricks, PySpark, Delta Lake, and Lakeflow Spark Declarative Pipelines.

---

⭐ If you find this project useful, feel free to explore the code and share your feedback!

#AzureDatabricks #DataEngineering #PySpark #DeltaLake #ETL #Lakeflow #AutoLoader
