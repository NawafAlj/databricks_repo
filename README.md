# 🚀 GCS-to-Databricks Medallion Pipeline

A production-grade Data Engineering pipeline built on **Databricks** and **Google Cloud Platform (GCP)**. This project demonstrates a full **Medallion Architecture** (Bronze, Silver, Gold) with integrated GitHub version control, Unity Catalog governance, and secure cloud storage.



## 🏗️ Architecture Overview

The pipeline automates the ingestion, transformation, and modeling of CRM and ERP data:

1.  **Raw Layer (GCS)**: Landing zone for raw CSV files in Google Cloud Storage.
2.  **Bronze Layer**: Ingestion into Managed Delta Tables with schema enforcement and metadata injection.
3.  **Silver Layer**: Data cleaning, deduplication, and standardization (e.g., date formats, casing).
4.  **Gold Layer**: Dimensional modeling (Star Schema) joining CRM and ERP data for BI readiness.

## 🛠️ Technical Stack

* **Platform**: Databricks (Unified Analytics Platform)
* **Compute**: Spark 3.x Clusters (DBR 16.4+)
* **Storage**: Google Cloud Storage (GCS)
* **Governance**: Unity Catalog (3-level namespace: `catalog.schema.table`)
* **Version Control**: GitHub (Integrated with Databricks Repos)
* **Language**: PySpark (Python)

---

## 🔐 Security & Governance

This project implements a secure, governed cloud data environment:
* **Identity & Access Management (IAM)**: Utilizes a dedicated GCP Service Account with specific roles (`Storage Admin`) to grant Databricks secure access to the GCS bucket.
* **Unity Catalog**: Managed via **External Locations** and **Storage Credentials** to ensure data isolation and fine-grained access control.
* **Managed Locations**: Strategic use of `MANAGED` storage for the `nawaf` catalog to avoid `LOCATION_OVERLAP` errors and ensure the platform handles physical file placement.
