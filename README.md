# Graduation Project

## Overview

This graduation project focuses on the design and implementation of an ETL process for loading, processing, and consolidating data from heterogeneous sources.

The project implements a modern **Data Lakehouse architecture** based on **AWS S3**, **Apache Iceberg**, **Apache Airflow**, and **ClickHouse**. The main goal is to build a reliable data pipeline that extracts raw data from external sources, stores it in a scalable object storage layer, processes it using lakehouse principles, and prepares it for analytical consumption.

The final processed data is planned to be used for analytics and reporting through **Power BI**.

---

## Project Architecture

![Data Infrastructure](resources/img/architecture.png)

The architecture consists of the following main components:

### 1. Apache Airflow

Apache Airflow is used as the orchestration layer of the project.

It is responsible for:

- scheduling ETL pipelines;
- managing task dependencies;
- monitoring pipeline execution;
- retrying failed tasks;
- providing visibility into the data loading process.

Airflow DAGs describe the full data lifecycle: extraction, loading, transformation, validation, and delivery to analytical storage.

---

### 2. Data Sources

The project supports loading data from external heterogeneous sources.

At the current stage, the data extraction layer is designed to work with source systems such as:

- APIs;
- structured files;
- semi-structured data formats;
- external web-based sources.

The extracted data is first loaded into the raw storage layer without heavy transformations.

---

### 3. AWS S3 Data Lake

AWS S3 is used as the main storage layer for raw and processed data.

It acts as a scalable and cost-efficient Data Lake where data can be stored in different formats and processing stages.

The storage layer may include several logical zones:

- **Raw zone** — original extracted data;
- **Staging zone** — cleaned and prepared data;
- **Curated zone** — validated and analytics-ready datasets.

This approach makes the pipeline more transparent, reproducible, and easier to maintain.

---

### 4. Apache Iceberg

Apache Iceberg is used as the table format for managing data inside the Data Lakehouse.

It provides important features for analytical data processing, including:

- schema evolution;
- partition management;
- ACID-like table operations;
- historical snapshots;
- reliable data updates;
- better support for large analytical datasets.

Using Iceberg makes the data lake more structured and allows it to behave more like a traditional analytical database while still keeping the flexibility of object storage.

---

### 5. ClickHouse

ClickHouse is planned to be used as the analytical database for fast querying and reporting.

Processed and validated data from the lakehouse layer can be loaded into ClickHouse for:

- analytical queries;
- dashboarding;
- aggregated reporting;
- business intelligence workloads.

The final data model for ClickHouse is currently being evaluated. Possible options include fact and dimension tables, denormalized analytical tables, or a hybrid model depending on reporting requirements.

---

### 6. Power BI

Power BI is used as the reporting and visualization layer.

It connects to the analytical storage layer and allows end users to build dashboards, monitor key metrics, and analyze consolidated data.

---

## Repository Rules

The repository follows a branch-based development workflow.

| Branch | Description |
|---|---|
| `feature/*` | Used for developing new features, DAGs, connectors, transformations, and other improvements. |
| `fix/*` | Used for quick fixes, bug fixes, and small urgent changes. |
| `master` | Production-ready branch containing stable DAGs and project code. |

The `master` branch is protected.

Direct pushes to `master` are not allowed. Any changes must be merged through a pull request after successfully passing the GitHub Actions pipeline.

---

## CI/CD Rules

The project uses GitHub Actions to validate changes before they are merged into the protected `master` branch.

The pipeline may include checks such as:

- code syntax validation;
- DAG import validation;
- linting;
- unit tests;
- dependency checks;
- basic project structure validation.

Only changes that successfully pass the pipeline can be merged into the production branch.

---

## Data Infrastructure

The project implements a Data Lakehouse architecture for storing, processing, and managing data in AWS S3 using Apache Iceberg.

The general data flow is:

1. Data is extracted from external sources.
2. Apache Airflow orchestrates the ETL process.
3. Raw data is loaded into AWS S3.
4. Data is structured and managed using Apache Iceberg.
5. Processed data is prepared for analytical use.
6. Data is loaded into ClickHouse.
7. Power BI is used for reporting and visualization.

This architecture combines the scalability of a Data Lake with the reliability and analytical capabilities of a Data Warehouse.

---

## Current Project Status

At the current stage, the core data infrastructure design has been defined.

Implemented or planned components include:

- Airflow-based orchestration;
- raw data loading into AWS S3;
- Apache Iceberg integration for lakehouse storage;
- analytical layer based on ClickHouse;
- Power BI reporting layer;
- GitHub Actions validation pipeline;
- protected `master` branch workflow.

The ClickHouse data model is currently under evaluation and will be finalized based on analytical requirements and dashboard design.

---

## Project Goal

The main goal of the project is to demonstrate the full cycle of designing and implementing an ETL process for heterogeneous data sources.

The project covers:

- data extraction;
- data loading;
- data transformation;
- data consolidation;
- data storage design;
- workflow orchestration;
- analytical database preparation;
- BI reporting integration.

As a result, the project represents a practical example of a modern data engineering pipeline based on Data Lakehouse principles.
