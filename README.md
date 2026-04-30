# IngestAI: Distributed Data Ingestion & Metadata Platform

This repository provides a containerized Big Data environment built around the Apache Hadoop ecosystem. It serves as a practical, foundational workspace for exploring scalable data pipelines, managing distributed metadata, and experimenting with transactional processing. 

The Python-based ingestion framework, IngestAI, is a work in progress and is currently being actively developed alongside the core infrastructure.

## 🚀 System Capabilities

The platform is engineered to handle complex data lifecycles with the following core capabilities:
*   **Distributed Storage**: Scalable HDFS cluster configured with two DataNodes. The system operates with a replication factor of 2 to ensure fault-tolerant data storage.
*   **Resource Management**: Global resource orchestration via YARN, allocating 4096 MB of memory and 4 cores per node.
*   **High-Performance Execution**: Integration of Apache Tez as the primary execution engine for low-latency DAG processing.
*   **Transactional Metadata**: Full ACID support enabled through Apache Hive 4.0 using `DbTxnManager`. Metadata is backed by a persistent PostgreSQL database.
*   **Optimized Querying**: Hive is configured with Vectorized Execution and a Cost-Based Optimizer (CBO) enabled for maximum performance.

## 📁 Repository Structure

```text
.
├── configs/                 # XML configurations (core-site, hdfs-site, hive-site, etc.)
├── lib/                     # External dependencies (e.g., PostgreSQL JDBC driver)
├── scripts/                 # [IN DEVELOPMENT] IngestAI Python/Bash automation scripts
├── test/                    # End-to-end integration tests
├── docker-compose.yml       # Orchestrates HDFS, YARN, Hive, Tez, and Postgres
└── pyproject.toml           # Python project configuration and dependency management
```

## 📋 Prerequisites

Before deploying the cluster, ensure your host machine has the following:
*   **Docker & Docker Compose** installed and running.
*   **Minimum 8GB RAM** allocated to Docker (Hadoop and Hive services are resource-intensive).
*   **Python 3.12** (Strict requirement `>=3.12, <3.13` as defined in `pyproject.toml`) for running the IngestAI framework.

## 🛠 Quick Start Setup

### 1. External Dependencies
The system requires the PostgreSQL JDBC driver to facilitate communication between the Hive Metastore and the database. Run the following command from the root of the project:

```bash
mkdir -p lib && curl -fL -o lib/postgresql-42.7.3.jar https://jdbc.postgresql.org/download/postgresql-42.7.3.jar
```

### 2. Infrastructure Deployment
Launch the full distributed stack using Docker Compose:

```bash
docker compose up -d
```
> **Note:** The NameNode container includes a strict health check that waits until HDFS safemode is turned OFF before allowing dependent services (like YARN and Hive) to start. It may take a few minutes for the cluster to fully initialize.

### 3. Python Environment Setup
To set up the **IngestAI** environment for local development:
```bash
# Assuming you are using a modern package manager like uv or standard pip
pip install -e .
pip install -e ".[dev]" # Installs Ruff, pre-commit, and MyPy
pre-commit install
```

---

## 🧪 System Validation

To verify the integration between the storage and compute layers, an end-to-end test suite is provided. Wait for the cluster to be fully healthy, then run:

```bash
./test/run_test.sh
```

This suite validates the connectivity between **HDFS**, **YARN**, and **HiveServer2** by executing a distributed `INSERT` operation using the **Tez engine** on an ACID-compliant table.

---

## 📂 Data Ingestion Framework [Development Phase]

> ⚠️ **Notice:** The **IngestAI** metadata-driven ingestion framework within the `scripts/` directory is currently in active development. It is an experimental toolkit leveraging `pandas`, `faker`, and `uvicorn` to handle synthetic data generation, automated HDFS namespace formatting, and cluster verification. Detailed documentation for individual scripts will be published once the framework stabilizes.

---

## ⚙️ Technology Stack
*   **Apache Hadoop 3.3.6** (HDFS & YARN)
*   **Apache Hive 4.0.1** (Server & Metastore)
*   **Apache Tez 0.10.2** (Execution Engine)
*   **PostgreSQL** (Metadata Backend)
*   **Python 3.12** (IngestAI Framework: Pandas, Faker, Uvicorn)
*   **Code Quality**: Ruff (Linting/Formatting), MyPy (Type Checking), Pre-commit