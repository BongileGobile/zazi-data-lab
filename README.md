# Zazi Data Lab: Enterprise Real-Time Streaming & Lakehouse Pipeline

[![Zazi Data Lab CI](https://github.com/BongileGobile/zazi-data-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/BongileGobile/zazi-data-lab/actions/workflows/ci.yml)

**Architected by Bongile Gobile | GoNam Group PTY LTD**

An end-to-end, event-driven data engineering pipeline built entirely on open-source technologies. Simulates real-time financial transactions, applies South African 15% VAT enrichment, persists to PostgreSQL (OLTP) and LocalStack S3 Parquet (OLAP), and provisions cloud storage via Terraform Infrastructure-as-Code.

---

## Architecture Overview

```text
+-------------------+       +-------------------+       +-----------------------+
|  Event Producer   | ----> |   Apache Kafka    | ----> |   Stream Consumer     |
|  (producer.py)    |       |  (Event Broker)   |       |    (consumer.py)      |
+-------------------+       +-------------------+       +-----------------------+
                                      |                             |
                                      v                             +---> [ PostgreSQL ] (OLTP Store)
                             +-----------------+                    |
                             |     Kafdrop     |                    +---> [ LocalStack S3 ] (Parquet Lake)
                             | (Broker Web UI) |                                  |
                             +-----------------+                                  v
                                                                         +-----------------+
                                                                         |     Grafana     |
                                                                         | (Observability) |
                                                                         +-----------------+
                                                                                  ^
                                                                                  |
                                                                         [ Terraform (IaC) ]
```

### Core Architectural Decisions
* **Decoupled Ingestion & Processing:** Apache Kafka buffers incoming transaction spikes, ensuring zero data loss if downstream analytical sinks experience latency.
* **Dual-Sink Persistence (Lambda/Kappa Hybrid Pattern):**
  * **Hot Path (PostgreSQL):** Low-latency transactional storage optimized for real-time operational queries and Grafana dashboards.
  * **Cold/Analytical Path (S3 Parquet):** Columnar storage partitioned in AWS S3 (emulated locally via LocalStack and provisioned via Terraform) using `pyarrow` for cost-effective historical analytics.
* **Infrastructure-as-Code (IaC):** Cloud infrastructure is fully version-controlled using Terraform with path-style endpoint routing (`s3_use_path_style = true`) for zero-cost local emulation.
* **Self-Documenting Engineering:** Every script and infrastructure manifest strictly adheres to **"WHY-driven" documentation**, detailing the business and architectural rationale behind technical implementations.
* **Automated Quality Gates (CI/CD):** GitHub Actions runs automated dependency resolution and bytecode compilation (`py_compile`) in an isolated Ubuntu runner on every push to `main`.

---

## Technology Stack

| Layer | Technology | Purpose |
| :--- | :--- | :--- |
| **Message Broker** | Apache Kafka & Zookeeper | Distributed event streaming and topic management |
| **Stream Processing** | Python 3.12 (`kafka-python`, `pandas`) | Real-time ingestion, schema validation, and 15% VAT calculation |
| **Operational Sink** | PostgreSQL | ACID-compliant relational storage for live transaction state |
| **Analytical Data Lake** | LocalStack (AWS S3) & Apache Parquet | Cloud-emulated object storage with versioning enabled |
| **Infrastructure-as-Code** | Terraform v1.9.8 | Declarative provisioning of AWS storage resources |
| **Observability** | Grafana & Kafdrop | Real-time metrics visualization and Kafka topic/offset inspection |
| **CI/CD Automation** | GitHub Actions | Automated static analysis and code validation on every push |

---

## Repository Structure

```text
zazi-data-lab/
├── .github/
│   └── workflows/
│       └── ci.yml           # Automated GitHub Actions CI/CD validation pipeline
├── terraform/
│   └── main.tf              # Terraform IaC for S3 data lake & versioning
├── .gitignore               # Excludes venv, bytecode, and Terraform state files
├── docker-compose.yml       # Full multi-container infrastructure definition
├── producer.py              # Synthetic financial transaction event generator
├── consumer.py              # Stream processor, VAT transformer, and dual-sink loader
├── requirements.txt         # Pinned production Python dependencies
└── README.md                # System architecture and operational runbook
```

---

## Quickstart & Local Deployment

### 1. Prerequisites
* Docker & Docker Compose
* Python 3.12+
* Terraform CLI (`v1.9.8+`)

### 2. Spin Up Infrastructure
Launch Kafka, Zookeeper, PostgreSQL, LocalStack, Kafdrop, and Grafana in detached mode:
```bash
docker-compose up -d
```

### 3. Provision Infrastructure-as-Code (Terraform)
Initialize Terraform and provision the versioned analytical S3 data lake bucket against LocalStack:
```bash
cd terraform
terraform init
terraform apply -auto-approve
cd ..
```

### 4. Configure Python Environment & Run Pipeline
Create an isolated virtual environment, install dependencies, and run your producer and consumer streams:
```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

* **Terminal 1 — Event Producer:** `python producer.py`
* **Terminal 2 — Stream Consumer:** `python consumer.py`

---

## Service Endpoints

* **Kafdrop (Kafka UI):** `http://localhost:9000`
* **Grafana (Analytics Dashboard):** `http://localhost:3000`
* **PostgreSQL Operational DB:** `localhost:5432`
* **LocalStack S3 Endpoint:** `http://localhost:4566`
