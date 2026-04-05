# AWS Serverless ETL Pipeline

Event-driven ETL pipeline on AWS using Lambda, Glue, S3, and Redshift.
Infrastructure provisioned via AWS CDK (Python). Orchestrated by Apache Airflow running on MWAA.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA SOURCES                                    │
│    S3 File Drop  │  API Gateway (webhook)  │  Kinesis Stream (events)   │
└────────┬─────────┴──────────┬──────────────┴──────────┬─────────────────┘
         │                    │                          │
         ▼                    ▼                          ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      TRIGGER LAYER  (Lambda)                             │
│   S3 Event → Lambda (validate + classify)                               │
│   API Gateway → Lambda (ingest webhook payload → S3 raw)                │
│   Kinesis → Lambda (buffer + batch write to S3)                         │
└────────────────────────────┬────────────────────────────────────────────┘
                             │ triggers Glue job
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                     TRANSFORM LAYER  (AWS Glue)                          │
│   Glue Job (PySpark): schema validation, dedup, type casting            │
│   Glue Data Catalog: auto-crawled schema registry                       │
│   Output: Parquet, partitioned by source/date → S3 processed/           │
└────────────────────────────┬────────────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                      LOAD LAYER  (Redshift)                              │
│   COPY command via Lambda → Redshift Serverless                         │
│   Upsert via staging table + MERGE                                      │
│   Post-load validation (row count + null checks)                        │
└─────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                  ORCHESTRATION + MONITORING                               │
│   Apache Airflow (MWAA)  │  CloudWatch Logs  │  SNS Alerts  │  Lambda  │
└─────────────────────────────────────────────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────────────────────────────┐
│                   INFRASTRUCTURE  (AWS CDK Python)                        │
│   S3 buckets  │  Lambda functions  │  Glue jobs  │  IAM roles          │
│   Redshift Serverless  │  VPC  │  SNS topics  │  CloudWatch alarms     │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## Tech Stack

| Layer | Technology |
|---|---|
| Trigger | AWS Lambda (Python 3.12) |
| Transform | AWS Glue 4.0 (PySpark) |
| Storage | Amazon S3 (raw / processed / archive) |
| Load | Amazon Redshift Serverless |
| Orchestration | Apache Airflow (Amazon MWAA) |
| Infrastructure | AWS CDK (Python) |
| Monitoring | CloudWatch + SNS |
| CI/CD | GitHub Actions |

---

## Project Structure

```
aws-serverless-etl/
├── lambda/
│   ├── trigger/          # S3 event handler — validates and triggers Glue
│   ├── transformer/      # Post-Glue Lambda — loads to Redshift via COPY
│   └── notifier/         # SNS alert dispatcher
├── glue/
│   └── scripts/          # PySpark Glue job scripts
├── airflow/
│   └── dags/             # MWAA Airflow DAGs
├── cdk/
│   ├── bin/              # CDK app entry point
│   └── lib/              # Stack definitions
├── tests/
│   ├── unit/             # Lambda + Glue unit tests
│   └── integration/      # End-to-end pipeline tests
├── utils/                # Shared helpers (S3, Redshift, logging)
└── .github/workflows/    # CI/CD pipeline
```

---

## Setup

### Prerequisites
- AWS CLI configured (`aws configure`)
- Node.js >= 18 (for CDK CLI)
- Python 3.12
- CDK CLI: `npm install -g aws-cdk`

### 1. Install Dependencies
```bash
pip install -r requirements.txt
npm install -g aws-cdk
```

### 2. Bootstrap CDK (first time only)
```bash
cd cdk
cdk bootstrap aws://ACCOUNT_ID/us-east-1
```

### 3. Deploy Infrastructure
```bash
cdk deploy --all --require-approval never
```

### 4. Configure Environment
```bash
cp .env.example .env
# Fill in: Redshift endpoint, S3 bucket names, SNS topic ARN
```

### 5. Upload Glue Script to S3
```bash
aws s3 cp glue/scripts/transform_orders.py s3://YOUR_BUCKET/glue-scripts/
```

### 6. Trigger Pipeline Manually
```bash
# Drop a file into the raw S3 bucket — Lambda fires automatically
aws s3 cp sample_data/orders_sample.json s3://YOUR_RAW_BUCKET/orders/2024/01/15/
```

---

## Glue Job Features

- Schema enforcement via Glue Data Catalog
- Deduplication on `record_id` with latest-wins strategy
- PII field hashing (SHA-256) before write
- Output partitioned by `source / year / month / day`
- Parquet with Snappy compression

---

## Redshift Load Pattern

```sql
-- Staging table load via COPY
COPY staging.orders
FROM 's3://processed-bucket/orders/2024/01/15/'
IAM_ROLE 'arn:aws:iam::ACCOUNT:role/RedshiftS3Role'
FORMAT AS PARQUET;

-- Upsert into target
MERGE INTO public.orders
USING staging.orders AS src
ON public.orders.order_id = src.order_id
WHEN MATCHED THEN UPDATE SET ...
WHEN NOT MATCHED THEN INSERT ...;
```

---

## CDK Stacks

| Stack | Resources |
|---|---|
| `NetworkStack` | VPC, subnets, security groups |
| `StorageStack` | S3 buckets (raw, processed, archive, scripts) |
| `LambdaStack` | 3 Lambda functions + IAM roles + event triggers |
| `GlueStack` | Glue job, crawler, Data Catalog database |
| `RedshiftStack` | Redshift Serverless namespace + workgroup |
| `MonitoringStack` | CloudWatch alarms + SNS topic + dashboard |

Deploy individual stacks:
```bash
cdk deploy StorageStack
cdk deploy LambdaStack
```

---

## CI/CD (GitHub Actions)

On push to `main`:
1. Run unit tests (`pytest tests/unit/`)
2. CDK synth (validate all stacks)
3. Deploy to dev environment
4. Run integration smoke test

---

## Key Design Decisions

**Lambda over Glue for trigger logic** — Glue has a cold start of 2–3 minutes. Lambda fires in under 1 second for file validation and job kickoff. Glue handles the heavy PySpark work where startup cost is amortized.

**CDK over CloudFormation/Terraform** — CDK in Python lets the same team write infrastructure and application code in one language. Constructs are reusable, typed, and testable.

**Redshift Serverless over provisioned** — No cluster management. Auto-scales for bursty ETL loads. Cost scales to zero during idle periods.

**MWAA over Step Functions** — Complex dependency management, retry logic, and backfill are better handled in Airflow DAGs than Step Functions state machines for a pipeline this size.

---

## License

MIT
