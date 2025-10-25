# Architecture & Design Document

## System Architecture Diagram

[Diagram showing: Kaggle → Lambda → S3 Raw → Glue Crawler → Glue Catalog → Glue ETL → S3 Transformed → Redshift]

## Component Details

### 1. Data Ingestion Layer (Lambda)
- **Function:** Extract Kaggle dataset
- **Output:** Raw Parquet files in S3

### 2. Data Discovery Layer (Glue Crawler)
- **Crawler:** Scans S3 raw folder
- **Output:** Glue Catalog metadata
- **Frequency:** On-demand

### 3. Transform Layer (Glue ETL)
- **Input:** Glue Catalog tables
- **Processing:** PySpark transformations
- **Output:** Transformed Parquet in S3

### 4. Data Warehouse Layer (Redshift)
- **Cluster:** Serverless with 1 namespace
- **Schema:** Star schema (1 fact + 5 dimensions)
- **Loading:** COPY from S3

## Data Flow Diagram

[Detailed flow with data volumes & transformation steps]

## Design Decisions

### Why AWS Glue vs EMR?
- Glue is fully managed (no cluster management)
- Glue Crawler automates schema discovery
- Glue job scheduling built-in

### Why Redshift vs BigQuery/Snowflake?
- AWS ecosystem integration
- Cost-effective for this data volume
- Serverless option reduces overhead

## Scalability Considerations

- S3 partitioning strategy for large datasets
- Redshift distribution keys for query optimization
- Lambda timeout & memory configuration
