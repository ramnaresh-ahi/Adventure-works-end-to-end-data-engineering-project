# Adventure Works Data Engineering Project

## 📋 Overview

End-to-end data pipeline demonstrating modern data engineering practices using AWS services. 
Extracts retail data from Kaggle, transforms it using AWS Glue, and loads into Redshift Serverless 
for analytics.

**Status:** ✅ Complete | **Timeline:** October 2025

---

# Problem Statement & Solution

## 📋 Problem Statement

### Business Challenge
Adventure Works needs to understand which products and customer segments generate the most profit to optimize marketing spend and product strategy.

### Key Questions
1. Which product categories have the highest profit margins?
2. Which customer demographics (age, income, gender) spend the most?
3. Which product-customer segment combinations are most profitable?
4. Which geographic regions perform best?

### Business Goal
Enable the sales team to make **data-driven decisions** on product focus and marketing budget allocation based on profitability, not just revenue.

---

## 💡 Solution Overview

### Key Metrics Calculated

| Metric | Formula | Business Use |
|--------|---------|--------------|
| **Profit** | Revenue - Cost | Identify profitable products |
| **Profit Margin %** | (Profit / Revenue) × 100 | Compare product profitability |
| **Average Order Value (AOV)** | Total Revenue / Order Count | Measure customer spending |
| **Customer Lifetime Value (CLV)** | Sum of all customer orders | Identify VIP customers |
| **Age Groups** | Bucket ages into 5 groups | Demographic analysis |
| **Income Brackets** | Low / Medium / High / Premium | Income-based segmentation |

---

## 🎯 Analytics Insights

### 1. Product Performance Analysis

**What We Analyze:**
- Revenue contribution by product category
- Profit margins across product lines
- Identify high-volume, low-margin products (optimization opportunities)
- Identify high-margin, low-volume products (growth opportunities)

**Key Findings:**
- Which categories drive the most profit (not just revenue)
- Products to promote vs. products to phase out
- Price elasticity and margin optimization opportunities
- Category-wise performance trends

---

### 2. Customer Segmentation by Demographics

**What We Analyze:**
- Spending patterns by age group
- Income bracket impact on purchase behavior
- Gender-based product preferences
- Geographic differences in customer value

**Key Findings:**
- High-value customer demographics (highest AOV & CLV)
- Age groups with best profit potential
- Income brackets most likely to purchase premium products
- Regional customer quality differences

---

### 3. Profitability Matrix (Product × Customer Segment)

**What We Analyze:**
- Which products sell best to which customer segments
- Product-segment combinations with highest margins
- Cross-selling opportunities within demographics
- Untapped market combinations

**Key Findings:**
- Premium products perform best with high-income customers
- Age group preferences by product category
- Geographic variations in product performance
- Marketing budget allocation by segment

---

### 4. Geographic Territory Performance

**What We Analyze:**
- Revenue and profit by country/region
- Regional market maturity (established vs. growth markets)
- Territory profitability and efficiency
- Geographic expansion opportunities

**Key Findings:**
- Top-performing territories by profit
- Underperforming regions needing attention
- Growth potential markets for expansion
- Regional profit margin differences

---

### 5. Customer Value & Segmentation

**What We Analyze:**
- Customer lifetime value distribution
- Purchase frequency patterns
- Repeat customer vs. new customer value
- Customer retention opportunities

**Key Findings:**
- VIP customers (high CLV, high frequency)
- At-risk customers (declining purchase patterns)
- High-value customer profile for targeting
- Customer retention strategy opportunities

---

## 🎯 Business Outcomes & Actions

### Immediate Actions Enabled

✅ **Product Strategy**
- Focus marketing on high-margin products instead of high-volume ones
- Identify and optimize low-margin product lines
- Allocate shelf space based on profitability, not just sales volume

✅ **Marketing Optimization**
- Target premium products to high-income segments
- Geographic marketing campaigns based on territory performance
- Age-group specific product promotions

✅ **Customer Management**
- VIP customer retention programs (highest CLV segment)
- Risk mitigation for declining customer segments
- Personalized offers based on demographic profile

✅ **Revenue Growth**
- Cross-sell opportunities within customer segments
- Market expansion in underperforming territories
- Product-mix optimization for maximum profitability

✅ **Budget Allocation**
- Data-driven marketing spend by product & segment
- Reduced spending on low-ROI combinations
- Investment in high-potential segments

---

## 🏗️ Architecture

[Insert architecture diagram here]

### Data Flow
1. **Data Ingestion** → AWS Lambda extracts from Kaggle API
2. **Data Discovery** → AWS Glue Crawler catalogs raw data
3. **Data Transformation** → Glue ETL job transforms using PySpark
4. **Data Warehouse** → Data loaded into Redshift Serverless
5. **Analytics Ready** → Star schema ready for BI tools

---

## 🛠️ Technologies & Services Used

| Layer | Technology | Purpose |
|-------|-----------|---------|
| **Data Ingestion** | AWS Lambda, Python | Extract Kaggle data |
| **Data Catalog** | AWS Glue Crawler | Schema discovery |
| **ETL Processing** | AWS Glue, PySpark | Transform & clean data |
| **Data Lake** | Amazon S3 | Store raw & transformed data |
| **Data Warehouse** | Redshift Serverless | Analytics-ready warehouse |
| **Infrastructure** | IAM, VPC, Security Groups | AWS configuration |
| **Language** | Python, SQL | Scripting & queries |

---

## 📊 Key Achievements

✅ **End-to-End Pipeline** - Complete data flow from source to warehouse  
✅ **Data Modeling** - Star schema with 5+ dimension tables  
✅ **Data Quality** - Handled schema mismatches, data type conversions  
✅ **Cloud Infrastructure** - Configured VPC, security groups, IAM roles  
✅ **Scalable Design** - Serverless architecture for cost optimization  
✅ **Problem Solving** - Debugged and resolved connectivity & data issues  

---

## 📁 Project Structure
```
adventure-works-data-engineering/
│
├── README.md 
├── ARCHITECTURE.md (Architecture & design decisions)
├── LICENSE
│
├── 01_data_ingestion/
│   ├── lambda_function.py (Data extraction from Kaggle)
│   ├── requirements.txt
│   └── README.md (Lambda setup instructions)
│
├── 02_data_discovery/
│   ├── glue_crawler_config.json
│   └── README.md (Crawler setup instructions)
│
├── 03_data_transformation/
│   ├── glue_etl_job.py (PySpark transformation script)
│   ├── glue_job_config.json
│   └── README.md (ETL job documentation)
│
├── 04_data_warehouse/
│   ├── redshift_schema.sql (DDL for dimension/fact tables and data loading)
│   └── README.md (Redshift setup)
│
├── 05_infrastructure/
│   ├── iam_policy.json (IAM roles & policies)
│   ├── security_groups.json
│   └── README.md (Infrastructure setup)
│
├── docs/
│   ├── architecture_diagram.png
│   ├── data_flow_diagram.png
│   └── schema_design.png
│
└── images/
    └── (screenshots of each stage)
```
See each folder's README for detailed setup instructions.

---

## 🚀 Quick Start

### Prerequisites
- AWS Account with appropriate permissions
- Python 3.8+
- AWS CLI configured

### Setup Steps

1. **Configure AWS Credentials**


2. **Deploy Lambda Function**
See `01_data_ingestion/README.md`

3. **Create Glue Crawler**
See `02_data_discovery/README.md`

4. **Deploy Glue ETL Job**
See `03_data_transformation/README.md`

5. **Create Redshift Tables**
See `04_data_warehouse/README.md`

---

## 📈 Data Model

### Star Schema Design

**Fact Table:** `fact_sales`
- Measures: order quantity, unit price, total amount, total cost
- Dimensions: customer, product, territory, date

**Dimension Tables:**
- `dim_customers` (10,000+ records)
- `dim_products` (500+ records)
- `dim_territories` (10+ records)
- `product_categories` & `product_subcategories`

---

## 🔑 Key Features

### 1. Data Ingestion (Lambda)
- Extracts Adventure Works dataset from Kaggle API
- Automatically loads to S3 `raw/` folder
- Scheduled execution via CloudWatch Events

### 2. Data Discovery (Glue Crawler)
- Automatically discovers schema from Parquet files
- Populates Glue Data Catalog
- Enables Athena & other AWS services to query data

### 3. Data Transformation (Glue ETL)
- Merges multiple sales tables
- Handles data type conversions (string ↔ date, int)
- Removes duplicates & null values
- Outputs Parquet format to S3 `transformed/` folder

### 4. Data Warehouse (Redshift)
- Implements star schema for analytics
- Optimized for OLAP queries
- Supports complex joins across dimensions

---

## 🎯 Challenges & Solutions

| Challenge | Solution |
|-----------|----------|
| Data type mismatches | Custom PySpark transformations with explicit casting |
| Schema discovery | AWS Glue Crawler automation |
| Merging multiple tables | DynamicFrameCollection to single DataFrame |
| S3 to Redshift loading | COPY commands with IAM role authentication |
| Networking issues | VPC security group & public endpoint configuration |

---

## 📊 Data Statistics

- **Total Raw Data:** ~500k+ records
- **Source Tables:** 10 tables (sales, customers, products, etc.)
- **Transformation Rules:** 20+ data quality checks
- **Final Records in Warehouse:** ~500k+ optimized rows
- **Compression:** Parquet format provides 80%+ compression vs CSV

---

## 💡 Design Decisions

### Why Star Schema?
✅ Optimized for analytical queries  
✅ Simplified joins  
✅ Fast aggregations  

### Why Serverless Redshift?
✅ Auto-scaling based on workload  
✅ Pay-per-use pricing  
✅ No cluster management  

### Why Parquet Format?
✅ Columnar storage for compression  
✅ Schema evolution support  
✅ Compatible with Spark & Athena  

---

## 🔐 Security & Permissions

- **IAM Roles:** Principle of least privilege
- **VPC Configuration:** Private subnets with security groups
- **Data Encryption:** S3 encryption enabled
- **Access Control:** Role-based access for Glue jobs

See `05_infrastructure/` for detailed IAM policies.

---

## 📚 Project Learnings

### AWS Services Mastered
- Lambda for serverless compute
- Glue for ETL orchestration
- S3 for data lake storage
- Redshift for data warehousing
- IAM for access management

### Data Engineering Concepts Applied
- ETL pipeline design
- Star schema modeling
- Data quality transformations
- Cloud infrastructure setup
- Troubleshooting & debugging

---

## 📝 Future Enhancements

- [ ] Add data quality metrics & monitoring
- [ ] Implement incremental loading (CDC)
- [ ] Create data lineage documentation
- [ ] Add cost optimization analysis
- [ ] Implement automated testing
- [ ] Add monitoring & alerting dashboard

---

## 📞 Contact & Questions

For questions about this project, [Connect with me on LinkedIn](https://www.linkedin.com/in/ramnaresh-ahirwar-77abc/)

---

## 📄 License

This project is licensed under the MIT License - see LICENSE file for details.

---

## 🙏 Acknowledgments

- Kaggle for Adventure Works dataset
- AWS documentation & best practices
- Open-source Python/PySpark communities


