CREATE DATABASE ADVENTUREWORKS;
USE ADVENTUREWORKS;

CREATE TABLE dim_customers (
    customerkey BIGINT,
    birthdate DATE,
    maritalstatus VARCHAR(50),
    gender VARCHAR(10),
    emailaddress VARCHAR(255),
    annualincome INT,
    totalchildren BIGINT,
    educationlevel VARCHAR(100),
    occupation VARCHAR(100),
    homeowner VARCHAR(50),
    Age INT,              
    Currency VARCHAR(10), 
    FullName VARCHAR(256) 
)
DISTKEY (customerkey)
SORTKEY(annualincome, Age);


COPY dim_customers
FROM 's3://your_s3_bucket'
IAM_ROLE 'your_arn_url/your_role'
FORMAT AS PARQUET;

select * from dim_customers limit 10;

CREATE TABLE dim_product_categories(
    productcategorykey BIGINT,
    categoryname VARCHAR(256)
)
DISTKEY(productcategorykey);

COPY dim_product_categories
FROM 's3://your_s3_bucket'
IAM_ROLE 'your_arn_url/your_role'
FORMAT AS PARQUET;

select * from dim_product_categories limit 10;

CREATE TABLE dim_product_subcategories(
    productsubcategorykey BIGINT,
    subcategoryname VARCHAR(256),
    productcategorykey BIGINT
)
DISTKEY(productsubcategorykey);

COPY dim_product_subcategories
FROM 's3://your_s3_bucket'
IAM_ROLE 'your_arn_url/your_role'
FORMAT AS PARQUET;

select * from dim_product_subcategories limit 10;

CREATE TABLE dim_products(
    productkey BIGINT,
    productsubcategorykey BIGINT,
    productsku VARCHAR(256),
    productname VARCHAR(256),
    modelname VARCHAR(256),
    productdescription VARCHAR(256),
    productcolor VARCHAR(256),
    productsize VARCHAR(256),
    productstyle VARCHAR(256),
    productcost DOUBLE PRECISION,
    productprice DOUBLE PRECISION
)
DISTKEY(productkey)
SORTKEY(productcost, productprice);

COPY dim_products
FROM 's3://your_s3_bucket'
IAM_ROLE 'your_arn_url/your_role'
FORMAT AS PARQUET;

select * from dim_products limit 10;

CREATE TABLE dim_territories(
    salesterritorykey BIGINT,
    region VARCHAR(256),
    country VARCHAR(256),
    continent VARCHAR(256)
)
DISTKEY(salesterritorykey);

COPY dim_territories
FROM 's3://your_s3_bucket'
IAM_ROLE 'your_arn_url/your_role'
FORMAT AS PARQUET;

select * from dim_territories limit 10;

CREATE TABLE sales(
    orderdate DATE,
    stockdate DATE,
    ordernumber VARCHAR(256), 
    productkey BIGINT,
    customerkey BIGINT,
    territorykey BIGINT,
    orderlineitem BIGINT,
    orderquantity BIGINT
);

COPY sales
FROM 's3://your_s3_bucket'
IAM_ROLE 'your_arn_url/your_role'
FORMAT AS PARQUET;

select * from sales limit 10;

CREATE TABLE fact_sales AS
SELECT 
    s.ordernumber,
    s.orderdate,
    s.stockdate,
    s.productkey,
    s.customerkey,
    s.territorykey,
    s.orderlineitem,
    s.orderquantity,
    round(p.productcost) as unitcost,
    round(p.productprice) as unitprice,
    (s.orderquantity * round(p.productprice)) as totalamount,
    (s.orderquantity * round(p.productcost)) AS totalcost
FROM sales s
LEFT JOIN dim_products p ON p.productkey = s.productkey;

select * from fact_sales limit 10;
-- Drop staging table
DROP TABLE sales;
