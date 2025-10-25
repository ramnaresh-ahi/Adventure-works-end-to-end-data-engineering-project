import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrameCollection
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame

# Script generated for node Custom Returns
def MyTransform_Returns(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, to_date

    spark = glueContext.spark_session
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("returndate", to_date(col("returndate"), "MM/dd/yyyy"))
    df = df.filter(col("returndate").isNotNull())

    dynf = DynamicFrame.fromDF(df, glueContext, "transformed")
    return DynamicFrameCollection({"CustomTransform0": dynf}, glueContext)

# Script generated for node Custom Customers
def MyTransform_Customers(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, to_date, year, current_date, lit, trim, regexp_replace, concat_ws
    from pyspark.sql.types import IntegerType

    spark = glueContext.spark_session
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("birthdate", to_date(col("birthdate"), "MM/dd/yyyy"))
    df = df.filter(col("birthdate").isNotNull())

    df = df.withColumn("Age", year(current_date()) - year(col('birthdate')))
    df = df.withColumn("Currency", lit('$'))
    df = df.withColumn("annualincome", trim(regexp_replace(col('annualincome'), '[\\$,]', '')))
    df = df.withColumn('annualincome', col('annualincome').cast(IntegerType()))
    df = df.withColumn('FullName', concat_ws(' ', col('prefix'), col('firstname'), col('lastname')))
    df = df.drop('prefix', 'firstname', 'lastname')

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed")
    return DynamicFrameCollection({"CustomTransform0": dynamic_frame}, glueContext)

# Script generated for node Customer calendar
def MyTransform_Calendar(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import col, to_date

    spark = glueContext.spark_session
    spark.conf.set("spark.sql.legacy.timeParserPolicy", "LEGACY")

    df = dfc.select(list(dfc.keys())[0]).toDF()
    df = df.withColumn("date", to_date(col("date"), "MM/dd/yyyy"))
    df = df.filter(col("date").isNotNull())

    dynamic_frame = DynamicFrame.fromDF(df, glueContext, "transformed")
    return DynamicFrameCollection({"CustomTransform0": dynamic_frame}, glueContext)

# Script generated for node Adventureworks_sales
def MyTransform_Sales(glueContext, dfc) -> DynamicFrameCollection:
    from awsglue.dynamicframe import DynamicFrame, DynamicFrameCollection
    from pyspark.sql.functions import to_date, col

    dfs = []
    for key in dfc.keys():
        dyf = dfc[key]
        df = dyf.toDF()
        dfs.append(df)

    merged_df = dfs[0]
    for df in dfs[1:]:
        merged_df = merged_df.unionByName(df)

    merged_df = merged_df.withColumn("orderdate", to_date(col("orderdate"), "MM/dd/yyyy")) \
                         .withColumn("stockdate", to_date(col("stockdate"), "MM/dd/yyyy"))

    merged_df = merged_df.filter(col("orderdate").isNotNull() & col("stockdate").isNotNull())

    merged_dyf = DynamicFrame.fromDF(merged_df, glueContext, "merged_sales")
    return DynamicFrameCollection({"merged_sales": merged_dyf}, glueContext)
     
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Adventureworks_calendar
Adventureworks_calendar_node1761037317577 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_calendar_csv", transformation_ctx="Adventureworks_calendar_node1761037317577")

# Script generated for node Adventureworks_product_subcategories
Adventureworks_product_subcategories_node1761037554551 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_product_subcategories_csv", transformation_ctx="Adventureworks_product_subcategories_node1761037554551")

# Script generated for node Adventureworks_territories
Adventureworks_territories_node1761037805262 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_territories_csv", transformation_ctx="Adventureworks_territories_node1761037805262")

# Script generated for node Adventureworks_product_categories
Adventureworks_product_categories_node1761037509945 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_product_categories_csv", transformation_ctx="Adventureworks_product_categories_node1761037509945")

# Script generated for node Adventureworks_sales_2016
Adventureworks_sales_2016_node1761037725254 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_sales_2016_csv", transformation_ctx="Adventureworks_sales_2016_node1761037725254")

# Script generated for node Adventureworks_sales_2015
Adventureworks_sales_2015_node1761037673976 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_sales_2015_csv", transformation_ctx="Adventureworks_sales_2015_node1761037673976")

# Script generated for node Adventureworks_customers
Adventureworks_customers_node1761037448255 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_customers_csv", transformation_ctx="Adventureworks_customers_node1761037448255")

# Script generated for node Adventureworks_products
Adventureworks_products_node1761037592653 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_products_csv", transformation_ctx="Adventureworks_products_node1761037592653")

# Script generated for node Adventureworks_sales_2017
Adventureworks_sales_2017_node1761037765887 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_sales_2017_csv", transformation_ctx="Adventureworks_sales_2017_node1761037765887")

# Script generated for node Adventureworks_returns
Adventureworks_returns_node1761037632451 = glueContext.create_dynamic_frame.from_catalog(database="adventure_works_db", table_name="adventureworks_returns_csv", transformation_ctx="Adventureworks_returns_node1761037632451")

Customercalendar_node1761039907602 = MyTransform_Calendar(glueContext, DynamicFrameCollection({"Adventureworks_calendar_node1761037317577": Adventureworks_calendar_node1761037317577}, glueContext))

CustomCustomers_node1761041700240 = MyTransform_Customers(glueContext, DynamicFrameCollection({"Adventureworks_customers_node1761037448255": Adventureworks_customers_node1761037448255}, glueContext))

Adventureworks_sales_node1761096365326 = MyTransform_Sales(glueContext, DynamicFrameCollection({"Adventureworks_sales_2015_node1761037673976": Adventureworks_sales_2015_node1761037673976, "Adventureworks_sales_2016_node1761037725254": Adventureworks_sales_2016_node1761037725254, "Adventureworks_sales_2017_node1761037765887": Adventureworks_sales_2017_node1761037765887}, glueContext))

CustomReturns_node1761094054666 = MyTransform_Returns(glueContext, DynamicFrameCollection({"Adventureworks_returns_node1761037632451": Adventureworks_returns_node1761037632451}, glueContext))

# Script generated for node Select From Collection
SelectFromCollection_node1761136755725 = SelectFromCollection.apply(dfc=Customercalendar_node1761039907602, key=list(Customercalendar_node1761039907602.keys())[0], transformation_ctx="SelectFromCollection_node1761136755725")

# Script generated for node Select From Collection
SelectFromCollection_node1761137475293 = SelectFromCollection.apply(dfc=CustomCustomers_node1761041700240, key=list(CustomCustomers_node1761041700240.keys())[0], transformation_ctx="SelectFromCollection_node1761137475293")

# Script generated for node Select From Collection
SelectFromCollection_node1761138418623 = SelectFromCollection.apply(dfc=Adventureworks_sales_node1761096365326, key=list(Adventureworks_sales_node1761096365326.keys())[0], transformation_ctx="SelectFromCollection_node1761138418623")

# Script generated for node Select From Collection
SelectFromCollection_node1761137349552 = SelectFromCollection.apply(dfc=CustomReturns_node1761094054666, key=list(CustomReturns_node1761094054666.keys())[0], transformation_ctx="SelectFromCollection_node1761137349552")

# Script generated for node load_product_subcategories
EvaluateDataQuality().process_rows(frame=Adventureworks_product_subcategories_node1761037554551, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761136732579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
load_product_subcategories_node1761136894533 = glueContext.write_dynamic_frame.from_options(frame=Adventureworks_product_subcategories_node1761037554551, connection_type="s3", format="glueparquet", connection_options={"path": "s3://adventure-works-data-engineering-project/transformed/product_subcategories/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="load_product_subcategories_node1761136894533")

# Script generated for node load_territories
EvaluateDataQuality().process_rows(frame=Adventureworks_territories_node1761037805262, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761136732579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
load_territories_node1761136946770 = glueContext.write_dynamic_frame.from_options(frame=Adventureworks_territories_node1761037805262, connection_type="s3", format="glueparquet", connection_options={"path": "s3://adventure-works-data-engineering-project/transformed/territories/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="load_territories_node1761136946770")

# Script generated for node load_product_categories
EvaluateDataQuality().process_rows(frame=Adventureworks_product_categories_node1761037509945, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761136732579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
load_product_categories_node1761136987065 = glueContext.write_dynamic_frame.from_options(frame=Adventureworks_product_categories_node1761037509945, connection_type="s3", format="glueparquet", connection_options={"path": "s3://adventure-works-data-engineering-project/transformed/product_categories/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="load_product_categories_node1761136987065")

# Script generated for node load_products
EvaluateDataQuality().process_rows(frame=Adventureworks_products_node1761037592653, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761136732579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
load_products_node1761137041872 = glueContext.write_dynamic_frame.from_options(frame=Adventureworks_products_node1761037592653, connection_type="s3", format="glueparquet", connection_options={"path": "s3://adventure-works-data-engineering-project/transformed/products/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="load_products_node1761137041872")

# Script generated for node load_calendar
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1761136755725, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761136732579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
load_calendar_node1761136846311 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1761136755725, connection_type="s3", format="glueparquet", connection_options={"path": "s3://adventure-works-data-engineering-project/transformed/calendar/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="load_calendar_node1761136846311")

# Script generated for node load_customer
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1761137475293, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761136732579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
load_customer_node1761137513805 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1761137475293, connection_type="s3", format="glueparquet", connection_options={"path": "s3://adventure-works-data-engineering-project/transformed/customers/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="load_customer_node1761137513805")

# Script generated for node load_sales
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1761138418623, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761136732579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
load_sales_node1761138464347 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1761138418623, connection_type="s3", format="glueparquet", connection_options={"path": "s3://adventure-works-data-engineering-project/transformed/sales/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="load_sales_node1761138464347")

# Script generated for node load_returns
EvaluateDataQuality().process_rows(frame=SelectFromCollection_node1761137349552, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1761136732579", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
load_returns_node1761137373980 = glueContext.write_dynamic_frame.from_options(frame=SelectFromCollection_node1761137349552, connection_type="s3", format="glueparquet", connection_options={"path": "s3://adventure-works-data-engineering-project/transformed/returns/", "partitionKeys": []}, format_options={"compression": "snappy"}, transformation_ctx="load_returns_node1761137373980")

job.commit()
