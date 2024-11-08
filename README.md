# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize SparkSession
spark = SparkSession.builder \
    .appName("AWS S3 Integration and Data Processing") \
    .getOrCreate()

# Set AWS S3 access keys securely (use environment variables or Databricks secrets in production)
spark._jsc.hadoopConfiguration().set("fs.s3a.access.key", "AKIAZG4APFAQRRDBUO65")
spark._jsc.hadoopConfiguration().set("fs.s3a.secret.key", "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")

# Define S3 bucket and file path
bucket_name = "iiht-ntt-24"
sale_order_file_path = f"s3a://{bucket_name}/data.csv/SaleOrder.csv"

# CSV options
csv_options = {
    "inferSchema": "true",
    "header": "true",
    "sep": ","
}

# Try block for processing Sales data
try:
    # Load the SaleOrder.csv file from S3 into a DataFrame
    dfSales = spark.read.format("csv").options(**csv_options).load(sale_order_file_path)

    # Display the DataFrame to verify contents
    dfSales.show()

    # Handle the SalesTable creation or replacement
    table_name = "SalesTable"
    warehouse_location = spark.conf.get("spark.sql.warehouse.dir", "dbfs:/FileStore/tables")
    table_path = f"{warehouse_location}/{table_name.lower()}"

    # Check if the directory exists and clear it
    if dbutils.fs.ls(table_path):
        dbutils.fs.rm(table_path, recurse=True)
        
    # Drop existing table if it exists
    if spark.catalog.tableExists(table_name):
        spark.sql(f"DROP TABLE IF EXISTS {table_name}")
        
    # Save DataFrame as a Parquet table
    dfSales.write.format("parquet").mode("overwrite").option("path", table_path).saveAsTable(table_name)
    print(f"Table {table_name} created successfully.")

except Exception as e:
    print(f"An error occurred while processing Sales data: {e}")

# Try block for handling Delta table operations
try:
    # Load the permanent table created previously
    permanent_table_df = spark.table("SilverCustomer")

    # Filter rows where CustomerKey is not null
    silver_customers_df = permanent_table_df.filter(col("CustomerKey").isNotNull())

    # Create a temporary view for further operations
    silver_customers_df.createOrReplaceTempView("temp_silver_customers_table")

    # Define the Delta table path
    delta_table_location = "/FileStore/tables/temp_silver_customers_table_delta"
    dbutils.fs.rm(delta_table_location, True)  # Ensure the directory is empty

    # Save the DataFrame as a Delta table
    silver_customers_df.write.format("delta").mode("overwrite").save(delta_table_location)
    print(f"Delta table saved at {delta_table_location}")

    # Access and display the saved Delta table
    delta_table_df = spark.read.format("delta").load(delta_table_location)
    delta_table_df.show()

except Exception as e:
    print(f"An error occurred during Delta table operations: {e}")

