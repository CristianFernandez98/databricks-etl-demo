# Databricks notebook source
# DBTITLE 1,Import core configurations
import configparser
from pathlib import Path
config = configparser.ConfigParser()
config_path = Path("/Workspace/Users/runcoyote2017@gmail.com/databricks-etl-demo/conf/core_config.ini")
# Get the catalog name configuration and set the current Catalog and database 
NULL = config.read(config_path)
catalog = config.get("databricks", "catalog_name")
database = 'silver'
spark.catalog.setCurrentCatalog(catalog) 
spark.catalog.setCurrentDatabase(database)

# COMMAND ----------

# DBTITLE 1,Run utils notebook
# MAGIC %run ../../lib/utils_notebook

# COMMAND ----------

# DBTITLE 1,Read and clean raw data
#Read raw data from bronze
raw_caregiver_table = f"{catalog}.bronze.raw_caregivers"
raw_caregiver_df = spark.table(raw_caregiver_table)
# Replace null values with N/A and remove duplicates based on fisrt and last name
caregiver_df = replace_nulls(raw_caregiver_df,"N/A")
caregiver_df = remove_duplicates(caregiver_df,["first_name","last_name"])

# COMMAND ----------

# DBTITLE 1,Transformations
#Define the most appropiate schema for the data and convert the data types
caregiver_schema =StructType(
    [StructField("id",LongType(),False),
     StructField("first_name",StringType(),True),
     StructField("middle_name",StringType(),True),
     StructField("last_name",StringType(),True),
     StructField("gender",StringType(),True),
     StructField("birthdate",DateType(),True),
     StructField("ssn",StringType(),True),
     StructField("salary",DecimalType(10,2),True)]
)
caregiver_df = convert_data_types(raw_caregiver_df,caregiver_schema)
# Rename and change the data structure to use as unique ID
caregiver_df = rename_columns(caregiver_df,'id','caregiver_id')
caregiver_df_with_id = caregiver_df.withColumn("caregiver_id", monotonically_increasing_id() + expr("10000000"))
# Standardize gender data and create a column for age
caregiver_df_standarize_gender = caregiver_df_with_id.withColumn("gender", udf_standardize_gender(col("gender")))
final_caregiver_df = caregiver_df_standarize_gender.withColumn("age", floor(datediff(current_date(), col("birthdate")) / 365.25))


# COMMAND ----------

# DBTITLE 1,Create table with cleaned data
# Create a table with cleaned and transform data
create_table(final_caregiver_df,"cleaned_caregivers")
#Count and print inserted rows
rows_inserted = inserted_rows('cleaned_caregivers')
print(f"rows inserted:{rows_inserted}")
