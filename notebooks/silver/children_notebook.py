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
raw_children_table = f"{catalog}.bronze.raw_children"
raw_children_df = spark.table(raw_children_table)
# Replace null values with N/A and remove duplicates based on fisrt and last name
children_df = replace_nulls(raw_children_df,"N/A")
children_df = remove_duplicates(children_df,["first_name","last_name"])

# COMMAND ----------

# DBTITLE 1,Transformations
#Define the most appropiate schema for the data and convert the data types
children_schema =StructType(
    [StructField("child_id",LongType(),False),
     StructField("first_name",StringType(),True),
     StructField("middle_name",StringType(),True),
     StructField("last_name",StringType(),True),
     StructField("birthdate",DateType(),True),
     StructField("gender",StringType(),True),
     StructField("caregiver_id",LongType(),False)]
)
children_df = convert_data_types(children_df,children_schema)
# Change the data structure to use as unique ID
children_df_with_id = children_df.withColumn("child_id", monotonically_increasing_id() + expr("10000000"))
# Standardize gender data and create a column for age
children_df_standarize_gender = children_df_with_id.withColumn("gender", udf_standardize_gender(col("gender")))
final_children_df = children_df_standarize_gender.withColumn("age", floor(datediff(current_date(), col("birthdate")) / 365.25))


# COMMAND ----------

# DBTITLE 1,Create table with cleaned data
# Create a table with cleaned and transform data
create_table(final_children_df,"cleaned_children")
#Count and print inserted rows
rows_inserted = inserted_rows('cleaned_children')
print(f"rows inserted:{rows_inserted}")
