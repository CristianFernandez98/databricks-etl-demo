# Databricks notebook source
import configparser
from pathlib import Path
import os

#----------------------------------------------------------------------------------------------------
# Text/INI Based Configurations are cached here !
#----------------------------------------------------------------------------------------------------
config = configparser.ConfigParser()
config_path = Path("/Workspace/Users/runcoyote2017@gmail.com/databricks-etl-demo/conf/core_config.ini")

NULL = config.read(config_path)

db_path = config.get("databricks", "schema_name")
library = config.get("notebook","library_dir")
print(library)
catalog, database = db_path.split(".") # Pulled from core_config.ini
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
spark.catalog.setCurrentCatalog(catalog) 
spark.catalog.setCurrentDatabase(database)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS family_support;
# MAGIC CREATE SCHEMA IF NOT EXISTS family_support.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS family_support.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS family_support.gold;

# COMMAND ----------

# MAGIC %run ../lib/utils_notebook

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *
people_schema =StructType(
    [StructField("id",IntegerType(),False),
     StructField("firstName",StringType(),True),
     StructField("middleName",StringType(),True),
     StructField("last Name",StringType(),True),
     StructField("gender-",StringType(),True),
     StructField("birthDate",DateType(),True),
     StructField("ssn",StringType(),True),
     StructField("salary",DoubleType(),True)]
)
ruta_archivo = "dbfs:/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt"
df_spark = read_file_to_DF(ruta_archivo,'csv',":",people_schema)
df_spark = normalize_column_names(df_spark)
df_spark.printSchema()
#display(df_spark)
# dbfs:/databricks-datasets/learning-spark-v2/people/people-with-header-10m.txt
# dbfs:/databricks-datasets/learning-spark-v2/sf-fire/sf-fire-calls.csv

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM family_support.silver.cleaned_caregivers
