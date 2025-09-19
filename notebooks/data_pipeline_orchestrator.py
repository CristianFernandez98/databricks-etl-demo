# Databricks notebook source
# DBTITLE 1,Import core configurations
import configparser
from pathlib import Path
config = configparser.ConfigParser()
config_path = Path("/Workspace/Users/runcoyote2017@gmail.com/databricks-etl-demo/conf/core_config.ini")
# Get the catalog name configuration and set the current Catalog and database 
NULL = config.read(config_path)
catalog = config.get("databricks", "catalog_name")
spark.catalog.setCurrentCatalog(catalog)

# COMMAND ----------

# DBTITLE 1,Create catalog and schemas
# Check if the catalog and schemas exist
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.bronze")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.silver")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.gold")

# COMMAND ----------

# DBTITLE 1,Run the Bronze Notebooks
dbutils.notebook.run("./bronze/caregivers_notebook", 60)
dbutils.notebook.run("./bronze/children_notebook", 60)

# COMMAND ----------

# DBTITLE 1,Run the Silver Notebooks
dbutils.notebook.run("./silver/caregivers_notebook", 60)
dbutils.notebook.run("./silver/children_notebook", 60)

# COMMAND ----------

# DBTITLE 1,Run the Gold Notebook
dbutils.notebook.run("./gold/gold_tables_definition", 60)
