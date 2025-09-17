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

catalog, database = db_path.split(".") # Pulled from core_config.ini
spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {database}")
spark.catalog.setCurrentCatalog(catalog) 
spark.catalog.setCurrentDatabase(database)