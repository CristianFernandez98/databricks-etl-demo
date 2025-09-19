# Databricks notebook source
from pyspark.sql import SparkSession
import re
from pyspark.sql.functions import *
from pyspark.sql.types import *

def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config('spark.driver.extraJavaOptions') \
            .master("local[2]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()

#Function to read files and create a DataFrame
def read_file_to_DF(file_path, format="csv", delimiter=",", schema=None, header="true"):
    try:
        reader = spark.read.format(format).option("header", header).option("delimiter", delimiter)
        if schema is None:
            reader = reader.option("inferSchema", "true")
        else:
            reader = reader.schema(schema)
        return reader.load(file_path)
    except Exception as e:
        print(f"Error reading file: {e}")
        return None
    
#Function to create a table from DataFrame
def create_table(df, table_name, mode="overwrite"):
    df.write.mode(mode).saveAsTable(table_name)


## Data Cleaning

def delete_columns(df,columns):
    return df.drop(*columns)

def replace_nulls(df,replace_with):
    return df.na.fill(replace_with)

def remove_duplicates(df, columns=None):
    return df.dropDuplicates(columns)

## Transformations

def convert_data_types(df, schema: StructType):
    for field in schema.fields:
        df = df.withColumn(field.name, col(field.name).cast(field.dataType))
    return df

def normalize_column_names(df):
    for column in df.columns:
        df = df.withColumnRenamed(column, column.replace(" ", "_").replace("-", "_").lower())
    return df

def rename_columns(df,old_value, new_value):
    df = df.withColumnRenamed(old_value,new_value)
    return df

# Function to standardize gender
def standardize_gender(gender):
    if gender is None:
        return "Unknown"
    gender = gender.strip().lower()
    if re.match(r'^(m|ma|male)$', gender):
        return "Male"
    elif re.match(r'^(f|fe|female)$', gender):
        return "Female"
    else:
        return "Unknown"
udf_standardize_gender = udf(standardize_gender, StringType())

## Analysis
def count_unique_values(df, column):
    return df.groupBy(column).count()

def sort_by_column(df,column,ascending=True):
    return df.sort(column,ascending=ascending)

def inserted_rows(table_name):
    return spark.table(table_name).count()

