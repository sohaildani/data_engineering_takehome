# data_loader.py
# Handles spark session creation and loading NYC jobs dataset

import logging
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType
)
from pyspark.sql.functions import col, to_date
from src.utils import create_spark_session


logging.basicConfig(level=logging.INFO)
log = logging.getLogger("data_loader")

# Schema inference

def get_schema() -> StructType:

    return StructType([
        StructField("Job ID", StringType(), True),
        StructField("Agency", StringType(), True),
        StructField("Posting Type", StringType(), True),
        StructField("# Of Positions", IntegerType(), True),
        StructField("Business Title", StringType(), True),
        StructField("Civil Service Title", StringType(), True),
        StructField("Title Code No", StringType(), True),
        StructField("Level", StringType(), True),
        StructField("Job Category", StringType(), True),
        StructField("Full-Time/Part-Time indicator", StringType(), True),
        StructField("Salary Range From", DoubleType(), True),
        StructField("Salary Range To", DoubleType(), True),
        StructField("Salary Frequency", StringType(), True),
        StructField("Work Location", StringType(), True),
        StructField("Division/Work Unit", StringType(), True),
        StructField("Job Description", StringType(), True),
        StructField("Minimum Qual Requirements", StringType(), True),
        StructField("Preferred Skills", StringType(), True),
        StructField("Additional Information", StringType(), True),
        StructField("To Apply", StringType(), True),
        StructField("Hours/Shift", StringType(), True),
        StructField("Work Location 1", StringType(), True),
        StructField("Recruitment Contact", StringType(), True),
        StructField("Residency Requirement", StringType(), True),
        StructField("Posting Date", StringType(), True),
        StructField("Post Until", StringType(), True),
        StructField("Posting Updated", StringType(), True),
        StructField("Process Date", StringType(), True),
    ])


# Load Data

def load_nyc_jobs_data(spark: SparkSession, path: str) -> DataFrame:
    log.info(f"Reading file: {path}")

    df = spark.read.option("header", True).schema(get_schema()).csv(path)

    log.info(f"Rows loaded: {df.count()}")

    return df


# Date Parsing

def parse_date_columns(df: DataFrame) -> DataFrame:
    # convert string dates to proper DateType
    
    df = df.withColumn("Posting Date", to_date(col("Posting Date"), "MM/dd/yyyy"))\
    .withColumn("Post Until", to_date(col("Post Until"), "MM/dd/yyyy"))\
    .withColumn("Posting Updated", to_date(col("Posting Updated"), "MM/dd/yyyy"))\
    .withColumn("Process Date", to_date(col("Process Date"), "MM/dd/yyyy"))

    return df


# Basic Validation

def validate_dataframe(df: DataFrame) -> None:
    if df.rdd.isEmpty():
        raise ValueError("DataFrame is empty. Check input file.")

    log.info("DataFrame validation passed")
