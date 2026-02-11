
# test_kpi_analysis.py

from pyspark.sql import SparkSession
from src.kpi_analysis import top_job_categories


def test_top_job_categories():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )

    # sample job category data
    data = [("IT",), ("IT",), ("HR",)]
    columns = ["Job Category"]

    df = spark.createDataFrame(data, columns)

    result_df = top_job_categories(df)
    top_category = result_df.collect()[0]["Job Category"]

    assert top_category == "IT"

    spark.stop()
