# test_data_cleaning.py

from pyspark.sql import SparkSession
from src.data_cleaning import create_avg_salary


def test_create_avg_salary():

    spark = (
        SparkSession.builder
        .master("local[1]")
        .appName("test")
        .getOrCreate()
    )

    data = [(10000.0, 20000.0)]
    columns = ["Salary Range From", "Salary Range To"]

    df = spark.createDataFrame(data, columns)

    result_df = create_avg_salary(df)
    result = result_df.collect()[0]["avg_salary"]

    assert result == 15000.0

    spark.stop()
