
# processed_nyc_jobs.py
#
# End-to-end pipeline for processing NYC Jobs dataset.
# Steps:
#   - Read raw CSV
#   - Clean salary columns
#   - Add derived features
#   - Save final dataset as parquet
#

import logging
from pyspark.sql import SparkSession

from src.data_loader import (
    create_spark_session,
    load_nyc_jobs_data,
    parse_date_columns,
    validate_dataframe
)

from src.data_cleaning import (
    remove_invalid_salary,
    create_avg_salary,
    normalize_salary
)

from src.feature_engineering import (
    extract_degree,
    extract_skills,
    create_salary_band
)

from src.utils import create_spark_session, null_check

#logging

logging.basicConfig(level=logging.INFO)

log = logging.getLogger("nyc_jobs_pipeline")


def main(input_path: str, output_path: str):

    log.info("NYC Jobs pipeline started")

    # create spark session
    spark = create_spark_session("nyc_jobs_processing")

    # load raw dataset
    df = load_nyc_jobs_data(spark, input_path)

    # parse posting / closing dates
    df = parse_date_columns(df)

    # basic validation (schema / row checks etc.)
    validate_dataframe(df)

    #cleaning data

    # remove rows where salary range is invalid
    df = remove_invalid_salary(df)

    # create average salary column
    df = create_avg_salary(df)

    # normalize salary (annualized or standardized)
    df = normalize_salary(df)

    #feature engineering

    # extract degree requirement from job description
    df = extract_degree(df)

    # skill flags
    tech_stack = ["Python", "SQL", "Spark", "AWS", "Java"]
    df = extract_skills(df, tech_stack)

    # create salary band (low / mid / high)
    df = create_salary_band(df)

    # quick null check (useful during development)
    log.info("Null summary (sanity check)")
    null_check(df).show(truncate=False)

    # write output

    log.info(f"Writing parquet output -> {output_path}")


    df.write.mode("overwrite").parquet(output_path)

    log.info("NYC Jobs pipeline finished successfully")

    spark.stop()


if __name__ == "__main__":

    # paths can later be replaced with argparse
    input_path = "/dataset/nyc-jobs.csv"
    output_path = "output/processed_nyc_jobs.parquet"

    main(input_path, output_path)
