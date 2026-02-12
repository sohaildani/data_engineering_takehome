# MyDocument.md

## Project: NYC Jobs Pipeline

------------------------------------------------------------------------

# 1. Learnings

### 1. Spark Project Structuring

-   Structured PySpark project using modular architecture (`src/`,
    `tests/`, `output/`).
-   Separated data loading, cleaning, transformation, and KPI logic.
-   Created reusable functions like `create_spark_session` and
    `load_data`.

### 2. Docker + Jupyter Integration

-   Ran Spark inside Docker using `jupyter/pyspark-notebook`.
-   Implemented proper volume mounting to sync local files with
    container.
-   Diagnosed and fixed 0 KB file issue caused by incorrect mounting.

### 3. Import & Path Management

-   Resolved `ModuleNotFoundError` using sys.path adjustments.
-   Understood importance of project root structure and module
    organization.

### 4. Data Cleaning in Spark

-   Removed invalid salary records.
-   Performed proper data type conversions.
-   Used Spark transformations efficiently without collecting to driver.

### 5. Logging & Debugging

-   Added structured logging for better traceability.
-   Improved debugging skills by analyzing stack traces carefully.

------------------------------------------------------------------------

# 2. Challenges Faced

### 1. Docker File Sync Issue

-   VSCode showed content but container file was 0 KB.
-   Root cause: incorrect volume mounting.
-   Resolved by properly mapping local directory to container.

### 2. Module Import Errors

-   Faced `ModuleNotFoundError` and `ImportError`.
-   Causes included incorrect structure, nested functions, and kernel
    caching.
-   Fixed by restructuring project and restarting kernel.

### 3. Git Commit Issue

-   GitHub showed empty files despite local edits.
-   Root cause: file not saved or not committed.
-   Fixed using proper git add, commit, and push workflow.

### 4. Spark Configuration in Notebook

-   Ensured proper Spark session initialization.
-   Managed local vs container execution differences.

------------------------------------------------------------------------

# 3. Key Considerations

-   Validate schema before transformations.
-   Remove invalid salary records before aggregation.
-   Use Spark transformations for scalability.
-   Maintain modular and reusable code structure.
-   Use Docker for environment consistency.

------------------------------------------------------------------------

# 4. Assumptions

1.  Dataset schema remains consistent.
2.  Salary fields are numeric or convertible to numeric.
3.  Missing salary records are removed during cleaning.
4.  Local Spark execution (`local[*]`) is sufficient.
5.  Data volume fits within local execution environment.

------------------------------------------------------------------------

# 5. Future Improvements

-   Add comprehensive unit tests.
-   Implement schema validation layer.
-   Parameterize input and output paths.
-   Integrate CI/CD pipeline (e.g., GitHub Actions).
-   Add advanced data quality checks.
-   Persist output in parquet format for better performance.

------------------------------------------------------------------------

# 6. Conclusion

This project strengthened understanding of:

-   PySpark pipeline development
-   Docker-based execution
-   Project structuring
-   Debugging environment and import issues
-   Git version control best practices

The solution is modular, scalable, and ready for extension into a
production-grade pipeline.
