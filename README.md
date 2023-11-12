# Data Sink

This script reads a CSV file from a specified DBFS location, performs some data transformations, and writes the processed data to DBFS. The transformations include converting a date column to dates, window transformations, and casting columns to appropriate types. The processed data is written to a CSV file.

## Usage

To use this script, you need to have the following libraries installed:

- os
- pyspark.sql.SparkSession
- pyspark.sql.functions
- pyspark.sql.window

You can visualise the script the notebook `notebooks/datasink.py` file.

## Configuration

The following configurations can be modified in the script:

- `INPUT_FILE_LOCATION`: The location of the input CSV file.
- `OUTPUT_FILE_LOCATION`: The location where the processed data will be written.
- `FILE_TYPE`: The type of the input and output files.
- `INFER_SCHEMA`: Whether to infer the schema of the input file.
- `FIRST_ROW_IS_HEADER`: Whether the first row of the input file is a header.
- `DELIMITER`: The delimiter used in the input file.

## Data Transformation

The script performs the following data transformations:

- Converts the `Date` column to dates.
- Performs window transformations.

### Video Demo
You can watch the demo video [here](https://www.youtube.com/watch?v=7W6g75UgQ6w).


