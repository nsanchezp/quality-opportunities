import pandas as pd
import psycopg2
from google.cloud import bigquery
from pandas.api.types import is_numeric_dtype, is_datetime64_dtype
from extract_bigquery import extract_bigquery_table
from extract_postgresql import extract_postgresql_table
from extract_csv import extract_csv_file

# Define function to calculate data quality for each column
def check_data_quality(df):
    quality_report = {}
    for column in df.columns:
        quality_checks = []
        
        # Check 1: Check if column has null values
        if df[column].isnull().sum() > 0:
            quality_checks.append("Null values")
        
        # Check 2: Check if column has unique values
        if len(df[column].unique()) == len(df):
            quality_checks.append("Unique values")
        
        # Check 3: Check if column has any leading/trailing whitespaces
        if any(df[column].astype(str).str.startswith(" ")) or any(df[column].astype(str).str.endswith(" ")):
            quality_checks.append("Leading/trailing whitespaces")
        
        # Check 4: Check if column has any special characters
        if any(df[column].astype(str).str.contains('[^a-zA-Z0-9_ ]', regex=True)):
            quality_checks.append("Special characters")
        
        # Check 5: Check if column has numeric data type
        if is_numeric_dtype(df[column]):
            quality_checks.append("Numeric data type")
        
        # Check 6: Check if column has datetime data type
        if is_datetime64_dtype(df[column]):
            quality_checks.append("Datetime data type")
        
        # Add quality checks to quality report
        quality_report[column] = quality_checks
    
    return quality_report

# Define function to export quality fails report
def export_quality_fails_report(quality_report, file_name):
    with open(file_name, "w") as file:
        for column, quality_fails in quality_report.items():
            if len(quality_fails) > 0:
                file.write(f"Column '{column}' failed the following quality checks: {', '.join(quality_fails)}.\n")

# Extract data from BigQuery
df = extract_bigquery_table("your-project-id", "your-dataset-id", "your-table-id")

# Calculate data quality for each column
quality_report = check_data_quality(df)

# Export quality fails report
export_quality_fails_report(quality_report, "bigquery_quality_fails_report.txt")

# Extract data from PostgreSQL
conn = psycopg2.connect(
    host="your-host",
    port="your-port",
    database="your-database",
    user="your-username",
    password="your-password"
)
df = extract_postgresql_table(conn, "your-table-name")

# Calculate data quality for each column
quality_report = check_data_quality(df)

# Export quality fails report
export_quality_fails_report(quality_report, "postgresql_quality_fails_report.txt")

# Extract data from CSV
df = extract_csv_file("your-csv-file-path")

# Calculate data quality for each column
quality_report = check_data_quality(df)

# Export quality fails report
export_quality_fails_report(quality_report, "csv_quality_fails_report.txt")
