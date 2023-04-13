#Data Quality Checker
This set of Python code files is designed to extract data from different sources, calculate data quality for each column, and export quality fails reports.

Files
extract_bigquery.py: This Python code file contains a function to extract a table from BigQuery and convert it to a Pandas DataFrame.
extract_postgresql.py: This Python code file contains a function to extract a table from a PostgreSQL database and convert it to a Pandas DataFrame.
extract_csv.py: This Python code file contains a function to read a CSV file into a Pandas DataFrame.
main.py: This Python code file contains the main script that uses the above functions to extract data from different sources, calculate data quality for each column, and export quality fails reports.
Usage
Replace the placeholders in the Python code files with your own parameters for each data source, such as project id, dataset id, table id, host, port, database, username, password, table name, and CSV file path.
Run the main.py Python code file to extract data from different sources, calculate data quality for each column, and export quality fails reports.
The quality fails reports will be exported as text files named bigquery_quality_fails_report.txt, postgresql_quality_fails_report.txt, and csv_quality_fails_report.txt in the same directory as the Python code files.

Dependencies
pandas
psycopg2
google-cloud-bigquery
You can install these dependencies using pip:

Copy code
pip install pandas psycopg2 google-cloud-bigquery
License
This code is released under the MIT License.
