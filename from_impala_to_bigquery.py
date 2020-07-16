#!/usr/bin/python
# -*- coding: utf-8 -*-

"""
  From Impala to BigQuery

"""
import argparse
import os
import sys
import pandas as pd
from impala.util import as_pandas
from impala.dbapi import connect
import google.cloud.bigquery as bigquery

# Environment VARs
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "" # must have the proper credential

"""
   See Argparse Tutorial:  https://docs.python.org/2/howto/argparse.html#
"""
def parseArguments():
    parser = argparse.ArgumentParser(description="From Impala to Google BigQuery")

    # Mandatory arguments
    required_named = parser.add_argument_group("Required arguments")
    required_named.add_argument("--input_year", help="Login input year YYYY to be processed", required=True)
    required_named.add_argument("--input_month", help="Login input month MM to be processed", required=True)
    required_named.add_argument("--input_day", help="Login input day DD to be processed", required=True)

    # Optional arguments
    parser.add_argument("--dry_run", help="execution in dry run: show commands but dont launch process", action="store_true")
    return parser.parse_args()

def execImpalaQuery(query):
    conn = connect(host='<HOST>', port=21050, database='default')
    cur = conn.cursor()
    cur.execute(query)
    df = as_pandas(cur)
    return df

def loadTableFile(file_path, table_id):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # First: delete table
    client.delete_table(table_id, not_found_ok=True)
    print("Deleted table '{}'.".format(table_id))

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
    )

    with open(file_path, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)

    job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )

    return table

def getTableId(date):

    # Construct a BigQuery client object.
    client = bigquery.Client()

    # Get rows
    query="""
        PUT YOUR QUERY HERE
    """
    df = (
        client.query(query, project='<PROJECT_ID>')
        .result()
        .to_dataframe()
    )

    print(df.head())

    return df

print("------------------- START -------------------")
# Get Arguments
args = parseArguments()
input_year = args.input_year
input_month = args.input_month
input_day = args.input_day
print("# Arguments #")
print("input_year: " + input_year)
print("input_month: " + input_month)
print("input_day: " + input_day)

# Common constants
table_id = "project.dataset.table"
temp_file = "/tmp/from_impala_to_bigquery.csv"
print("temp_file: " + temp_file)
print("table_id: " + table_id)

# If all args are properly filled
if sys.argv[1] is not None and sys.argv[2] is not None and sys.argv[3] is not None:

    print("1. Exec query against Impala")
    query = """
        PUT IMPALA QUERY HERE
    """
    df_1 = execImpalaQuery(query)
    df_1.drop_duplicates().to_csv(temp_file, index=False)

    #OPTIONAL: STEPS TO MERGE TABLES
    #query = """
    #    ALSO, YOU CAN ADD MORE IMPALA QUERIES TO MERGE THEM
    #"""
    #df_2 = execImpalaQuery(query)

    #print("2. Merge both dataframes")
    #df_3 = pd.merge(df_2.drop_duplicates(), df_1.drop_duplicates(), on=['<FIELD_1>', '<FIELD_2>'])

    #print("3. Save output into temp file, removing duplex")
    #df_3.drop_duplicates().to_csv(temp_file, index=False)

    print("4. Upload info to Google BigQuery table")
    loadTableFile(temp_file, table_id)

    #OPTIONAL PRINT YOUR TABLES
    #print("5. BigQuery: Results")
    #df_4 = getTableId(input_year+input_month+input_day)

print("-------------------- END --------------------")
