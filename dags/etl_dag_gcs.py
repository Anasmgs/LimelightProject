"""
ETL DAG using GCS & BigQuery.

To use this DAG, you need to set some ENV variables within the Airflow UI:

- `GCSBucketIn`: the name of the GCS bucket where the raw data streamed will be stored.
- `GCSBucketOut`: the name of the GCS bucket where the cleaned data will be stored.
- `BQDataset`: the name of the BigQuery dataset where the cleaned data will be loaded.

Also set the connection for the GCloud account.
"""
import json
import logging
from datetime import datetime
from datetime import timedelta
import pytz
import pandas as pd
import requests
import os
from pathlib import Path
from google.cloud import storage, bigquery
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

import warnings
warnings.filterwarnings("ignore", category=FutureWarning) 

#Get ENV vars (defined in Airflow UI in our case)

GCS_bucket_in = Variable.get("GCSBucketIn")
GCS_bucket_out = Variable.get("GCSBucketOut")
BQ_Dataset = Variable.get("BQDataset")
BQ_Table = Variable.get("BQTable")



default_args = {
    "owner": "airflow",
    "start_date": datetime(2023, 5, 15),
}


def extract_velib_jsons(**context):
    """Transforms raw data from JSON file to ingestable data for .
    """

    # Connect to our GCS bucket and get list of newly updated JSON files
    GCS_hook = GCSHook(gcp_conn_id="google_cloud_default")
    returned_listfilenames = GCS_hook.list_by_timespan(bucket_name=GCS_bucket_in,  
                                                       timespan_start= (datetime.now() - timedelta(hours=1)).replace(tzinfo=pytz.utc), 
                                                       timespan_end=datetime.now().replace(tzinfo=pytz.utc))
    

    # Use list of newly modified jsons in bucket to download them in temporary ./data local folder
    for file_name in returned_listfilenames:
        dim_file_name = file_name.split("/")[6]
        GCS_hook.download(bucket_name=GCS_bucket_in,
                          object_name=f'{file_name}',  
                          filename=f"./data/{dim_file_name}" ) 
        # Push the saved filenames to the Airflow context so that we can use them later
        context["task_instance"].xcom_push(key="velib_jsons_filenames", value=returned_listfilenames)

    logging.info("list of new JSON files to transform saved")



def transform_velib_data(**context):
    """Transform the JSON data to a CSV ingestible by Big Query and Looker Studio
    """

    GCS_hook = GCSHook(gcp_conn_id="google_cloud_default")
    # We get the filenames from the context
    returned_listfilenames = context["task_instance"].xcom_pull(key="velib_jsons_filenames")

    for file_name in returned_listfilenames:
        dim_file_name = file_name.split("/")[6]
        temp = []
        with open(f"./data/{dim_file_name}") as f: # we unpack JSON
            for line in f:
                temp.append(json.loads(line))
        df = pd.DataFrame()
        for i in range(len(temp)): # i is the index for the 20 JSON lines unpacked (1 line every 3 minutes)
            for j in range(len(temp[0]["records"])): # j is the index for the 1462 velib stations for each i (1462 stations data for every 3 minutes of the hour)
                df_temp = pd.json_normalize(temp[i]["records"][j]["fields"]) # temporary dataframe with all wanted infos for every station xminute (except timestamp)
                df_temp["timestamp"]=temp[i]["records"][j]["record_timestamp"] # add timestamp for every station xminute to the temporary dataframe
                df = df.append(df_temp) # gradually concatenate temporary dataframes to "master" dataframe
    
    # Data cleaning on dataframe for easy analysis and vizualisation
        df = df.reset_index(drop=True) # set-up index 
        df.index.name='index' # index naming for integration in Looker Studio for viz
        df["duedate"] = df["duedate"].apply(pd.to_datetime) 
        df["timestamp"]=df["timestamp"].apply(lambda x: datetime.utcfromtimestamp(x/1000)) #convert timestamp from UNIX to UTC
        df["timestamp_year"]=df["timestamp"].apply(lambda x: x.year)
        df["timestamp_month"]=df["timestamp"].apply(lambda x: x.month)
        df["timestamp_day"]=df["timestamp"].apply(lambda x: x.day)
        df["timestamp_hour"]=df["timestamp"].apply(lambda x: x.hour)
        df["timestamp_minute"]=df["timestamp"].apply(lambda x: x.minute)
        df["timestamp_second"]=df["timestamp"].apply(lambda x: x.second)
        df["coordonnees_geo"]=df["coordonnees_geo"].apply(lambda x: f"{x[0]},{x[1]}") #convert coordinates to right format for integration in Looker Studio for viz
    
    # Keep the same filename between the JSON file and the CSV
        csv_filename = file_name.split(".")[0] + ".csv"
        csv_filename_full_path = f"./data/{csv_filename}"

    # Save it temporarily in ./data folder
        filepath = Path(csv_filename_full_path)  
        filepath.parent.mkdir(parents=True, exist_ok=True)  
        df.to_csv(filepath)  

    # Load it to GCS
        GCS_hook.upload(filename=csv_filename_full_path, object_name=csv_filename_full_path[2:], bucket_name=GCS_bucket_out)
    
    # Push the filename to the context so that we can use it later
    
    context["task_instance"].xcom_push(key="velib_csv_filename", value=csv_filename_full_path[2:])

    logging.info("CSVs saved")

def load_gcs_to_bq(**context):
    """Loads the clean CSV data into BigQuery to enable analysis using SQL Queries
    """

    csv_to_load = context["task_instance"].xcom_pull(key="velib_csv_filename")

    op = GCSToBigQueryOperator(task_id="load_gcs_to_bq",
                                           bucket=GCS_bucket_out,
                                            source_objects= [f"{csv_to_load}"],
                                            source_format="csv",
                                            destination_project_dataset_table=f"{BQ_Dataset}.{BQ_Table}",
                                            write_disposition="WRITE_APPEND",
                                            create_disposition="CREATE_IF_NEEDED",
                                            autodetect=True,
                                            location="US")

    op.execute(context)

    logging.info("CSVs saved in BQ table")




with DAG(dag_id="etl_dag_gcs", default_args=default_args, schedule_interval= timedelta(minutes=30), catchup=False) as dag:
    start = DummyOperator(task_id="start")

    extract_velib_jsons = PythonOperator(task_id="extract_velib_jsons", python_callable=extract_velib_jsons)

    transform_velib_data = PythonOperator(task_id="transform_velib_data", python_callable=transform_velib_data)

    load_gcs_to_bq = PythonOperator(task_id="load_gcs_to_bq", python_callable=load_gcs_to_bq)   # we 'transformed' the GCStoBQoperator to a Python operator in order to be able to use the XCom from previous task



    extract_velib_jsons >> transform_velib_data >> load_gcs_to_bq

        
