### GCP COMPLIANCE BLOCK ####

import sys
import os
from google.cloud import bigquery
client = bigquery.Client()

########################### install wheel library manually ###########################################
!gsutil cp gs://fedex_sas_to_pyspark/library//airops_grid_lib-0.1.12-py3-none-any.whl
# Step 2: Install the wheel file
!pip install airops_grid_lib-0.1.12-py3-none-any.whl

!pip install paramiko
#########################################################################################################

# Generated with sas2pyspark version 2.8.4
# Removed SparkSession import
import pyspark.sql.functions as F
from pyspark.sql.functions import col
from pyspark.sql.types import StructType

# Removed SparkSession

def lowcase(df):
 # Convert all column names to lower case
    df = df.toDF(*[c.lower() for c in df.columns])
    return df

project_id = "poc-project-486808"  #@param {type:"string"}

gcs_base_path = "gs://airops_grid_datasets"  #@param {type:"string"}



#TODO:In SAS file line no.3-8,please review for email.

# This DATA step doesn't include a set statement, and no last dataset is known,
# so we initialize the dataframe to an empty one.

emailserver = spark.createDataFrame([[]], StructType([]))
# LENGTH statement not yet implemented