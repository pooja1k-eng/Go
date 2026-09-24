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

# Removed SparkSession

def lowcase(df):
 # Convert all column names to lower case
    df = df.toDF(*[c.lower() for c in df.columns])
    return df

project_id = "poc-project-486808"  #@param {type:"string"}

gcs_base_path = "gs://airops_grid_datasets"  #@param {type:"string"}



ifadapt__adapt_ea_eo_fea = spark.read.parquet(f"{gcs_base_path}/uc/ifadapt/adapt_ea_eo_fea")

ifadapt__adapt_ea_eo_fea = lowcase(ifadapt__adapt_ea_eo_fea)

ifadapt__adapt_ea_eo_fea.createOrReplaceTempView('ifadapt__adapt_ea_eo_fea')

# ============================================================
# STEP 1: Query-QTA3QOlfKax27V1d_result_2.sas
# ============================================================
# Summary of auto-fixes:
# - Mistake 1: Incorrect use of 'as' for table aliasing in PySpark SQL.
#   Technical Reason: PySpark SQL requires 'AS' (uppercase) for table aliasing, or omitting 'AS' entirely. Using 'as' (lowercase) may cause parsing errors in some Spark versions.
#   Fix Applied: Changed 'as' to 'AS' in the FROM clause for table aliasing.

query_for_adapt_ea_eo_fea = spark.sql("""select distinct
    t1.doctype,
    t1.docid,
    t1.docrev,
    t1.title,
    t1.wri_nbr
from ifadapt__adapt_ea_eo_fea AS t1
where t1.title like '%79-0097%' and t1.dw_is_latest_flg = 'Y'
order by t1.docid""")
query_for_adapt_ea_eo_fea.createOrReplaceTempView("query_for_adapt_ea_eo_fea")