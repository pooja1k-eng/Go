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



fmacrs__acars_delays_new = spark.read.parquet(f"{gcs_base_path}/uc/fmacrs/acars_delays_new")
fmflt__ac_fleet_desc = spark.read.parquet(f"{gcs_base_path}/uc/fmflt/ac_fleet_desc")

fmacrs__acars_delays_new = lowcase(fmacrs__acars_delays_new)
fmflt__ac_fleet_desc = lowcase(fmflt__ac_fleet_desc)

fmacrs__acars_delays_new.createOrReplaceTempView('fmacrs__acars_delays_new')
fmflt__ac_fleet_desc.createOrReplaceTempView('fmflt__ac_fleet_desc')
#TODO:In sas file, line no.7 we commented the filename. please review  if it is required.

# /* ============================================================ */
# /* STEP 1: qubrrDzG8CuIJI2c_result_2.sas */
# /* ============================================================ */

# Summary of auto-fixes:
# - Mistake 1: Incorrect date literal syntax in WHERE clause (`t1.month = date '2012-10-01'`)
# - Technical Reason 1: PySpark SQL does not support the `date 'YYYY-MM-DD'` literal syntax; instead, string comparison or `to_date` should be used.
# - Fix Applied 1: Replaced `t1.month = date '2012-10-01'` with `t1.month = '2012-10-01'`

query_for_acars_delays_new = spark.sql(
    """select distinct
    t1.month,
    t1.day,
    t1.zuludate,
    t1.flight,
    t1.origin,
    t1.destination,
    t1.tailnum,
    t2.fleet_desc,
    t1.schdepgt,
    t1.scharrgt,
    t1.act_departure_datetime,
    t1.act_arrival_datetime,
    t1.aircraft,
    t1.acars_code,
    t1.delay_type,
    t1.code_description
from fmacrs__acars_delays_new as t1
left join fmflt__ac_fleet_desc as t2 on t1.equip = t2.equip_type
where t1.code_type = 'EFFICIENCY' and t1.month = '2012-10-01'"""
)
query_for_acars_delays_new.createOrReplaceTempView("query_for_acars_delays_new")