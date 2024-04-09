'''
This ETL job is fetches data from Service Now's CMDB Servers table and aggregates to an output file

'''

import sys
sys.path.append('../../utils')
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from utils.ETL import create_frame_from_catalog, repartition_frames, write_dynamic_frame, deduplicate_rows

# Initializing CMDBServers the job with default parameters.
args = getResolvedOptions(sys.argv, ["JOB_NAME", "S3_OUTPUT_PATH"]) 
sc = SparkContext()
glueContext = GlueContext(sc)
sc.setLogLevel("ERROR")
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Job Vars
fields_to_keep = []
dynamic_frame_list = []
partitions = ["year", "month", "day"]


'''
Creates a list of dictionaries with the parameters

Dict keys: 
----------
prefix : string - the prefix that will be applied into the CMDB Servers DynamicFrame fields.
database : string - database imported from datagov name to create CMDB Servers DynamicFrame.
table_name : string - table imported from datagov name to create create CMDB DynamicFrame.
'''
datasets = [
    {
        "prefix":"cmdb_ci_aws_accounts",
        "database": "db_sor_01",
        "table_name": "awc_sor",
        "mappings": [
            ("year", "year"),
            ("month", "month"),
            ("day", "day")
        ]
    }
]


for frame in datasets: 
    dyf = create_frame_from_catalog(frame, fields_to_keep, glueContext)
    dedup_dyf = deduplicate_rows(dyf, glueContext)
    dynamic_frame_list.append({
        'dyf_name': f"{frame['prefix']}",
        'dyf_object': dedup_dyf
    })


# Repart tables to solve small files and write to s3 CMDBServers path as a gziped json
write_dynamic_frame(
    repartition_frames(dynamic_frame_list[0]['dyf_object'], 64), 
    partitions, 
    "json", 
    "gzip", 
    args["S3_OUTPUT_PATH"], 
    glueContext)

job.commit()
