"""
Superstore Unload Script.
"""

import os
import pandas as pd
from pathlib import Path
from datetime import datetime
import json
import boto3
from util.utils import create_redshift_conn, read_sql_file, run_redshift_query

ssm_client = boto3.client('ssm', region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')

current_dir = Path(os.getcwd())

# Query placeholders
S3_PATH = "<s3_path>"
IAM_ROLE = "<iam_role>"
INS_DTM = "<ins_dtm>"


if __name__ == '__main__':
    
    
    ltst_rntm = datetime.today().strftime('%Y-%m-%d')
    feed_name='sales'
    parameter = ssm_client.get_parameter(Name='superstore_config', WithDecryption=True)
    config = json.loads(parameter['Parameter']['Value'])

    # Get sql file 
    sql_file_path = Path(config['path_sql'] + 'sql_tgt_' + feed_name + '.ini')
    load_sql_query = read_sql_file(sql_file_path, section='query', param='load')
    unload_sql_query = read_sql_file(sql_file_path, section='query', param='unload')
    source_s3_path = config['unload_path'] + '/' + ltst_rntm + '/'
    destination_s3_path = config['output_path'] + '/' + ltst_rntm + '/'
    ins_dtm = ltst_rntm + '%'

    # Replace placeholders in LOAD command
    load_sql_query = load_sql_query. \
        replace(INS_DTM, ins_dtm)
        
    
    # Replace placeholders in UNLOAD command
    unload_sql_query = unload_sql_query. \
        replace(S3_PATH, destination_s3_path). \
        replace(IAM_ROLE, config['db_config']['iam_role'])
               
    #Create database connection to Redshift
    rs_conn = create_redshift_conn(config=config['db_config'])

    if rs_conn:
        print("Redshift database connection - Successful")
        
        # Load processed data to target table from S3
        step = 'Load'
        status = run_redshift_query(rs_conn, load_sql_query, feed_name, step)

        if status=='Success':
            #unload output data to S3 target location
            step='Unload'
            run_redshift_query(rs_conn, unload_sql_query, feed_name, step)

        
    else:
        print("Redshift database connection - Failed")
