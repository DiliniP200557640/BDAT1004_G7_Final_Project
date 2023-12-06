"""
Superstore Load Script.
"""

from kaggle.api.kaggle_api_extended import KaggleApi
import os
import pandas as pd
from pathlib import Path
from datetime import datetime
import json
import boto3
from io import StringIO
from util.utils import create_redshift_conn, read_sql_file, run_redshift_query

ssm_client = boto3.client('ssm', region_name='us-east-1')
s3_client = boto3.client('s3', region_name='us-east-1')

current_dir = Path(os.getcwd())

# Query placeholders
S3_PATH = "<s3_path>"
IAM_ROLE = "<iam_role>"

def extract_data(dataset_path, dataset):

    try:
        #downloading dataset
        api.dataset_download_files(dataset_path)

        from zipfile import ZipFile
        zf = ZipFile(dataset)

        #extracted data is saved in the same directory as notebook
        zf.extractall() 
        zf.close()

    except Exception as e:
        print('Error Occured : {}'.format(e))


if __name__ == '__main__':
    
    
    ltst_rntm = datetime.today().strftime('%Y-%m-%d')
    feed_name='sales'
    step = 'Load'
    parameter = ssm_client.get_parameter(Name='superstore_config', WithDecryption=True)
    config = json.loads(parameter['Parameter']['Value'])
    
    dataset_path = config['dataset_path']
    dataset = config['dataset']
    api = KaggleApi()
    api.authenticate()

    extract_data(dataset_path,dataset)
    data=pd.read_csv('Sample - Superstore.csv', encoding='windows-1254')
    print(data.head(5))

    #data cleaning
    nulls = data.isnull().sum()

    if nulls.any() > 0:
        data = data.dropna()
    else:
        print("No Nulls Found")

    # Find the number of duplicate data
    duplicates = data.duplicated().sum()

    if duplicates > 0:
        
        # Show the duplicated rows
        data[data.duplicated(keep = 'last')]
        
        # Drop the duplicated rows
        data.drop_duplicates(inplace = True)
        # Find the no. of rows and columns
        data.shape
    else:
        print("No Duplicates Found")

    
    # Drop column 'row_id' from the DataFrame
    data = data.drop("Row ID", axis=1)

    # Convert DataFrame to CSV in memory (StringIO)
    csv_buffer = StringIO()
    data.to_csv(csv_buffer, index=False)

    # Upload CSV file to S3
    
    file_name= 'processed_data/Sales/'+ltst_rntm+'/processed_data.csv'
    s3_client.put_object(Bucket=config['input_bucket'], Key=file_name, Body=csv_buffer.getvalue())

    # Get sql file 
    sql_file_path = Path(config['path_sql'] + 'sql_stg_' + feed_name + '.ini')
    sql_query = read_sql_file(sql_file_path, section='query', param='load')
    unload_s3_path = config['unload_path'] + '/' + ltst_rntm + '/'

    # Replace placeholders in UNLOAD command
    sql_query = sql_query. \
        replace(S3_PATH, unload_s3_path). \
        replace(IAM_ROLE, config['db_config']['iam_role'])
    
               
    #Create database connection to Redshift
    rs_conn = create_redshift_conn(config=config['db_config'])

    if rs_conn:
        print("Redshift database connection - Successful")
        
        run_redshift_query(rs_conn, sql_query, feed_name, step)

        
    else:
        print("Redshift database connection - Failed")
