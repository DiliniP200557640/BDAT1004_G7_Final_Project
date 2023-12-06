"""
Glue Job to trigger the data pipeline
"""
import boto3

client = boto3.client('datapipeline')
pipeline_id = 'df-0669805282ROXOIEXFRQ'
response = client.activate_pipeline(pipelineId=pipeline_id)



