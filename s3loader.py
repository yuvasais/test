""" For pulling data from AW S3 buckets -Importing the necessary Python Modules"""
import boto3
import sys
import os
import pandas as pd
import csv
import io
from properties import *
from io import StringIO # Python 3.x
class s3:
    def load_s3(self):
            
        client = boto3.client('s3', aws_access_key_id=aws_id,
                aws_secret_access_key=aws_secret)

       
        csv_obj = client.get_object(Bucket=bucket_name, Key=object_key)
        body = csv_obj['Body']
        csv_string = body.read().decode('utf-8')

        df_s3 = pd.read_csv(StringIO(csv_string))
        print(df_s3) 
        return df_s3
