# run.py
#
# Copyright (C) 2011-2019 Vas Vasiliadis
# University of Chicago
#
# Wrapper script for running AnnTools
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import os
import sys
import time
import driver
import shutil
import boto3
import time
import json
import configparser
from botocore.config import Config
from botocore.exceptions import ClientError

"""A rudimentary timer for coarse-grained profiling
"""
config = configparser.ConfigParser()
config.read('ann_config.ini')

# RESULT_BUCKET = 'mpcs-cc-gas-results'
RESULT_BUCKET = config.get('aws', 'S3ResultBucket')

class Timer(object):
  def __init__(self, verbose=True):
    self.verbose = verbose

  def __enter__(self):
    self.start = time.time()
    return self

  def __exit__(self, *args):
    self.end = time.time()
    self.secs = self.end - self.start
    if self.verbose:
       print(f"Approximate runtime: {self.secs:.2f} seconds")

def parse_path(path):
    # parse path to get job_id and file_name and dir_name
    dir_name, file_name = os.path.dirname(path), os.path.basename(path)[:-4]
    job_id = os.path.basename(dir_name)
    return dir_name, file_name, job_id        


def update_dynamo_to_running(job_id):
    # reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
    try:
        dynamo = boto3.client('dynamodb')
        # table = 'gaoyunl1_annotations'
        table = config.get('aws', 'DynamoDBTableName')
        response = dynamo.update_item(TableName = table, 
                                    Key={'job_id':{'S': job_id}},
                                    UpdateExpression='SET job_status = :new_value',
                                    ConditionExpression="job_status = :pending_status",
                                    ExpressionAttributeValues={':new_value':{'S': 'RUNNING'},
                                                               ':pending_status':{'S':'PENDING'}}
                                    )
        print(response)
        return True
    except Exception as e:
        print(e)
        return False


def update_dynamo_to_complete(job_id, log_file_key, result_file_key):
    # reference: https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/GettingStarted.UpdateItem.html
    try:
        dynamo = boto3.client('dynamodb')
        # table = 'gaoyunl1_annotations'
        table = config.get('aws', 'DynamoDBTableName')
        response = dynamo.update_item(TableName = table, 
                                    Key={'job_id':{'S': job_id}},
                                    UpdateExpression='SET job_status = :status_value, s3_results_bucket=:res_bucket_value, s3_key_result_file = :res_key_value, s3_key_log_file = :log_key_value, complete_time = :ct',
                                    ExpressionAttributeValues={
                                      ':status_value':{'S':'COMPLETED'},
                                      ':res_bucket_value':{'S': RESULT_BUCKET},
                                      ':res_key_value': {'S': result_file_key},
                                      ':log_key_value': {'S': log_file_key},
                                      ':ct':{'N':str(int(time.time()))}
                                      },
                                    )
        print(response)
        return response
    except Exception as e:
        print(e)


def upload_files(dir_name, file_name, job_id, user):
    # reference: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/s3-uploading-files.html
    try:
        s3 = boto3.client('s3', region_name=config.get('aws', 'AwsRegionName'))
        keys = []
        for result_file in [file_name + '.vcf.count.log', file_name + '.annot.vcf']:
            file_path = os.path.join(dir_name, result_file)
            key = 'gaoyunl1/' + user + '/' + job_id + '/' + result_file
            s3.upload_file(file_path, RESULT_BUCKET, key)  # upload file to S3
            keys.append(key)
        print('Checkpoint: Files upload to s3 completed')

        update_dynamo_to_complete(job_id, keys[0], keys[1])
        print('Checkpoint: Update to dynamo completed')

        shutil.rmtree(dir_name)
        print('Checkpoint: Local files removed')
    except FileNotFoundError as e:
       print(e)
    except ClientError as e:
       print(e)
    except Exception as e:
       print(e)

def publish_to_sns(data):
    # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    try:
        sns = boto3.client('sns', region_name=config.get('aws', 'AwsRegionName'))
        # topic_arn = 'arn:aws:sns:us-east-1:659248683008:gaoyunl1_job_requests'
        topic_arn = config.get('aws', 'SNSJobResultTopic')
        message = json.dumps(data)
        subject = 'New Annotation Job Results'
        response = sns.publish(TopicArn=topic_arn, Message=message, Subject=subject)
        print(f'SNS response: {response}')
        return response
    except ClientError as e:
        raise
    except Exception as e:
        raise

if __name__ == '__main__':
# Call the AnnTools pipeline
    if len(sys.argv) > 1:
        path, user, name, email = sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4]
        dir_name, file_name, job_id = parse_path(path)
        if update_dynamo_to_running(job_id):
            with Timer():
                driver.run(sys.argv[1], 'vcf')
            upload_files(dir_name, file_name, job_id, user)  # upload files to S3
            data = {'job_id': job_id, 'user_id': user, 'user_name': name, 'user_email': email}
            publish_to_sns(data)
    else:
        print("A valid .vcf file must be provided as input to this program.")

### EOF