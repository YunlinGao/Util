import json
import subprocess
import os
import re
import boto3
import configparser
from botocore.config import Config
from botocore.exceptions import ClientError

config = configparser.ConfigParser()
config.read('ann_config.ini')

# ANNTOOLS_DRIVER_PATH = './anntools/run.py'
# RESULTS_PATH = './results'

# s3 = boto3.client('s3', region_name='us-east-1', config=Config(signature_version="s3v4"))
s3 = boto3.client('s3', region_name=config.get('aws', 'AwsRegionName'), config=Config(signature_version="s3v4"))

# --------------------- HELPER FUNCTIONS --------------------------

def read_log(path):
    with open(path, 'r') as f:
        lines = f.readlines()
    return lines
    
def poll_sqs_messages():
    # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
    sqs = boto3.resource('sqs', region_name=config.get('aws', 'AwsRegionName'))
    # url = 'https://sqs.us-east-1.amazonaws.com/659248683008/gaoyunl1_job_requests'
    url = config.get('aws', 'SQSRequestQueueUrl')
    queue = sqs.Queue(url)
    while True:
        # messages = queue.receive_messages(WaitTimeSeconds=10)
        messages = queue.receive_messages(WaitTimeSeconds=config.getint('aws', 'SQSPollingWaitTime'))
        for message in messages:
            try:
                sqs_response = json.loads(message.body)
                data = json.loads(sqs_response['Message'])
                response = query_ann_jobs(data)
                print(response)
            except Exception as e:
                print(e)
                raise
            finally:
                message.delete()
        print('Done with loop')
    
def query_ann_jobs(data):
    try:
        bucket = data['s3_inputs_bucket']
        job_id = data['job_id']
        file_name = data['input_file_name']
        user = data['user_id']
        key = data['s3_key_input_file']
        user_name = data['user_name']
        user_email = data['user_email']
        user_role = data['user_role']
        
        # job_dir_path = os.path.join(RESULTS_PATH, job_id)
        job_dir_path = os.path.join(config.get('anntools', 'ResultPath'), job_id)
        os.mkdir(job_dir_path)

        file_path = os.path.join(job_dir_path, file_name)

        s3.download_file(bucket, key, file_path)

        # subprocess.Popen(["python", ANNTOOLS_DRIVER_PATH, file_path, user])
        subprocess.Popen(["python", config.get('anntools', 'DriverPath'), file_path, user, user_name, user_email])
        
        content = {"code": 201, "data": {"job_id": job_id, "input_file": file_name}}
    except ClientError as e:
        content = {"code": 500, "status": "error", "message": str(e)}
    except Exception as e:
        content = {"code": 500, "status": "error", "message": str(e)}
    finally:
        return content

if __name__ == '__main__':
    poll_sqs_messages()