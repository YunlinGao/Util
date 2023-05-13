import json, boto3
import logging
from botocore.exceptions import ClientError

SENDER = "gaoyunl1@mpcs-cc.com"
REGION = "us-east-1"

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    client = boto3.client("ses", region_name=REGION)
    # Some code about reading information from the event and context
    # variables goes here
    # Grab the user's email from the event and send the notification to them
    for record in event['Records']:
        payload = json.loads(record["body"])
        # logger.info(payload) # This is the payload that we get from the SQS queue for debugging
        message = json.loads(payload['Message'])
        job_id = message['job_id']
        user_id = message['user_id']
        user_name = message['user_name']
        user_email = message['user_email']
        recipient = user_email
        try:
            response = client.send_email(
            Destination = { 'ToAddresses': [recipient] },
            Message = {
                'Body': { 'Text': { 'Data': f'Hi {user_name}, \n The job {job_id} has finished!' } },
                'Subject': { 'Data': 'Job finished!' }
            },
            Source = SENDER
            )
            logger.info(f'Email sent to {recipient} for job {job_id}')
        except ClientError as error:
            logger.error(f'Failed to send email to {recipient} for job {job_id}. Error message: {error.response["Error"]["Message"]}')
            return {
            'statusCode': 500,
            'body': json.dumps(error.response)
            }
    return {
        'statusCode': 200,
        'body': json.dumps(response),
    }