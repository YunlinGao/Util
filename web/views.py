# views.py
#
# Copyright (C) 2011-2020 Vas Vasiliadis
# University of Chicago
#
# Application logic for the GAS
#
##
__author__ = 'Vas Vasiliadis <vas@uchicago.edu>'

import uuid
import time
import json
from datetime import datetime

import boto3
from boto3.dynamodb.conditions import Key
from botocore.client import Config
from botocore.exceptions import ClientError

from flask import (abort, flash, redirect, render_template,
  request, session, url_for)

from gas import app, db
from decorators import authenticated, is_premium
from auth import get_profile, update_profile


# ---------------------- HELPER FUNCTIONS ---------------------------- #

def insert_dynamo(item):
    # Reference: https://docs.python.org/3/library/time.html
    try:
        dynamo = boto3.resource('dynamodb')
        # table = dynamo.Table('gaoyunl1_annotations')
        table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
        item['submit_time'] = int(time.time())
        item['job_status'] = 'PENDING'
        response = table.put_item(Item = item)
        print(f'Dynamo resonse: {response}')
    except ClientError as e:
        app.logger.error(f"Client Error when inserting to DynamoDb: {e}") 
    except Exception as e:
        app.logger.error(f"Error when inserting to DynamoDb: {e}")

def publish_to_sns(data):
    # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sns/client/publish.html
    try:
        sns = boto3.client('sns', region_name=app.config['AWS_REGION_NAME'])
        # topic_arn = 'arn:aws:sns:us-east-1:659248683008:gaoyunl1_job_requests'
        topic_arn = app.config['AWS_SNS_JOB_REQUEST_TOPIC']
        message = json.dumps(data)
        subject = 'New Annotation Job Request'
        response = sns.publish(TopicArn=topic_arn, Message=message, Subject=subject)
        print(f'SNS response: {response}')
        return response
    except ClientError as e:
        app.logger.error(f"Client Error when publishing to SNS: {e}")
        raise
    except Exception as e:
        app.logger.error(f"Error when publishing to SNS: {e}")
        raise

def ephoch_to_readable_time(epoch):
  return datetime.fromtimestamp(epoch).strftime('%Y-%m-%d %H:%M:%S')

def create_presigned_download_url(object_name):
    # Create a presigned url for downloading the results of an annotation job
    # Reference: https://docs.aws.amazon.com/AmazonS3/latest/userguide/ShareObjectPreSignedURL.html
    s3_client = boto3.client('s3', 
                             region_name=app.config['AWS_REGION_NAME'],
                             config=Config(signature_version='s3v4'))
    bucket_name = app.config['AWS_S3_RESULTS_BUCKET']

    try:
        response = s3_client.generate_presigned_url('get_object',
                                                    Params={'Bucket': bucket_name,
                                                            'Key': object_name},
                                                    ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
    except ClientError as e:
        app.logger.error(f"Client Error when creating presigned url: {e}")
        raise
    except Exception as e:
        app.logger.error(f"Error when creating presigned url: {e}")
        raise
    app.logger.info(f"Presigned download url: {response}")
    # The response contains the presigned URL
    return response

# ------------------------- API ENDPOINTS ---------------------------- #
"""Start annotation request
Create the required AWS S3 policy document and render a form for
uploading an annotation input file using the policy document.

Note: You are welcome to use this code instead of your own
but you can replace the code below with your own if you prefer.
"""
@app.route('/annotate', methods=['GET'])
@authenticated
def annotate():
  # Create a session client to the S3 service
  s3 = boto3.client('s3',
    region_name=app.config['AWS_REGION_NAME'],
    config=Config(signature_version='s3v4'))

  bucket_name = app.config['AWS_S3_INPUTS_BUCKET']
  user_id = session['primary_identity']

  # Generate unique ID to be used as S3 key (name)
  key_name = app.config['AWS_S3_KEY_PREFIX'] + user_id + '/' + \
    str(uuid.uuid4()) + '~${filename}'

  # Create the redirect URL
  redirect_url = str(request.url) + '/job'

  # Define policy fields/conditions
  encryption = app.config['AWS_S3_ENCRYPTION']
  acl = app.config['AWS_S3_ACL']
  fields = {
    "success_action_redirect": redirect_url,
    "x-amz-server-side-encryption": encryption,
    "acl": acl
  }
  conditions = [
    ["starts-with", "$success_action_redirect", redirect_url],
    {"x-amz-server-side-encryption": encryption},
    {"acl": acl}
  ]

  # Generate the presigned POST call
  try:
    presigned_post = s3.generate_presigned_post(
      Bucket=bucket_name, 
      Key=key_name,
      Fields=fields,
      Conditions=conditions,
      ExpiresIn=app.config['AWS_SIGNED_REQUEST_EXPIRATION'])
  except ClientError as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
    return abort(500)
    
  # Render the upload form which will parse/submit the presigned POST
  return render_template('annotate.html', s3_post=presigned_post)


"""Fires off an annotation job
Accepts the S3 redirect GET request, parses it to extract 
required info, saves a job item to the database, and then
publishes a notification for the annotator service.

Note: Update/replace the code below with your own from previous
homework assignments
"""
@app.route('/annotate/job', methods=['GET'])
@authenticated
def create_annotation_job_request():

  # Get bucket name, key, and job ID from the S3 redirect URL
  bucket_name = str(request.args.get('bucket'))
  s3_key = str(request.args.get('key'))
  print(f'bucket_name: {bucket_name}')
  print(f's3_key: {s3_key}')

  # Extract the job ID from the S3 key
  _, user, object_name = s3_key.split('/')
  job_id, file_name = object_name.split('~')

  # Get user profile
  profile = get_profile(user)

  # Persist job to database
  data = {'user_id':user, 
          'job_id':job_id, 
          'input_file_name':file_name,
          's3_inputs_bucket':bucket_name, 
          's3_key_input_file': s3_key,
          'user_name':profile.name,
          'user_email':profile.email,
          'user_institution':profile.institution,
          'user_role':profile.role}
  try:
    insert_dynamo(data)
  except Exception as e:
    app.logger.error(f"Unable to persist job to database: {e}") 
  app.logger.info("Check point: insert_dynamo done")

  # Send message to request queue
  try:
    publish_to_sns(data)
  except Exception as e:
    app.logger.error(f"Unable to generate presigned URL for upload: {e}")
  app.logger.info("Check point: publish_to_sns done")

  return render_template('annotate_confirm.html', job_id=job_id)


"""List all annotations for the user
"""
@app.route('/annotations', methods=['GET'])
@authenticated
def annotations_list():
  user_id = session['primary_identity']
  try:
    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = table.query(IndexName='user_id_index',
                           KeyConditionExpression=Key('user_id').eq(user_id))
  except ClientError as e:
    app.logger.error(f"Unable to retrieve annotation jobs from database: {e}")
    return abort(500)
  except Exception as e:
    app.logger.error(f"Unable to retrieve annotation jobs from database: {e}")
    return abort(500)
  
  # Get list of annotations to display
  annotations = response['Items']
  annotations.sort(key=lambda x: x['submit_time'], reverse=True)
  # Convert submit_time from epoch to string for all annotations
  for annotation in annotations:
    annotation['submit_time'] = ephoch_to_readable_time(annotation['submit_time'])
  app.logger.info(f"Retrieved {len(annotations)} annotations from database")
  return render_template('annotations.html', annotations=annotations)


"""Display details of a specific annotation job
"""
@app.route('/annotations/<id>', methods=['GET'])
@authenticated
def annotation_details(id):
  try:
    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = table.get_item(Key={'job_id': id})
  except ClientError as e:
    app.logger.error(f"Unable to retrieve annotation job {id} from database: {e}")
    return abort(500)
  except Exception as e:
    app.logger.error(f"Unable to retrieve annotation job {id} from database: {e}")
    return abort(500)
  annotation = response['Item']
  annotation['submit_time'] = ephoch_to_readable_time(annotation['submit_time'])
  # if the job is complete, convert the complete_time from epoch to string, generate presigned URL for result file
  if 'complete_time' in annotation:
    annotation['complete_time'] = ephoch_to_readable_time(annotation['complete_time'])  # convert complete_time from epoch to readable time
    annotation['result_file_url'] = create_presigned_download_url(annotation['s3_key_result_file'])  # generate presigned URL for result file download
  app.logger.info(f"Retrieved annotation job {id} from database")
  if annotation['user_id'] != session['primary_identity']:
    return abort(403)
  return render_template('annotation_details.html', annotation=annotation)


"""Display the log file contents for an annotation job
"""
@app.route('/annotations/<id>/log', methods=['GET'])
@authenticated
def annotation_log(id):
  # Get the annotation job info from the DynamoDB table
  try:
    dynamo = boto3.resource('dynamodb', region_name=app.config['AWS_REGION_NAME'])
    table = dynamo.Table(app.config['AWS_DYNAMODB_ANNOTATIONS_TABLE'])
    response = table.get_item(Key={'job_id': id})
  except ClientError as e:
    app.logger.error(f"Unable to retrieve annotation job {id} from database: {e}")
    return abort(500)
  except Exception as e:
    app.logger.error(f"Unable to retrieve annotation job {id} from database: {e}")
    return abort(500)
  annotation = response['Item']

  # Verify that the user is authorized to view this log file
  if annotation['user_id'] != session['primary_identity']:
    return abort(403)
  
  bucket = app.config['AWS_S3_RESULTS_BUCKET']
  log_file_key = annotation['s3_key_log_file']
  
  # Get the log file contents from S3
  # Reference: https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/s3/client/get_object.html
  try:
    s3 = boto3.client('s3', region_name=app.config['AWS_REGION_NAME'])
    response = s3.get_object(Bucket=bucket, Key=log_file_key)
    log_file_contents = response['Body'].read().decode('utf-8')
    app.logger.info(f"Retrieved log file for annotation job {id} from S3")
  except ClientError as e:
    app.logger.error(f"Unable to retrieve log file for annotation job {id} from S3: {e}")
    return abort(500)
  except Exception as e:
    app.logger.error(f"Unable to retrieve log file for annotation job {id} from S3: {e}")
    return abort(500)
  
  return render_template('view_log.html', log_file_contents=log_file_contents, job_id=id)


"""Subscription management handler
"""
@app.route('/subscribe', methods=['GET', 'POST'])
@authenticated
def subscribe():
  if (request.method == 'GET'):
    # Display form to get subscriber credit card info
    if (session.get('role') == "free_user"):
      return render_template('subscribe.html')
    else:
      return redirect(url_for('profile'))

  elif (request.method == 'POST'):
    # Update user role to allow access to paid features
    update_profile(
      identity_id=session['primary_identity'],
      role="premium_user"
    )

    # Update role in the session
    session['role'] = "premium_user"

    # Request restoration of the user's data from Glacier
    # Add code here to initiate restoration of archived user data
    # Make sure you handle files not yet archived!

    # Display confirmation page
    return render_template('subscribe_confirm.html') 

"""Reset subscription
"""
@app.route('/unsubscribe', methods=['GET'])
@authenticated
def unsubscribe():
  # Hacky way to reset the user's role to a free user; simplifies testing
  update_profile(
    identity_id=session['primary_identity'],
    role="free_user"
  )
  return redirect(url_for('profile'))


"""DO NOT CHANGE CODE BELOW THIS LINE
*******************************************************************************
"""

"""Home page
"""
@app.route('/', methods=['GET'])
def home():
  return render_template('home.html')

"""Login page; send user to Globus Auth
"""
@app.route('/login', methods=['GET'])
def login():
  app.logger.info(f"Login attempted from IP {request.remote_addr}")
  # If user requested a specific page, save it session for redirect after auth
  if (request.args.get('next')):
    session['next'] = request.args.get('next')
  return redirect(url_for('authcallback'))

"""404 error handler
"""
@app.errorhandler(404)
def page_not_found(e):
  return render_template('error.html', 
    title='Page not found', alert_level='warning',
    message="The page you tried to reach does not exist. \
      Please check the URL and try again."
    ), 404

"""403 error handler
"""
@app.errorhandler(403)
def forbidden(e):
  return render_template('error.html',
    title='Not authorized', alert_level='danger',
    message="You are not authorized to access this page. \
      If you think you deserve to be granted access, please contact the \
      supreme leader of the mutating genome revolutionary party."
    ), 403

"""405 error handler
"""
@app.errorhandler(405)
def not_allowed(e):
  return render_template('error.html',
    title='Not allowed', alert_level='warning',
    message="You attempted an operation that's not allowed; \
      get your act together, hacker!"
    ), 405

"""500 error handler
"""
@app.errorhandler(500)
def internal_error(error):
  return render_template('error.html',
    title='Server error', alert_level='danger',
    message="The server encountered an error and could \
      not process your request."
    ), 500

### EOF
