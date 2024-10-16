import boto3
import os
import json

# Create an SQS and S3 clients
sqs = boto3.client('sqs')
s3 = boto3.client('s3')

def lambda_handler(event, context):
    try:
        # Define the queue URL
        queue_url = os.environ.get('QUEUE_URL')
    
        # Get S3 bucket name
        registers_bucket_name = os.environ.get('REGISTERS_BUCKET')
    
        # Receive message from SQS queue
        response = sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=1,
            MessageAttributeNames=[
                'All'
            ],
            VisibilityTimeout=0,
            WaitTimeSeconds=5
        )
    
        # Get message
        message = response['Messages'][0]
        message_body = message['Body']
        message_attributes = message['MessageAttributes']
        message_receipt_handle = message['ReceiptHandle']
    
        json_data = {
            'id': message_attributes['id']['StringValue'],
            'name': message_attributes['name']['StringValue'],
            'email': message_attributes['email']['StringValue']
        }

        # Put json object in bucket
        s3.put_object(
            Body=json.dumps(json_data),
            Bucket=registers_bucket_name,
            Key = '{}.json'.format(json_data['id'])
        )
        
        # Delete processed message from queue
        dlt_response = sqs.delete_message(
            QueueUrl=queue_url,
            ReceiptHandle=message_receipt_handle
        )
                    
        return {
            'status_code': 200,
            'success': message_body
        }
    
    except Exception as e:
        return {
            'status_code': 500,
            'error': 'Error in recieving message: {}'.format(e)
        }