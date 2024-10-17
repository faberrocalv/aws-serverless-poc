import json
import boto3

# Define the lambda handler
sqs = boto3.client('sqs')

# Send the message to the SQS queue
response = sqs.send_message(
    QueueUrl='https://sqs.us-east-1.amazonaws.com/370792723501/UdeMRegistersQueue',
    MessageBody='Registro exitoso',
    MessageAttributes={
        'id': {
            'DataType': 'String',
            'StringValue': '09123456724'
        },
        'name': {
            'DataType': 'String',
            'StringValue': 'Fernando Berrocal'
        },
        'email': {
            'DataType': 'String',
            'StringValue': 'faberrocalv@gmail.com'
        }
    }
)