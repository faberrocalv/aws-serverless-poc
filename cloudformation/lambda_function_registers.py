import boto3
import os
import json
import logging

# 
s3 = boto3.client('s3')
sqs = boto3.client('sqs')

# Configurar el logger
logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    
    # Ver cuántos registros (mensajes) se recibieron en el evento
    records = event.get('Records', [])
    logger.info('Se recibieron {} mensajes de SQS para procesar'.format(len(records)))

    # Get S3 bucket name
    registers_bucket_name = os.environ.get('REGISTERS_BUCKET')
    
    # Define the queue URL
    queue_url = os.environ.get('QUEUE_URL')
    
    for record in records:
        # Cada record contiene un mensaje de SQS
        try:
            # Aquí puedes agregar tu lógica de procesamiento del mensaje
            
            logger.info('Procesando registro...')
            
            message_body = record['body']
            message_attributes = record['messageAttributes']
            message_receipt_handle = record['receiptHandle']
            
            logger.info('Procesando registro para cliente {}'.format(message_attributes['id']['stringValue']))
            
            json_data = {
                'id': message_attributes['id']['stringValue'],
                'name': message_attributes['name']['stringValue'],
                'email': message_attributes['email']['stringValue']
            }
            
            logger.info('Guardando json en S3...')

            # Put json object in bucket
            s3.put_object(
                Body=json.dumps(json_data),
                Bucket=registers_bucket_name,
                Key = '{}.json'.format(json_data['id'])
            )
            
            logger.info('Eliminando registro procesado de la cola...')
            
            # Delete processed message from queue
            dlt_response = sqs.delete_message(
                QueueUrl=queue_url,
                ReceiptHandle=message_receipt_handle
            )
            
            logger.info('Registro procesado exitosamente...')
            
        except Exception as e:
            # Si hay un error al procesar un mensaje, lo logueamos.
            logger.error('Error al procesar el mensaje: {}'.format(e))
    
    return {
        'statusCode': 200,
        'body': json.dumps('Registros procesados exitosamente')
    }

