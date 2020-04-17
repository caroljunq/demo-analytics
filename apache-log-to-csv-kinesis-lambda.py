import json
import boto3
import base64
import os
from datetime import datetime

print('Loading function')

def lambda_handler(event, context):
    
    # variable environment 
    BUCKET = os.environ["BUCKET"]
    
    s3 = boto3.client('s3')
    logs = ''
    filename = datetime.now().strftime('%Y-%m-%d_%H-%M-%S') + ".csv"
    for record in event['records']:
        payload_log = base64.b64decode(record['data'])
        elems = str(payload_log).split(' ')
        order_date_date = elems[3][1:]
        order_date_hour = elems[4]
        params = elems[11].replace('Mozilla/5.0','').replace('"','').split('&')
        split_params = [elem.split('=') for elem in params]
        params_values = [elem[1] for elem in split_params]
        customer_id = params_values[0]
        campaign_id = params_values[1]
        media_source = params_values[2]
        prod_id = params_values[3]
        logs += f'{customer_id},{order_date_date} {order_date_hour},{campaign_id},{media_source},{prod_id}\n'
        
    s3.put_object(Body=logs, Bucket=BUCKET, Key=filename)
    print('Processed ' + str(len(logs)) + 'logs')
    return {
        'statusCode': 200,
        'body': json.dumps('Processed ' + str(len(logs)) + 'logs')
    }