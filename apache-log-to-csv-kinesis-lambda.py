import json
import boto3
import base64
import os
from datetime import datetime

print('Loading function')

def lambda_handler(event, context):

    output = []
    
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
        message = customer_id + ',' + order_date_date + ',' + order_date_hour + ',' + campaign_id + ',' + prod_id + '\n'

        message_bytes = message.encode('ascii')
        base64_bytes = base64.b64encode(message_bytes)
        base64_message = base64_bytes.decode('ascii')
        
        output_record = {
            'recordId': record['recordId'],
            'result': 'Ok',
            'data': base64_message
        }
        output.append(output_record)
        
        
    print('Successfully processed {} records.'.format(len(event['records'])))
    
    return {'records': output}
    
