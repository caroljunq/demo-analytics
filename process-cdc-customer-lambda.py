import json
import boto3

def lambda_handler(event, context):
    s3 = boto3.client('s3')
    
    # retrieve bucket name and file_key from the S3 event
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    
    # file name
    file_key = event['Records'][0]['s3']['object']['key']
    new_name = "processed-data/cdc-customers/" + file_key.split("/")[4]
    new_content = ""
 
    print(new_name)
    # get the object
    obj = s3.get_object(Bucket=bucket_name, Key=file_key)
    count = 0
    
    # get lines inside the csv
    lines = obj['Body'].read().decode('utf-8').splitlines()
    
    # skip first row = header
    n_customers = range(1,len(lines))
    
    for i in n_customers:
        count += 1
        fields = lines[i].split(",")
        new_content += f"{fields[1]},{fields[2]},{fields[3]},{fields[4]},{fields[5]}\n"
        
    
    s3.put_object(Body=new_content, Bucket=bucket_name, Key=new_name)
    print(str(count) + ' customers were processed!')
    
    return {
        'statusCode': 200,
        'body': json.dumps('Hello from Lambda!')
    }
