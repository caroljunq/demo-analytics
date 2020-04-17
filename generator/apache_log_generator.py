import common_functions
import numpy as np
import pandas as pd
import random
import mimesis
import json
import boto3


# firehose = boto3.client('firehose',region_name='us-east-1')

# reading config
with open('../config.json') as data:
    config = json.load(data)

# general config
language = config["language"]

# apache logs config
n_logs = config["apache_logs"]["total"]
log_start_date = config["apache_logs"]["log_start_date"]
log_end_date = config["apache_logs"]["log_end_date"]

# products config
n_products = config["products"]["total"]

# customers config
n_customers = config["customers"]["total"]

# campaign config
campaigns = config[language]["campaigns"]
n_campaigns = len(campaigns) 

# media config
media_sources = config["media"]["sources"]
media_prob = config["media"]["percentages"]

internet = mimesis.Internet()

# 203.93.245.97 - oracleuser [28/Sep/2000:23:59:07 -0700] "GET 
# /files/search/search.jsp?s=driver&a=10 HTTP/1.0" 200 2374 
# "http://datawarehouse.us.oracle.com/datamining/contents.htm" "Mozilla/4.7 [en] 
# (WinNT; I)"

logs_headers = [
'"Mozilla/5.0 (Macintosh; Intel Mac OS X 10.12; rv:59.0) Gecko/20100101 Firefox/59.0"'
,'"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0"'
,'"Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.0"'
,'"Mozilla/5.0 (Windows; Intel x86) Gecko/20100101 Firefox/59.0"'
,'"Mozilla/5.0 (Windows; Intel x86) Gecko/20100101 Chrome/64.0"'
,'"Mozilla/5.0 (Windows; Intel x86) Gecko/20100101 IE/32.0"'
,'"Mozilla/5.0 (Linux; Intel x86) Gecko/20100101 Firefox/59.0"'
,'"Mozilla/5.0 (Linux; Intel x86) Gecko/20100101 Chrome/64.0"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone6) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone6s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone6s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone6s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone7) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhone8s) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Apple; IOS 11.0.2; IPhoneX) AppleWebKit/537.36 (KHTML, like Gecko) Safari/59.00"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; ONEPLUS) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; MOTOROLA) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/68.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; SAMSUNG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0"'
,'"Mozilla/5.0 (Linux; Android 8.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 7.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/67.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0"'
,'"Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/65.0"'
]

# Example
# "104.123.196.99 - - [2018-04-09 00:28:24 +0000] ""GET / HTTP/1.1"" 200 38 ""id_cliente=67027&id_campaign=8&source=Website-ads&prod_id=1411 ""Mozilla/5.0 (Linux; Android 6.1.0; LG) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/66.0"""

log_size = len(logs_headers)
print("Initializing...")


# firehose = boto3.client('firehose',region_name="us-east-1")

for i in range(n_logs):	
    customer_id = random.randint(1,n_customers)
    order_date = common_functions.random_date(log_start_date,log_end_date,random.random())
    campaign_id = random.randint(1,n_campaigns)
    media_source = np.random.choice(media_sources, p=media_prob)  
    product_id = random.randint(1,n_products)
    ip = internet.ip_v4()
    seed2 = random.randint(0,log_size - 1)
    log = str(ip)+" - - ["+order_date+" +0000] \"GET / HTTP/1.1\" 200 38 \"id_cliente="+str(customer_id)+"&id_campaign="+str(campaign_id)+"&source="+str(media_source)+"&prod_id="+str(product_id)+" "+logs_headers[seed2] + "\n"
    print(log)

    # response = firehose.put_record(
    #     DeliveryStreamName='retail-delivery-stream',
    #     Record={
    #         'Data': log
    #      }
    # )

print("Finishing...")
