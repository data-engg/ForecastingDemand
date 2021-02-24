import time
from datetime import datetime
from datetime import timedelta
import random
import numpy as np
from configparser import ConfigParser
import getpass
from kafka import KafkaProducer
import json
from common.cassandra_util import get_job_params


geo_region_cd = "US"
scan_types = [0, 1, 2, 3, 4]
scan_dept_nbrs = [i for i in range(1, 99)]
scan_ids = [i for i in range(1, 99999999)]
ts = [-2, 2]

currency_codes = ['USD','ARS','AUD','BHD','BWP','BRL','GBP','BND','BGN','CAD'
                ,'CLP','CNY','COP','HRK','CZK','DKK' ,'AED','EUR','HKD','HUF','ISK','INR',
                  'IDR','IRR','JPY','LYD','MYR','MUR','MXN','NPR','NZD','PHP','RUB','SGD','ZAR','CHF']

sold_qts = [i for i in np.arange(5.5, 100.5, 2.5)]

# Fetch job parameter
job_params = get_job_params('producer')

# Kafka producer setup
config = ConfigParser()
config.read("/mnt/home/{user}/.odbc.ini".format(user=getpass.getuser()))

price_change_producer = KafkaProducer(bootstrap_servers=config['kafka']['bootstrap_servers'],
                                      acks=1,
                                      key_serializer=lambda x: str(x).encode('UTf-8'),
                                      value_serializer=lambda x: json.dumps(x).encode('UTf-8'))

sales_transaction_producer = KafkaProducer(bootstrap_servers=config['kafka']['bootstrap_servers'],
                                           acks=1,
                                           key_serializer=lambda x: str(x).encode('UTf-8'),
                                           value_serializer=lambda x: json.dumps(x).encode('UTf-8'))
# Price change event publish
with open('price_change_event.txt', 'r') as price_change_obj:
    all_price_change_lines = price_change_obj.readlines()
for line in all_price_change_lines:
    current_timestamp = datetime.now()
    row_insertion_dttm = str(current_timestamp)[0:19]  # common for both tables
    price_chng_activation_ts = str(current_timestamp - timedelta(minutes=5))[0:19]
    item_id, store_id, price_change_reason, prev_price_amt, curr_price_amt = line.split("|")

    price_change_event = dict()
    price_change_event['item_id'] = item_id
    price_change_event['store_id'] = store_id
    price_change_event['price_chng_activation_ts'] = price_chng_activation_ts
    price_change_event['geo_region_cd'] = geo_region_cd
    price_change_event['price_change_reason'] = price_change_reason
    price_change_event['prev_price_amt'] = float(prev_price_amt)
    price_change_event['curr_price_amt'] = float(curr_price_amt.replace('\n', ''))
    price_change_event['row_insertion_dttm'] = row_insertion_dttm
    price_change_producer.send(job_params['price_change_topic'], key=store_id, value=price_change_event)

    # Sales tranasction publish
    no_of_sales = [1, 2, 3, 4]
    i = 0
    while i < random.choice(no_of_sales):
        price_chng_activation_ts_ts = datetime.strptime(price_chng_activation_ts, '%Y-%m-%d %H:%M:%S')
        sales_timestamp = str(price_chng_activation_ts_ts - timedelta(minutes=random.choice(ts)))[0:19]
        scan_type = random.choice(scan_types)
        currency_code = random.choice(currency_codes)
        scan_id = random.choice(scan_ids)
        sold_unit_quantity = float(random.choice(sold_qts))
        scan_dept_nbr = random.choice(scan_dept_nbrs)

        sales_transaction = dict()
        sales_transaction['store_id'] = store_id
        sales_transaction['item_id'] = item_id
        sales_transaction['scan_type'] = int(scan_type)
        sales_transaction['geo_region_cd'] = geo_region_cd
        sales_transaction['currency_code'] = currency_code
        sales_transaction['scan_id'] = scan_id
        sales_transaction['sold_unit_quantity'] = float(sold_unit_quantity)
        sales_transaction['sales_timestamp'] = sales_timestamp
        sales_transaction['scan_dept_nbr'] = int(scan_dept_nbr)
        sales_transaction['row_insertion_dttm'] = row_insertion_dttm
        sales_transaction_producer.send(job_params['sales_trans_topic'], key=store_id, value=sales_transaction)
        i = i + 1
    time.sleep(15)