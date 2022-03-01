import os
import pyarrow.csv as pv
import pyarrow.parquet as pq
import pyarrow as pa
import logging
import requests
from pathlib import Path


init_url = 'https://nyc-tlc.s3.amazonaws.com/trip+data/'


table_schema_green = pa.schema(
    [
        ('VendorID',pa.string()),
        ('lpep_pickup_datetime',pa.timestamp('s')),
        ('lpep_dropoff_datetime',pa.timestamp('s')),
        ('store_and_fwd_flag',pa.string()),
        ('RatecodeID',pa.int64()),
        ('PULocationID',pa.int64()),
        ('DOLocationID',pa.int64()),
        ('passenger_count',pa.int64()),
        ('trip_distance',pa.float64()),
        ('fare_amount',pa.float64()),
        ('extra',pa.float64()),
        ('mta_tax',pa.float64()),
        ('tip_amount',pa.float64()),
        ('tolls_amount',pa.float64()),
        ('ehail_fee',pa.float64()),
        ('improvement_surcharge',pa.float64()),
        ('total_amount',pa.float64()),
        ('payment_type',pa.int64()),
        ('trip_type',pa.int64()),
        ('congestion_surcharge',pa.float64()),
    ]
)

table_schema_yellow = pa.schema(
   [
        ('VendorID', pa.string()), 
        ('tpep_pickup_datetime', pa.timestamp('s')), 
        ('tpep_dropoff_datetime', pa.timestamp('s')), 
        ('passenger_count', pa.int64()), 
        ('trip_distance', pa.float64()), 
        ('RatecodeID', pa.string()), 
        ('store_and_fwd_flag', pa.string()), 
        ('PULocationID', pa.int64()), 
        ('DOLocationID', pa.int64()), 
        ('payment_type', pa.int64()), 
        ('fare_amount',pa.float64()), 
        ('extra',pa.float64()), 
        ('mta_tax', pa.float64()), 
        ('tip_amount', pa.float64()), 
        ('tolls_amount', pa.float64()), 
        ('improvement_surcharge', pa.float64()), 
        ('total_amount', pa.float64()), 
        ('congestion_surcharge', pa.float64())]

)

def format_to_parquet(src_file, service):
    if not src_file.endswith('.csv'):
        logging.error("Can only accept source files in CSV format, for the moment")
        return
    table = pv.read_csv(src_file)

    if service == 'yellow':
        table = table.cast(table_schema_yellow)
    
    elif service == 'green':
        table = table.cast(table_schema_green)

    pq.write_table(table, src_file.replace('.csv', '.parquet'))



def download_parquet(year, service):
    for i in range(13):
        if i != 12:
            month = '0'+str(i+1)
            month = month[-2:]
            file_name = service + '_tripdata_' + year + '-' + month + '.csv'
            request_url = init_url + file_name
            request = requests.get(request_url)
            print(request.status_code)

            if request.status_code == int('200'):
                
                Path(f"tripdata/{service}/{year}/{month}").mkdir(parents=True, exist_ok=True)
                os.system(f"curl --retry 5 -sSL {request_url} > tripdata/{service}/{year}/{month}/{file_name}")
                print(f"Local: {file_name}")
                parquetized = format_to_parquet(f"tripdata/{service}/{year}/{month}/{file_name}", service)
                os.system(f"rm tripdata/{service}/{year}/{month}/{file_name}")
                file_name = file_name.replace('.csv', '.parquet')
                print(f"Parquet: {file_name}")



download_parquet('2021', 'green')
download_parquet('2020', 'green')
download_parquet('2021', 'yellow')
download_parquet('2020', 'yellow')