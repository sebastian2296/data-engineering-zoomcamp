#!/usr/bin/env python
# coding: utf-8

import argparse

import pandas as pd
from sqlalchemy import create_engine
import os
from time import time


def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name

    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
   


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)



    # First chunk of size 100.000
    df = next(df_iter)


    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])




    df.head(0).to_sql(con=engine, name=table_name, if_exists='replace')



    # Append all data to the database

    df.to_sql(con=engine, name=table_name, if_exists='append')





    for i in range(0, 13):
        t_start = time()
        
        df = next(df_iter, 0)
        
        if type(df) != int:
            df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
            df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])

            df.to_sql(con=engine, name='yellow_cabs', if_exists='append')

            t_end = time()

            print('Inserted another chunk in %.3f seconds' % (t_end - t_start))
        
    print('finished')

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres.')

    # user
    # password 
    # host 
    # port 
    # database name,
    # table name
    # url of the csv

    parser.add_argument('--user', help='user name for postgres')
    parser.add_argument('--password', help='password name for postgres')
    parser.add_argument('--host', help='host for postgres')
    parser.add_argument('--port', help='port for postgres')
    parser.add_argument('--db', help='database name for postgres')
    parser.add_argument('--table_name', help='name of the table where we will write the results to')
    parser.add_argument('--url', help='url of the csv file')


    args = parser.parse_args()
    main(args)


