import pandas as pd
from sqlalchemy import create_engine
from time import time


def ingest_callable(user, password, host, port, db, table_name, csv_name):

    print(table_name, csv_name)
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()
    print('Connection stablished successfully, started to insert data')
    t_start = time()
   


    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)



    #First chunk of size 100.000
    df = next(df_iter)


    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'])
    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'])




    df.head(0).to_sql(con=engine, name=table_name, if_exists='replace')



    #Append all data to the database

    df.to_sql(con=engine, name=table_name, if_exists='append')

    t_end = time()
    print('Inserted first chunk in %.3f seconds' % (t_end - t_start))


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

