import pandas as pd
from time import time
import argparse
from sqlalchemy import  create_engine
import os

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table = params.table
    url = params.url

    csv_name = 'output.csv'

    os.system(f"wget {url} -O {csv_name}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect

    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.head(n=0).to_sql(name=table, con=engine, if_exists='replace')
    df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)


    while True: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table, con=engine, if_exists='append')

        t_end = time()

        print('inserted a new chunk into the table in {} seconds'.format(t_end - t_start))


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data into postgres')


    parser.add_argument('--user',help="username for postgres")
    parser.add_argument('--password',help="password for postgres")
    parser.add_argument('--host',help="host for postgres")
    parser.add_argument('--port',help="port for postgres")
    parser.add_argument('--db',help="databasename ")
    parser.add_argument('--table',help="tablename")
    parser.add_argument('--url',help="url for the downloaded dataset")

    args = parser.parse_args()
    

    main(args)





