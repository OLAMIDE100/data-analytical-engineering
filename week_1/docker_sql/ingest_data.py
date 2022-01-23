import pandas as pd
from time import time
import argparse
from sqlalchemy import  create_engine
import os

def main(params):
    # stating the argument
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    table_1 = params.table_1
    table_2 = params.table_2
    url_1 = params.url_1
    url_2 = params.url_2

    csv_name_1 = 'output_1.csv'
    csv_name_2 = 'output_2.csv'
    
    #Downloading the various tables for our database
    os.system(f"wget {url_1} -O {csv_name_1}")
    os.system(f"wget {url_2} -O {csv_name_2}")
    
    #Connecting the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect
    
    #Reading and populating the database
    df_2 = pd.read_csv(csv_name_2)
    df_2.head(0).to_sql(name=table_2, con=engine, if_exists='replace')
    df_2.to_sql(name=table_2, con=engine, if_exists='append')
    

    df_iter = pd.read_csv(csv_name_1, iterator=True, chunksize=100000)
    df = next(df_iter)
    df.head(n=0).to_sql(name=table_1, con=engine, if_exists='replace')
    df_iter = pd.read_csv(csv_name_1, iterator=True, chunksize=100000)


    while True: 
        t_start = time()

        df = next(df_iter)

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_1, con=engine, if_exists='append')

        t_end = time()

        print('inserted a new chunk into the table in {} seconds'.format(t_end - t_start))

   


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='Ingest CSV data into postgres')


    parser.add_argument('--user',help="username for postgres")
    parser.add_argument('--password',help="password for postgres")
    parser.add_argument('--host',help="host for postgres")
    parser.add_argument('--port',help="port for postgres")
    parser.add_argument('--db',help="databasename ")
    parser.add_argument('--table_1',help="tablename")
    parser.add_argument('--url_1',help="url for the downloaded dataset")
    parser.add_argument('--table_2',help="tablename")
    parser.add_argument('--url_2',help="url for the downloaded dataset")

    args = parser.parse_args()
    

    main(args)





