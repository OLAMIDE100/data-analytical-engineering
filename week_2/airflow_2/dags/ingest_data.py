import pandas as pd
from time import time
from sqlalchemy import  create_engine
import os

def load_data(user,password,host,port,db,table_1,csv_name_1):
   
    #Connecting the database
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    print(table_1,csv_name_1)
    engine.connect()
    
    print("connection established sucessfully and starting to ingest data")
    #Reading and populating the databas
    
    t_start = time()

    df_iter = pd.read_csv(csv_name_1, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)


    df.head(n=0).to_sql(name=table_1, con=engine, if_exists='replace')
    df.to_sql(name=table_1, con=engine, if_exists='append')

    t_end = time()
    print('inserted first chuck into the table in {} seconds'.format(t_end - t_start))

    while True: 
        
        t_start = time()

        try:
            df = next(df_iter)
        except StopIteration:
            print("completed")
            break

        

        df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
        df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
        
        df.to_sql(name=table_1, con=engine, if_exists='append')

        t_end = time()

        print('inserted a new chunk into the table in {} seconds'.format(t_end - t_start))

   






