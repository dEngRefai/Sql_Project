import datetime as dt
from datetime import timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator 
import pandas as pd
import psycopg2
from sqlalchemy import create_engine



default_args={
    'owner': 'refai',
    'start_date': dt.datetime(2021, 5, 23),
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}


    
        
def automatedComparison():
    # define engine
    host="postgres_storage"
    database="testDB"
    user="me"
    password="1234"
    port='5432'
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')
    
    #do the comparison
    df=pd.read_sql_query('''
    SELECT * 
    FROM new_informants 
    inner join trusted_informants on trusted_informants.name =  new_informants.name AND trusted_informants.age =  new_informants.age 
    AND trusted_informants.city =  new_informants.city
    ''', engine)
    df.to_sql('processed_informants', engine, if_exists='replace', index=False)
    

 

with DAG('Automated_informants_veracity_check',
        default_args=default_args,
        schedule_interval=timedelta(minutes=1),
        catchup=False
        ) as dag:


    automated_comparison = PythonOperator(task_id="comparison",
                                    python_callable=automatedComparison)
                                    
    
    
    

automated_comparison 
