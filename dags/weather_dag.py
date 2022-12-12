
# imports

import pandas as pd
import json
import os


from datetime import datetime , timedelta


from airflow import DAG 
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator



# função clean data

def clean_data():
    filename = str( datetime.now().date() ) + '.json'
    tot_name = os.path.join( os.path.dirname(__file__),'src/data', filename )
    
    with open( tot_name, 'r' ) as inputfile:
        doc = json.load( inputfile )

    # extract data
    df_raw = {
        'name'        : doc['location']['name'],
        'region'      : doc['location']['region'],
        'country'     : doc['location']['country'],
        'lat'         : doc['location']['lat'],
        'lon'         : doc['location']['lon'],
        'temp_c'      : doc['current']['temp_c'],
        'wind_mph'    : doc['current']['wind_mph'],
        'pressure_mb' : doc['current']['pressure_mb'],
        'humidity'    : doc['current']['humidity'],
        'cloud'       : doc['current']['cloud'],
        'feelslike_c' : doc['current']['feelslike_c']
    }

    # convert data to csv
    df = pd.DataFrame( df_raw, index=[0] )

    end_path=os.path.join( os.path.dirname(__file__), 'src/data', 'weather.csv' )
    df.to_csv( end_path )


# Airflow DAG

default_args = {
    'owner' : 'Pedrosa',
    'depends_on_past': False,
    'email':['felipepedrosacpv@gmail.com'],
    'email_in_failure' : False,
    'email_on_retry' : False,
    'retries' : 5,
    'retry_delay' : timedelta(minutes = 1)
}

dag = DAG(
    dag_id = 'weather_dag',
    default_args = default_args,
    start_date = datetime(2022,12,10),
    schedule = timedelta(minutes=60)
)

# first task - Get data from API

task1 = BashOperator(
    task_id = 'get_weather',
    bash_command = 'python  /home/felipepedrosa/projetos/airflow/dags/src/get_weather.py',
    dag = dag
)

# second task - Data transformation

task2 = PythonOperator(
    task_id= 'clean_data',
   # provide_context = True,
    python_callable = clean_data,
    dag = dag

)

# tasks dependency
task1 >> task2 
