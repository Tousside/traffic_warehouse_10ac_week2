import sys, os
module_path = '../'
sys.path.append(module_path)

from airflow import DAG
from datetime import datetime, timedelta
from src.utils import  load_data
from airflow.hooks.postgres_hook import PostgresHook # type: ignore

from airflow.operators.bash import BashOperator
from airflow.operators.python  import PythonOperator, BranchPythonOperator
import json
from sqlalchemy import create_engine, MetaData

import pandas as pd
def load_data(data:str):
    """
        load the data into two data frame.
        One for track and the second for trajectory
        Parameter:
         data: sting the name of the raw csv dataset 

    """
    with open(data, 'r') as file:
        lines = file.readlines()

    lines_as_lists = [line.strip('\n').split(';') for line in lines]

    cols = lines_as_lists.pop(0)

    track_cols = cols[:4]
    
    trajectory_cols = ['track_id'] + cols[4:]

    track_info = []
    trajectory_info = []

    for row in lines_as_lists:
        track_id = row[0]

        # add the first 4 values to track_info
        track_info.append(row[:4]) 

        remaining_values = row[4:]
        # reshape the list into a matrix and add track_id
        trajectory_matrix = [ [track_id] + remaining_values[i:i+6] for i in range(0,len(remaining_values),6)]
        # add the matrix rows to trajectory_info
        trajectory_info = trajectory_info + trajectory_matrix

    df_track = pd.DataFrame(data= track_info,columns=track_cols)

    df_trajectory = pd.DataFrame(data= trajectory_info,columns=trajectory_cols)

    return df_track, df_trajectory


# # Define class to load data to PostgreSQL

class Database:
    def __init__(self, connection_params: dict):
        """
        Instantiate a database instance based on specified parameters.
        Params:
        connection_params (dict): define tha values for the following keys

        {
            "database": database_name,
            "user": username,
            "password": password,
            "host": host,
            "port": port
            
        }

        """
        try:
            self.database= connection_params["database"]
            self.database= connection_params["database"]
            self.user= connection_params["user"]
            self.password= connection_params["password"]
            self.host= connection_params["host"]
            self.port= connection_params["port"]
        except KeyError as e:
            print(f"One or more required connection parameters is missing:{e}")
        
    def _database_URI(self):
        """
        Return a database uri

        """
        uri= f"postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
        return uri
    
    def read_table_to_df(self, table_name: str)-> pd.DataFrame:
        """
            Read a table from PostgreSQL database into a pandas data frame.

            parameters:
                table_name (str): Name of the table to read.

            Returns:

            df (pd.DataFrame): DataFrame containing the table data.
        
        """

        try:
            engine=create_engine(self._database_URI())
        except:
            print("error creation the engine")
        # Use pandas to read the table data frame
        try:
            df= pd.read_sql_table(table_name, con=engine)
        except:
            print("error creation the data frame")
        
        return df
    

    def write_df_to_table(self, df: pd.DataFrame, table_name: str):
        """
                Write a pandas data frame to anew table in the PostgreSQL database
                Parameters:
                df (pd.DataFrame): DataFrame containing the data to be written 
                to the database .
                table_name (str): Name of the new table 
        """

        # create an engine (connection object) to the dataframe
        try:
            engine= create_engine(self._database_URI())
        except:
            print("error creating the engine")
        df.to_sql(table_name, engine, index=False, if_exists='replace')
        print(f"DataFrame successfully written to the '{table_name}' table")



# def load_to_postgres(**kwargs):
#     # Extract DataFrames from task instance
#     df1, df2 = kwargs['task_instance'].xcom_pull(task_ids='extract_data')
    
#     # Connect to PostgreSQL
#     pg_hook = PostgresHook(postgres_conn_id='your_postgres_conn_id')
    
#     # Write DataFrame 1 to PostgreSQL table
#     pg_hook.insert_rows(table='your_table_name1', rows=df1.values.tolist())
    
#     # Write DataFrame 2 to PostgreSQL table
#     pg_hook.insert_rows(table='your_table_name2', rows=df2.values.tolist())

# Instantiate MyClass with a parameter
# with open("db_connection_params.json", "r") as file:
#     connection_params=json.load(file)
# db = Database(connection_params)

def write_to_table(df, table_name, db):
    # Access methods of MyClass instance
    db.write_df_to_table(df, table_name)

###############DAG#########################
default_args = {
    'owner': 'koomi',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 29),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 60,
    'retry_delay': timedelta(minutes=60),
}


with DAG(
    dag_id='data_to_postgres',
    default_args=default_args,
    description='A DAG to send DataFrames to PostgreSQL',
    start_date= datetime(2024,4,29),
    schedule_interval='@daily'

) as dag:
    # Task 1: Extract Data
    extract_data_task = PythonOperator(
        task_id='extract_data',
        python_callable=load_data,
        op_args=['/data/20181029_d1_0800_0830.csv']
    )

    # Task 2: Load Data to PostgreSQL
    load_to_postgres_task = PythonOperator(
        task_id='load_to_postgres',
        python_callable=write_to_table,
    )

# Define task dependencies
extract_data_task >> load_to_postgres_task
    















  