import pandas as pd
import sys, os
# sys.path.append(os.path.abspath(".."))

from sqlalchemy import create_engine, MetaData


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

    return df_track.to_csv("vehicles.csv"), df_trajectory.to_csv("trajectories.csv")





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




    