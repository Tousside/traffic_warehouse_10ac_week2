import pandas as pd
import sys, os
# sys.path.append(os.path.abspath(".."))



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
    # save data
    df_track.to_csv("data/vehicles.csv")
    df_trajectory.to_csv("data/trajectories.csv")

raw_data_path="/home/tousside/Documents/10Academy/week2/traffic_warehouse/data/20181029_d1_0800_0830.csv"

load_data(raw_data_path)