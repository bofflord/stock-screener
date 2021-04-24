from pandas_profiling import ProfileReport
import os
import pandas as pd

def create_pandas_profiling_report(df, df_name):
    df_profile = ProfileReport(df, title=(df_name + ' Report'), minimal = True)
    df_profile.to_file((os.getcwd() + '/../data/4_data_analysis/' + df_name + '_report.html'))
    print(f'\nPandas profiling report of file {df_name} created\n\n')
    
def list_all_directory_files(directory):
    """[summary]

    Args:
        directory ([type]): [description]

    Returns:
        files (list): list of strings with filesnames
    """    
    root, dirs, files = next(os.walk(directory))
    path_list = []
    for file in files:
        path_list.append(directory + '/' + file)
    return path_list
    