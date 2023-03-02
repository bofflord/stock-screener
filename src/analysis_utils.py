from pandas_profiling import ProfileReport
import os
import pandas as pd

def create_pandas_profiling_report(df, df_name):
    """Creates pandas profiling report an Dataframe and saves it in html format to disk.

    Args:
        df ([Pandas Dataframe]): Dataframe which should be analyzed.
        df_name ([str]): Name of dataframe which is used in stored filename
    """    
    df_profile = ProfileReport(df, title=(df_name + ' Report'), minimal = True)
    df_profile.to_file((os.getcwd() + '/../data/4_data_analysis/' + df_name + '_report.html'))
    print(f'\nPandas profiling report of file {df_name} created\n\n')
    
def list_all_directory_files(directory):
    """Function that returns list of all file paths in directory

    Args:
        directory ([str]): directory path

    Returns:
        files (list): list of strings with filenames
    """    
    root, dirs, files = next(os.walk(directory))
    path_list = []
    for file in files:
        path_list.append(directory + '/' + file)
    return path_list
    
def read_result_tables():
    """Read calculted result tables in parquet format from disk

    Returns:
        company_info_df, fundamental_df, growth_df, screener_df: calculated result tables as Pandas Dataframe
    """    
    company_info_df = pd.read_parquet('..//data//5_results//' + 'company_info' + '.parquet.gzip')
    fundamental_df = pd.read_parquet('..//data//5_results//' + 'fundamental' + '.parquet.gzip')
    growth_df = pd.read_parquet('..//data//5_results//' + 'growth' + '.parquet.gzip')
    screener_df = pd.read_parquet('..//data//5_results//' + 'screener' + '.parquet.gzip')
    return company_info_df, fundamental_df, growth_df, screener_df