from pandas_profiling import ProfileReport
import os
import pandas as pd
import pyspark
from pyspark.sql import SparkSession

# import of functions from custom modules
from get_prices import *
from get_fundamentals import *
from get_peers import *
from get_company_info import *

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
    
def pipeline_staging():
    """staging pipeline which downloads all files from API sources to disk

    Returns:
        [list]: list of Ticker symbol in string format which exist in all sources.
    """    
    # create company info_df
    symbol_list = get_stock_symbol_list()
    # company info is also available via the IEX API
    # in this project a dataset from kaggle is used instead, therefore load from disk
    company_info_df = load_company_info_from_disk(symbol_list=symbol_list)
    symbol_list = company_info_df['ticker'].unique().tolist()
    
    # download peer data to disk
    #peer_df = download_peer_data(symbol_list)
    
    # extract fundamental data from Simfin API to fundamental df
    fundamental_df = combine_fundamentals(symbol_list)
    # reduce symbol_list to those where fundamental data  is available
    symbol_list = fundamental_df['Ticker'].unique().tolist()
    
    # Download historic stock prices for symbols
    download_ticker_prices(symbol_list)
    print('Staging pipeline run complete.')
    return symbol_list

def pipeline_processing(spark, period_dict, fp=10, exp_rr=0.15, symbol_list=['_all']):
    """Processing pipeline which creates result tables from data on disk

    Args:
        spark ([SparkSession]): spark session object
        period_dict ([dict]): dictionary with keys start_date and end_date which stores these in int format.
        fp (int, optional): [future period]. Defaults to 10.
        exp_rr (float, optional): [annual expected rate of return]. Defaults to 0.15.
        symbol_list (list, optional): [List of ticker symbols]. Defaults to ['_all'].

    Returns:
        company_info_df, fundamental_df, growth_df, screener_df: calculated result tables as Pandas Dataframe
    """    
    # create company info_df
    company_info_df = load_company_info_from_disk(symbol_list=symbol_list)
    symbol_list = company_info_df['ticker'].unique().tolist()
    peer_df = get_peer_data_from_disk(symbol_list)
    company_info_df = company_info_df.merge(peer_df, 
                                        on = 'ticker',
                                        how='left',
                                        validate='1:1')
    # combine fundamentals and calculate top5 kpis
    fundamental_df = combine_fundamentals(symbol_list)
    # filter on relevant time period
    fundamental_df = filter_df(fundamental_df, period_dict)
    fundamental_df = calculate_top5_kpi(fundamental_df)
    # reduce symbol_list to those where fundamental data is available
    symbol_list = fundamental_df['Ticker'].unique().tolist()
    # load ticker prices for symbols
    price_df = load_ticker_prices(spark, symbol_list)
    # restrict symbol_list to those with available price data
    symbol_list = price_df.select('Ticker').distinct().toPandas()['Ticker'].tolist()
    fundamental_df = filter_symbols(fundamental_df, symbol_list)
    # calculate annual price from historic price data
    ann_price_df = calculate_annual_price(spark, price_df, period_dict)
    # calculate annual price per earnings
    fundamental_df = calculate_annual_pe(ann_price_df, fundamental_df)
    # calculate growth kpi df
    growth_df = calculate_growth_rates(fundamental_df, agg_func='mean')
    growth_df = calculate_sticker_price(growth_df, fp=fp, exp_rr=exp_rr)
    screener_df = find_stocks_below_mos(spark, price_df, growth_df)
    # store resulting data sets on disk
    for df, df_name in zip([company_info_df, fundamental_df, growth_df, screener_df],
                  ['company_info', 'fundamental', 'growth', 'screener']):
        df.to_parquet('..//data//5_results//' + df_name + '.parquet.gzip', compression='gzip')
    return price_df, company_info_df, fundamental_df, growth_df, screener_df

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