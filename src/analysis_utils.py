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
    
def pipeline_staging():
    # create company info_df
    symbol_list = get_stock_symbol_list()
    # company info is also available via the IEX API
    # in this project a dataset from kaggle is used instead, therefore load from disk
    company_info_df = load_company_info_from_disk(symbol_list=symbol_list)
    symbol_list = company_info_df['ticker'].unique().tolist()
    
    # download peer data from disk
    #peer_df = download_peer_data(symbol_list)
    
    # TODO: extract fundamental data from Simfin API to staging tables
    
    # Download historic stock prices for symbols
    download_ticker_prices(symbol_list)
    

def pipeline_processing(spark, period_dict, fp=10, exp_rr=0.15, symbol_list=['_all']):
    # create company info_df
    company_info_df = load_company_info_from_disk(symbol_list=symbol_list)
    symbol_list = company_info_df['ticker'].unique().tolist()
    peer_df = get_peer_data_from_disk(symbol_list)
    company_info_df = company_info_df.merge(peer_df, 
                                        on = 'ticker',
                                        how='left',
                                        validate='1:1')
    # combine fundamentals and calculate top5 kpis
    fundamental_df = combine_fundamentals(symbol_list, period_dict)
    fundamental_df = calculate_top5_kpi(fundamental_df)
    # load ticker prices for symbols
    price_df = load_ticker_prices(spark, symbol_list)
    # calculate annual price from historic price data
    ann_price_df = calculate_annual_price(spark, price_df, period_dict)
    # calculate annual price per earnings
    fundamental_df = calculate_annual_pe(ann_price_df, fundamental_df)
    # calculate growth kpi df
    growth_df = calculate_growth_rates(fundamental_df, agg_func='mean')
    growth_df = calculate_sticker_price(growth_df, fp=fp, exp_rr=exp_rr)
    screener_df = find_stocks_below_mos(spark, price_df, growth_df)
    return company_info_df, fundamental_df, growth_df, screener_df

# def OLD_pipeline(spark, period_dict, fp=10, exp_rr=0.15):
#     # create company info_df
#     symbol_list = get_stock_symbol_list()
#     company_info_df = load_company_info_from_disk(symbol_list)
#     symbol_list = company_info_df['ticker'].unique().tolist()
#     peer_df = get_peer_data_from_disk(symbol_list)
#     company_info_df = company_info_df.merge(peer_df, 
#                                         on = 'ticker',
#                                         how='left',
#                                         validate='1:1')
#     # combine fundamentals and calculate top5 kpis
#     fundamental_df = combine_fundamentals(symbol_list, period_dict)
#     fundamental_df = calculate_top5_kpi(fundamental_df)
#     # Download historic stock prices for symbols
#     download_ticker_prices(symbol_list)
#     # load ticker prices for symbols
#     price_df = load_ticker_prices(spark, symbol_list)
#     # calculate annual price from historic price data
#     ann_price_df = calculate_annual_price(spark, price_df, period_dict)
#     # calculate annual price per earnings
#     fundamental_df = calculate_annual_pe(ann_price_df, fundamental_df)
#     # calculate growth kpi df
#     growth_df = calculate_growth_rates(fundamental_df, agg_func='mean')
#     growth_df = calculate_sticker_price(growth_df, fp=fp, exp_rr=exp_rr)
#     screener_df = find_stocks_below_mos(spark, price_df, growth_df)
#     return company_info_df, fundamental_df, growth_df, screener_df