#%%
# Imports
import pandas as pd
import numpy as np
import simfin as sf
import os, sys
import shutil
import plotly.express as px
import configparser
# import yfinance as yf
from typing import Union

from get_fundamentals import calculate_annual_pe, calculate_growth_rates, calculate_sticker_price, calculate_top5_kpi

# for testing only
from get_fundamentals import get_current_yr_kpi, calculate_growth_summary

#%%
# CONFIG
OUT_OFF_BOUNDS_PERCENTAGE_LIMIT = 3500 # 3500% percentage growth


#%%
# change working directory to script directory
# if sys.argv[0] != '/home/bofflord/01_projects/10_py37_dataeng_venv/.venv/lib/python3.7/site-packages/ipykernel_launcher.py':
#     os.chdir(os.path.dirname(sys.argv[0]))
#     print(f'working directory: {os.getcwd()}')

# read api keys from config
config = configparser.ConfigParser()
config.read('../data/api_keys.cfg')
print(f'Config sections: {config.sections()}')
# set api_key
sf.set_api_key(api_key=config['simfin']['api_key'])
# set data directory
sf.set_data_dir()


#%%
# Extract market, industry information data
markets_df = sf.load(dataset = 'markets')
industry_df = sf.load(dataset = 'industries')

# Extract company information
companies_df = pd.DataFrame()
market_list = markets_df.MarketId.unique()
market_list = [market for market in market_list if market not in ['it', 'ca']]
for market in market_list:
    market_company_df = sf.load(dataset = 'companies', market = market)
    market_company_df['market'] = market
    companies_df = pd.concat([companies_df, market_company_df], ignore_index=True)

companies_df = companies_df.merge(
    industry_df,
    how='left',
    on = 'IndustryId'
)
companies_df.head(3)

#%%
# Extract fundamental data
variant='annual'
cashflow_df = pd.DataFrame()
income_sm_df = pd.DataFrame()
balance_st_df = pd.DataFrame()

fund_df_dict = {
    'cashflow' : cashflow_df,
    'income' : income_sm_df,
    'balance' : balance_st_df

}

for market in market_list:
    for fund_report in fund_df_dict.keys():
        df = sf.load(dataset=fund_report, variant='annual', market = market)
        df['market'] = market
        fund_df_dict[fund_report] = pd.concat([fund_df_dict[fund_report], df], ignore_index=True)

cashflow_df = fund_df_dict['cashflow']
income_sm_df = fund_df_dict['income']
balance_st_df = fund_df_dict['balance']


#%%
def filter_symbols(df, symbol_list):
    df = df[df['Ticker'].isin(symbol_list)]
    return df

# filter symbol_list to those existing in all financial statements
symbol_list = list(
            set(cashflow_df['Ticker'].unique().tolist()) &\
            set(income_sm_df['Ticker'].unique().tolist()) &\
            set(balance_st_df['Ticker'].unique().tolist())
)
print(f'Symbols with available fundamental data: {len(symbol_list)}')
cashflow_df = filter_symbols(cashflow_df, symbol_list)
income_sm_df = filter_symbols(income_sm_df, symbol_list)
balance_st_df = filter_symbols(balance_st_df, symbol_list)
# merge filtered dataframes
merge_cols = ['Ticker',
                'SimFinId',
                'Currency',
                'Fiscal Year']   
fundamental_df = pd.merge(income_sm_df, 
                            cashflow_df, 
                            on = merge_cols,
                            suffixes = ('_is', '_cf'),
                            validate='1:1')
fundamental_df = pd.merge(fundamental_df, 
                            balance_st_df, 
                            on = merge_cols,
                            validate='1:1')
fundamental_df.head(3)

#%%
# extract share price data
price_df = pd.DataFrame()
for market in market_list:
    market_prices_df = sf.load(dataset = 'shareprices', variant='daily', market = market)
    market_prices_df['market'] = market
    price_df = pd.concat([price_df, market_prices_df], ignore_index=True)


def limit_two_df_to_same_elements_in_one_column(df1: pd.DataFrame, df2: pd.DataFrame, column: str) -> Union[pd.DataFrame, pd.DataFrame]:
    '''Return transformed copy of two dataframes that share the same elements in a column'''
    df1 = df1[df1[column].isin(df2[column].unique())].reset_index(drop=True)
    df2 = df2[df2[column].isin(df1[column].unique())].reset_index(drop=True)
    return df1, df2

fundamental_df, price_df = limit_two_df_to_same_elements_in_one_column(
    fundamental_df,
    price_df, 
    'Ticker'
)
#%%
# calculate annual prices from historic price data 
def calculate_annual_price(df: pd.DataFrame) -> pd.DataFrame:
    df['month'] = pd.to_datetime(df.Date).dt.month
    df['year'] = pd.to_datetime(df.Date).dt.year
    df = df[df.month==12].reset_index(drop=True)
    df = df.groupby(['year', 'Ticker'])['Low'].min().reset_index()
    df = df.rename(columns={'Low' : 'mean_low_price'})
    return df

ann_price_df = calculate_annual_price(price_df)

def print_df_shape(df: pd.DataFrame) -> None:
    print(f'Shape of df: {df.shape}')
    ticker_count = df.Ticker.nunique()
    print(f'Count of unique tickers in df : {ticker_count}')

print('before calculate_top5_kpi')
print_df_shape(fundamental_df)

# calculate roic, eps, bvps, fcf
fundamental_df = calculate_top5_kpi(fundamental_df)

print('after calculate_top5_kpi')
print_df_shape(fundamental_df)

# calculate annual price per earnings
fundamental_df = calculate_annual_pe(ann_price_df, fundamental_df)

print('after calculate_annual_pe')
print_df_shape(fundamental_df)

# calculate growth kpi df
growth_df = calculate_growth_rates(fundamental_df, agg_func='mean')

print('after calculate_growth_rates')
print_df_shape(growth_df)

# calulate sticker price
growth_df = calculate_sticker_price(growth_df, fp=10, exp_rr=0.15)

print('after calculate_sticker_price')
print_df_shape(growth_df)

# extract latest price 
curr_price_df = price_df[price_df.groupby('Ticker')['Date'].transform('max') == price_df.Date].reset_index(drop=True)
curr_price_df['last_low_price'] = curr_price_df.Low
growth_df = growth_df.merge(
    curr_price_df[['Ticker', 'last_low_price']],
    how = 'left',
    on = 'Ticker',
    validate = '1:1'
)
growth_df['mos_reached'] = growth_df.mos >= growth_df.last_low_price

growth_df['diff_price_mos'] = growth_df.mos - growth_df.last_low_price

growth_df['diff_price_mos_percentage'] = 100*((growth_df.mos - growth_df.last_low_price)/ growth_df.last_low_price).round(2)

growth_df['out_off_bounds_value'] = growth_df['diff_price_mos_percentage'] > OUT_OFF_BOUNDS_PERCENTAGE_LIMIT

# add company and industry info
growth_df = growth_df.merge(
    companies_df,
    how ='left',
    on = 'Ticker',
    validate='1:1'
)
growth_df.head(3)

#%%
# save outputs to parquet format 
for df, df_name in zip([fundamental_df, growth_df], ['fundamental', 'growth']):
    current_date = str(pd.to_datetime('today').date())
    df.to_parquet(f'../data/5_results/{current_date}_{df_name}.parquet')
    df.to_excel(f'../data/5_results/{current_date}_{df_name}.xlsx', index=False)