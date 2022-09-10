# %%
# Imports
import pandas as pd
import numpy as np
import simfin as sf
import os
import shutil
import plotly.express as px
import configparser
import yfinance as yf
from typing import Union

# %%
# read api keys from config
config = configparser.ConfigParser()
config.read('data/api_keys.cfg')
print(config.sections())
# set api_key
sf.set_api_key(api_key=config['simfin']['api_key'])
# set data directory
sf.set_data_dir()

# %%
# Extract market, industry information data
markets_df = sf.load(dataset = 'markets')
industry_df = sf.load(dataset = 'industries')

# Extract company information
companies_df = pd.DataFrame()
for market in markets_df.MarketId.unique():
    market_company_df = sf.load(dataset = 'companies', market = market)
    market_company_df['market'] = market
    companies_df = companies_df.append(market_company_df, ignore_index=True)

companies_df = companies_df.merge(
    industry_df,
    how='left',
    on = 'IndustryId'
)
companies_df.head(3)

# %%
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

for market in markets_df.MarketId.unique():
    for fund_report in fund_df_dict.keys():
        df = sf.load(dataset=fund_report, variant='annual', market = market)
        df['market'] = market
        fund_df_dict[fund_report] = fund_df_dict[fund_report].append(df, ignore_index=True)

cashflow_df = fund_df_dict['cashflow']
income_sm_df = fund_df_dict['income']
balance_st_df = fund_df_dict['balance']

# %%
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
fundamentals_df = pd.merge(income_sm_df, 
                            cashflow_df, 
                            on = merge_cols,
                            suffixes = ('_is', '_cf'),
                            validate='1:1')
fundamentals_df = pd.merge(fundamentals_df, 
                            balance_st_df, 
                            on = merge_cols,
                            validate='1:1')
fundamentals_df.head(3)

# %%
# extract share price data
price_df = pd.DataFrame()
for market in markets_df.MarketId.unique():
    market_prices_df = sf.load(dataset = 'shareprices', variant='daily', market = market)
    market_prices_df['market'] = market
    price_df = price_df.append(market_prices_df, ignore_index=True)

price_df.head(3)



# %%
def limit_two_df_to_same_elements_in_one_column(df1: pd.DataFrame, df2: pd.DataFrame, column: str) -> Union[pd.DataFrame, pd.DataFrame]:
    """
    Return transformed copy of two dataframes that share the same elements in a column
    """
    df1 = df1[df1[column].isin(df2[column].unique())].reset_index(drop=True)
    df2 = df2[df2[column].isin(df1[column].unique())].reset_index(drop=True)
    return df1, df2

fundamentals_df, price_df = limit_two_df_to_same_elements_in_one_column(
    fundamentals_df,
    price_df, 
    'Ticker'
)

# %%
# calculate annual prices from historic price data 
def calculate_annual_price(df: pd.DataFrame) -> pd.DataFrame:
    '''
    
    '''
    df['month'] = pd.to_datetime(df.Date).dt.month
    df['year'] = pd.to_datetime(df.Date).dt.year
    df = df[df.month==12].reset_index(drop=True)
    df = df.groupby(['year', 'Ticker'])['Low'].min().reset_index()
    df = df.rename(columns={'Low' : 'mean_low_price'})
    return df

calculate_annual_price(price_df).head()

# %%


