from fcntl import DN_MODIFY
import pandas as pd
import numpy as np
import simfin as sf
import os

def init_simfin_api(key='free'):
    """[Initialise Simfin API]

    Args:
        key (str, optional): Simfin API key. Defaults to 'free'.
    """    
    # Set your API-key for downloading data. This key gets the free data.
    sf.set_api_key(key)
    # Set the local directory where data-files are stored.
    # The directory will be created if it does not already exist.
    # SimFin data-directory.
    sf.set_data_dir(os.getcwd() + '/../data/2_fundamentals/simfin_data/')

def filter_df(df, period):
    df = df[(df['Fiscal Year']>=period['start_date'])&\
                    (df['Fiscal Year']<=period['end_date'])].reset_index(drop=True)
    return df

def filter_symbols(df, symbol_list):
    df = df[df['Ticker'].isin(symbol_list)]
    return df

def combine_fundamentals(symbol_list,
                         key='free', 
                         market='us', 
                         variant='annual'):
    """Merge different fundamental data into one dataframe

    Args:
        symbol_list (list): [description]
        period (dict): [description]
        key (str, optional): Simfin api key. Defaults to 'free'.
        market (str, optional): market for which data should be retrieved. Defaults to 'us'.
        variant (str, optional): time variant for data retrieval. Defaults to 'annual'.

    Returns:
        fundamentals_df (Pandas DataFrame): [description]
    """ 
    # initialize simfin API
    init_simfin_api(key)
    
    # download cashflow data from the SimFin server and load into a Pandas DataFrame.
    cashflow_df = sf.load_cashflow(variant=variant, market=market)
    cashflow_df = cashflow_df.reset_index()
    # Download the data from the SimFin server and load into a Pandas DataFrame.
    income_sm_df = sf.load_income(variant=variant, market=market)
    income_sm_df = income_sm_df.reset_index()
    # Download the data from the SimFin server and load into a Pandas DataFrame.
    balance_st_df = sf.load_balance(variant=variant, market=market)
    balance_st_df = balance_st_df.reset_index()
    # filter symbol_list to those existing in all financial statements
    symbol_list = list(set(symbol_list) &\
                set(cashflow_df['Ticker'].unique().tolist()) &\
                set(income_sm_df['Ticker'].unique().tolist()) &\
                set(balance_st_df['Ticker'].unique().tolist()))
    print('Symbols with available fundamental data: {}'.format(len(symbol_list)))
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
    print('Combined all fundamental data from financial statements to one Dataframe.')
    return fundamentals_df 


def calculate_roic(df):
    """calculate Return on Investment based on financial statements

    Args:
        df (Pandas DataFrame): dataframe with fundamentals from financial statements

    Returns:
        df (Pandas DataFrame): same df with additional column 'roic'
    """    
    # ROIC = (ni - di) / (de + eq)
    # net income: n_i -> roic_df['Net Income']
    # dividend: di -> cashflow_df['Dividends Paid']
    # debt: de -> 
    # equity: eq -> 
    # de + eq = balance_st_df['Total Liabilities & Equity']
    # Note: 'Dividends Paid' is negative amount -> add to net income to get difference
    
    # cleansing: convert df['Dividends Paid'] to Null since many data is missing
    df['Dividends Paid_clean'] = df['Dividends Paid'].fillna(0)
    df['roic'] = (df['Net Income'] + df['Dividends Paid_clean']) / df['Total Liabilities & Equity'].replace(0, np.NaN)
    print('Calculated roic and added it to Dataframe')
    return df

def calculate_eps(df):
    """Calculate Earnings per Share

    Args:
        df ([Pandas Dataframe]): [fundamental dataset]

    Returns:
        [Pandas Dataframe]: original dataframe with additional column
    """    
    df['eps'] = df['Net Income'] / df['Shares (Basic)']
    print('Calculated eps and added it to Dataframe')
    return df

def calculate_bvps(df):
    """Calculate Book Value per Share

    Args:
        df ([Pandas Dataframe]): [fundamental dataset]

    Returns:
        [Pandas Dataframe]: original dataframe with additional column
    """    
    df['bvps'] = df['Total Equity'] / df['Shares (Basic)']
    print('Calculated bvps and added it to Dataframe')
    return df

def calculate_fcf(df):
    """Calculate Free Cash Flow

    Args:
        df ([Pandas Dataframe]): [fundamental dataset]

    Returns:
        [Pandas Dataframe]: original dataframe with additional column
    """    
    df['fcf'] = df['Net Cash from Operating Activities']\
                - df['Net Cash from Investing Activities']
    print('Calculated fcf and added it to Dataframe')
    return df

def calculate_top5_kpi(df):
    """calculate top5 kpis from fundamental data in financial statements

    Args:
        df (Pandas Dataframe): combined fundamental data from financial statements

    Returns:
        df (Pandas Dataframe): original Dataframe appended by top 5 KPIs
    """    
    df = calculate_roic(df)
    df = calculate_eps(df)
    df = calculate_bvps(df)
    df = calculate_fcf(df)
    print('top5 KPIs added to fundamental data')
    return df
    
def calculate_growth_summary(df, gr_kpi_list, agg_func='mean'):
    """Sub-function for calculate_growth_rates. Calculate growth KPI summary of latest 5 and 10 years

    Args:
        df ([Pandas Dataframe]): [Fundamental data set]
        gr_kpi_list ([list]): [list of Growth KPIs]
        agg_func (str, optional): [Aggregation function for summary]. Defaults to 'mean'.

    Returns:
        [Pandas Dataframe]: Calculated growth KPI data
    """    
    # calculate 10 year and 5 year average
    # get current year
    # TODO: remove if no longer required
    # curr_year = df['Fiscal Year'].max()
    df_list = []
    for period in [5, 10]:
        # TODO: update logic to adapt to each ticker symbol
        # start_year = curr_year - period
        # df_filtered = df[df['Fiscal Year']>start_year]
        df_filtered = df[df.groupby('Ticker')['Fiscal Year'].transform('max') < (df['Fiscal Year']+period)].reset_index(drop=True)
        kpi_sum_list = [kpi + '_' + str(period) + 'yr' for kpi in gr_kpi_list]
        rename_dict = dict(zip(gr_kpi_list, kpi_sum_list))
        rename_dict['Fiscal Year'] = 'yrs_in_' + str(period) + 'yr'
        agg_dict = {gr_kpi:agg_func for gr_kpi in gr_kpi_list}
        agg_dict['Fiscal Year'] ='count'
        df_filtered = df_filtered.groupby(['Ticker'])\
                                        .agg(agg_dict)\
                                        .reset_index()\
                                        .round(2)
        df_filtered = df_filtered.rename(columns=rename_dict)
        # calculate rule1 growth rate
        kpi_list = ['bvps_gr_', 'eps_gr_']
        kpi_list = [(kpi + str(period) + 'yr') for kpi in kpi_list]
        df_filtered[('rule1_gr_' + str(period)+ 'yr')] = df_filtered[kpi_list].min(axis=1)
        df_filtered[('rule1_gr_' + str(period)+ 'yr')] = df_filtered[('rule1_gr_' + str(period)+ 'yr')].apply(lambda value: value if value > 0 else np.nan)
        df_filtered['pe_default_'+ str(period)+ 'yr'] = 2*100*df_filtered[('rule1_gr_' + str(period)+ 'yr')]
        df_list.append(df_filtered)
    df_sum = pd.merge(df_list[0], df_list[1], how='outer', on='Ticker')
    print('Calculated 5 and 10 year growth rate')
    return df_sum

def get_current_yr_kpi(df):
    """Sub-function for calculate_growth_rates. Add KPIs of current year to Growth dataset

    Args:
        df ([Pandas Dataframe]): [Fundamental dataset]

    Returns:
        [Pandas Dataframe]: [filtered KPIs of current year]
    """    
    col_list = ['Ticker', 'revenue_gr', 'eps']
    # curr_yr = df['Fiscal Year'].max()
    # df_curr_yr = df[df['Fiscal Year']==curr_yr][col_list].reset_index(drop=True)
    df = df[df.groupby('Ticker')['Fiscal Year'].transform('max') == df['Fiscal Year']][col_list].reset_index(drop=True)
    rename_dict = {col:(col + '_curr') for col in ['revenue_gr', 'eps']}
    df = df.rename(columns=rename_dict)
    return df

def calculate_growth_rates(df, agg_func='mean'):
    """[Function that creates starting point of Growth dataset]

    Args:
        df ([Pandas Dataframe]): [fundamental dataset]
        agg_func (str, optional): [aggregation function for sub-function calculate_growth_summary]. Defaults to 'mean'.

    Returns:
        [Pandas Dataframe]: [starting point of Growth dataset]
    """    
    group_col_list = ['Ticker',
                    'Fiscal Year']
    kpi_col_list = ['roic',
                    'revenue',
                    'eps',
                    'bvps',
                    'fcf'
                    ]
    df = df.rename(columns={'Revenue':'revenue'})
    column_list = group_col_list + kpi_col_list 
    # add price per earnings to list, but do not calculate change
    pe_df = df[group_col_list + ['pe']]
    df = df[column_list]
    # sort values to ensure that growth rate is calculated correctly
    df = df.sort_values(by=group_col_list, ascending = True)
    gr_kpi_list = [kpi + '_gr' for kpi in kpi_col_list]
    # create new columns for kpi growth rate
    df[kpi_col_list] = df[kpi_col_list].replace(0, np.NaN)
    df[gr_kpi_list] = df.groupby(['Ticker'])[kpi_col_list].pct_change()
    df = df.merge(pe_df, on = group_col_list, how= 'left', validate='1:1')
    gr_kpi_list.append('pe')
    print('Calculated KPI growth from year to year.')
    # extract current year kpi data
    df_curr_yr = get_current_yr_kpi(df)
    # calculate growth summary
    df_growth = calculate_growth_summary(df, gr_kpi_list, agg_func=agg_func)
    df = df_curr_yr.merge(df_growth, how='outer', on='Ticker',  validate='1:1')
    # data cleansing: remove inf values from numerical columns
    number_cols = df.select_dtypes(include='number').columns
    df[number_cols] = df[number_cols].replace([np.inf, -np.inf], np.nan) 
    # data cleansing: keep only entries with 5 years of data
    df = df[df['yrs_in_5yr']==5].reset_index(drop=True)
    return df

def calculate_annual_pe(ann_price_df, fundamental_df):
    """[Calculate annual price per earnings]

    Args:
        ann_price_df ([Pandas Dataframe]): [Annual price data (temporary dataset)]
        fundamental_df ([Pandas Dataframe]): [fundamental dataset]

    Returns:
        [Pandas Dataframe]: fundamental dataset with additional column
    """    
    # merge annual price to fundamental_df
    rename_dict = {'year' : 'Fiscal Year'}
    ann_price_df = ann_price_df.rename(columns=rename_dict)
    merge_cols = ['Ticker', 'Fiscal Year']
    fundamental_df = fundamental_df.merge(ann_price_df, 
                                            on = merge_cols, 
                                            how='left',
                                            validate= '1:1')  
    # calculate annual price per earnings pe = price/ eps
    fundamental_df['pe'] = fundamental_df['mean_low_price'] / fundamental_df['eps'].replace(0, np.NaN)
    return fundamental_df

def check_roic_fcf(row):
    """Sub-function for calculate_sticker_price. Sets future price values to Null if roic_gr_5yr, fcf_gr_5yr and bvps_gr_5yr are negative.

    Args:
        row ([Pandas Series]): [row with KPI data that is checked]

    Returns:
        [Pandas Series]: [row value in column 'price_future' of growth dataset]
    """    
    if (row['roic_gr_5yr'] < 0) |\
        (row['fcf_gr_5yr'] < 0) |\
        (row['bvps_gr_5yr'] < 0) :
        return np.nan
    else:
        return row['price_future']

def calculate_sticker_price(growth_df, fp=10, exp_rr=0.15):
    """Calculate sticker price from growth dataset

    Args:
        growth_df ([Pandas Dataframe]): [Growth dataset]
        fp (int, optional): [Future period used for price calculation]. Defaults to 10.
        exp_rr (float, optional): [expected rate of return]. Defaults to 0.15.

    Returns:
        [Pandas Dataframe]: [Growth dataset with additional columns for sticker price calculation]
    """    
    # calculate future price per earnings
    growth_df['pe_future'] = growth_df[['pe_5yr', 'pe_default_5yr']].apply(min, axis=1)
    # data cleansing: replace negative values with np.nan since it is not possible in reality
    growth_df['pe_future'] = growth_df['pe_future'].apply(lambda value: value if value > 0 else np.nan)
    # calculate future EPS
    growth_df['eps_future'] = growth_df['eps_curr']\
                             * growth_df['rule1_gr_5yr'].apply(lambda gr: (1+gr)**fp)
    # data cleansing: replace negative values with np.nan since it is not possible in reality
    growth_df['eps_future'] = growth_df['eps_future'].apply(lambda value: value if value > 0 else np.nan)
    # calculate future market price
    growth_df['price_future'] = growth_df['pe_future'] * growth_df['eps_future']
    # additional rule#1 quality check: replace price_future with np.nan if roic_gr-5yr or fcf_gr_5yr are negative
    growth_df['price_future'] = growth_df[['roic_gr_5yr','fcf_gr_5yr', 'bvps_gr_5yr', 'price_future']]\
                                            .apply(lambda row: check_roic_fcf(row), axis = 1)\
                                            .round(2)
    # calculate sticker price
    growth_df['sticker_price'] = growth_df['price_future']\
                                    .apply(lambda price: price / (1 + exp_rr)**fp)\
                                    .round(2)
    # calculate margin of safety
    growth_df['mos'] = (growth_df['sticker_price'] / 2).round(2)
    return growth_df