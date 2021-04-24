import pandas as pd
import yfinance as yf
import os, contextlib

def download_ticker_prices(symbol_list, period='max'):
    """download prices from yfinance for all symbols in a list of the defined period.

    Args:
        symbol_list (str): list of stock symbols (e.g. AAPL for Apple)
        period (str): valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max. Default: max.
    """    
    is_valid = [False] * len(symbol_list)
    # force silencing of verbose API
    with open(os.devnull, 'w') as devnull:
        with contextlib.redirect_stdout(devnull):
            for i in range(0, len(symbol_list)):
                s = symbol_list[i]
                data = yf.download(s, period=period)
                if len(data.index) == 0:
                    continue
            
                is_valid[i] = True
                # insert Ticker column
                data.insert(0, 'Ticker', s)
                data.to_csv('../data/3_prices/{}.csv'.format(s))
    print('Total number of valid symbols downloaded = {}'.format(sum(is_valid)))
    
