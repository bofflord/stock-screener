import os
from get_prices import *

script_dir = os.path.dirname(os.path.realpath(__file__))
price_data_dir = os.path.join(os.path.dirname(script_dir), 'data', '3_prices')
filenames=next(os.walk(price_data_dir))[2]
filenames=[f.split('.')[0] for f in filenames]
filenames=sorted(filenames)

last_element_idx = filenames.index('MGPI')

symbol_list = filenames[last_element_idx:]

download_ticker_prices(symbol_list, period='10y', download_path=price_data_dir)