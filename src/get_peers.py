import pandas as pd 
import pyEX as p
import configparser

def get_api_key_from_cfg(key='iexcloud_stable'):
    """Reads config file and returns api key

    Args:
        key (str): api key entry in config file. Defaults to 'iexcloud_stable'.

    Returns:
        iex_api_key_stable (str): API key from config file
    """      
    config = configparser.ConfigParser()
    config.read_file(open('../data/api_keys.cfg'))
    iex_api_key_stable = config.get(key,'api_key')
    return iex_api_key_stable

def download_peer_data(symbol_list):
    """retrieves peer data from IEX API

    Args:
        symbol_list (str): list of stock ticker symbols

    Returns:
        peer_df (Pandas Dataframe): peer data, columns: 
                                                    'ticker',
                                                    'peer_string',
                                                    'peer_list'
    """    
    # create Client subscribed API key and version
    iex_c = p.Client(api_token=get_api_key_from_cfg(), version = 'stable')
    peer_dict = {}
    for symbol in symbol_list:
        peer_dict[symbol] = (',').join(iex_c.peersDF(symbol).peer.values)   
    peer_df = pd.DataFrame.from_dict(peer_dict, orient='index').reset_index()
    peer_df.columns = ['ticker', 'peer_string']
    peer_df['peer_list'] = peer_df['peer_string'].str.split(',')
    peer_df.to_csv('..//data//1_company_info//peer_list.csv', index=False)
    return peer_df

def get_peer_data_from_disk(symbol_list):
    """load peer data from disk
    
    Args:
        symbol_list (str): list of stock ticker symbols
    
    Returns:
        peer_df (Pandas Dataframe): peer data, columns: 
                                                    'ticker',
                                                    'peer_string',
                                                    'peer_list'
    """    
    peer_df = pd.read_csv('..//data//1_company_info//peer_list.csv')
    peer_df['peer_list'] = peer_df['peer_string'].str.split(',')
    peer_df = peer_df[peer_df['ticker'].isin(symbol_list)].reset_index(drop=True)
    return peer_df

def download_advanced_stats_data(symbol_list):
    """retrieves advanced stats data from IEX API

    Args:
        symbol_list (str): list of stock ticker symbols

    Returns:
        peer_df (Pandas Dataframe): peer data, columns: 
                                                    'Ticker',
                                                    'Peer_string',
                                                    'Peer_list'
    """    
    # create Client subscribed API key and version
    iex_c = p.Client(api_token=get_api_key_from_cfg(), version = 'stable')
    stats_list = []
    for symbol in symbol_list:
        try:
            stats_df = iex_c.advancedStatsDF(symbol)
            stats_df.insert(0, 'Ticker', symbol) 
            stats_df['download_date'] = pd.to_datetime('now')
            stats_list.append(stats_df) 
        except:
            print(f'Failed to download data for symbol {symbol}')
    stats_df = pd.concat(stats_list, ignore_index=True)
    stats_df.to_csv('..//data//2_fundamentals//iex_data//advanced-stats.csv', index=False)
    return stats_df