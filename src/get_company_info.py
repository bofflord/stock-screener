import pandas as pd

def get_stock_symbol_list():
    """[Retrieve official symbol lists for all NASDAQ traded stocks]

    Returns:
        [list]: [filtered list of relevant stock symbols]
    """    
    # get ticket symbol list
    symbol_df = pd.read_csv("http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt", sep='|')
    # exclude test issues
    symbol_df = symbol_df[(symbol_df['Test Issue'] == 'N')]
    # exclude companies that are bankrupt
    symbol_df = symbol_df[symbol_df['Financial Status'].isna() | (symbol_df['Financial Status']=='N')]
    # exclude ETFs
    symbol_df = symbol_df[symbol_df['ETF']=='N']
    symbol_list = symbol_df['NASDAQ Symbol'].tolist()
    print('Symbol data extracted...')
    print('total number of symbols traded = {}'.format(len(symbol_list)))
    return symbol_list

def load_company_info_from_disk(symbol_list=['_all']):
    """load company info data from list for given symbol list.

    Args:
        symbol_list (str): list of stock ticker symbols

    Returns:
        company_info_df (Pandas DataFrame): company info data with columns:
                                            'ticker',
                                            'company name',
                                            'short name',
                                            'industry',
                                            'description',
                                            'website',
                                            'logo',
                                            'ceo',
                                            'exchange',
                                            'market cap',
                                            'sector',
                                            'tag 1',
                                            'tag 2',
                                            'tag 3'
    """    
    company_info_df = pd.read_csv('..//data//1_company_info//companies.csv')
    if symbol_list[0] != '_all':
        # reduce companies to those listed in the NASDAQ
        company_info_df = company_info_df[company_info_df['ticker'].isin(symbol_list)]
    print('Number of stocks symbols in list: {}'.format(company_info_df['ticker'].nunique()))
    print('Company data loaded from disk...')
    return company_info_df