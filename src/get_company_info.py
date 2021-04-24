import pandas as pd

def load_company_info_from_disk(symbol_list):
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
    # reduce companies to those listed in the NASDAQ
    company_info_df = company_info_df[company_info_df['ticker'].isin(symbol_list)]
    return company_info_df