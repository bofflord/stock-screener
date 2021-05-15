import pandas as pd
import yfinance as yf
import os, contextlib
import glob
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, DateType
from pyspark.sql import functions as F

def download_ticker_prices(symbol_list, period='max'):
    """download prices from yfinance for all symbols in a list of the defined period.

    Args:
        symbol_list (str): list of stock symbols (e.g. AAPL for Apple)
        period (str): valid periods: 1d,5d,1mo,3mo,6mo,1y,2y,5y,10y,ytd,max. Default: max.
    """   
    symbol_count = len(symbol_list) 
    is_valid = [False] * len(symbol_list)
    print('Start download of historic price data')
    # force silencing of verbose API
    with open(os.devnull, 'w') as devnull:
        with contextlib.redirect_stdout(devnull):
            for i in range(0, symbol_count):
                print('Downloading price data {} of {}'.format(i+1, symbol_count))
                s = symbol_list[i]
                try:
                    data = yf.download(s, period=period)
                    if len(data.index) == 0:
                        continue
                    is_valid[i] = True
                    # insert Ticker column
                    data.insert(0, 'Ticker', s)
                    data.to_csv('../data/3_prices/{}.csv'.format(s))
                except:
                    print(f'Error occured at download of symbol {s}')
    print('Ticker price data extracted...')
    print('Total number of valid symbols downloaded = {}'.format(sum(is_valid)))
    
def load_ticker_prices(spark, symbol_list):
    """Load ticker prices from disk to Spark Dataframe

    Args:
        spark ([SparkContext]): [spark context]
        symbol_list ([list]): [List of ticker symbols]

    Returns:
        [Spark Dataframe]: Dataframe with price data
    """    
    file_list = glob.glob('../data/3_prices/*.csv')
    file_list = [file_path for file_path in file_list\
                    if any(symbol in file_path for symbol in symbol_list)]
    schema = StructType() \
                .add("Date",DateType(),True) \
                .add("Ticker",StringType(),True) \
                .add("Open",DoubleType(),True) \
                .add("High",DoubleType(),True) \
                .add("Low",DoubleType(),True) \
                .add("Close",DoubleType(),True) \
                .add("Adj Close",DoubleType(),True) \
                .add("Volume",DoubleType(),True) 
    df = spark.read.option('header',True)\
                    .schema(schema)\
                    .csv(file_list)
    return df

def calculate_annual_price(spark, price_df, time_period):
    """Calculate annual price from daily price data. Definition: mean low price in month December.

    Args:
        spark ([SparkContect]): [description]
        price_df ([Spark Dataframe]): historic daily price data
        time_period ([dict]): dictionary with keys start_date and end_date which stores these in int format.

    Returns:
        [Pandas Dataframe]: Dataframe with annual price data
    """    
    # create column month
    price_df = price_df.withColumn('month', F.month(price_df['Date']))
    # create column year
    price_df = price_df.withColumn('year', F.year(price_df['Date']))
    # filter for time period
    price_df = price_df.filter(
                    (price_df['year']>=time_period['start_date']) & \
                    (price_df['year']<=time_period['end_date'])
                                )
    # filter for month December (==12)
    price_df = price_df.filter(price_df['month']==12)
    # group by Ticker and aggregate on mean
    ann_price_df = price_df.select('year', 'Ticker', 'Low')\
                            .groupBy('year', 'Ticker')\
                            .agg(F.mean('Low')\
                                    .alias('mean_low_price'))\
                            .orderBy('year', 'Ticker')
    # convert Spark Dataframe to Pandas Dataframe
    ann_price_df = ann_price_df.toPandas()
    return ann_price_df

def find_stocks_below_mos(spark, price_df, growth_df):
    """Return screener results of stocks whose intrinsic value is below market price.

    Args:
        spark ([SparkContext]): context of current Spark session
        price_df ([Pyspark Dataframe]): price dataset
        growth_df ([Pandas Dataframe]): growth kpi dataset

    Returns:
        [Pandas Dataframe]: screener results table
    """    
    # price_df : group by Ticker and filter price_df for max date, keep low price
    curr_price_df = price_df.select('Date', 'Ticker')\
                            .groupBy('Ticker')\
                            .agg(F.max('Date').alias('Date'))
    curr_price_df = curr_price_df.join(price_df.select('Date', 'Ticker', 'Low'), 
                                        ['Date', 'Ticker'], 
                                        'left_outer')
    curr_price_df = curr_price_df.withColumnRenamed('Date', 'last_date')\
                               .withColumnRenamed('Low', 'last_low_price') 
    # create Spark df from Pandas df growth_df
    growth_df_spark = spark.createDataFrame(\
                            growth_df[['Ticker', 'price_future', 'sticker_price', 'mos']])
    # filter on condition price < mos
    curr_price_df = curr_price_df.join(growth_df_spark,
                                        'Ticker',
                                        'left_outer')
    curr_price_df = curr_price_df.filter(curr_price_df['last_low_price'] <= curr_price_df['mos'])
    # drop rows with missing values
    curr_price_df = curr_price_df.na.drop()
    # convert to Pandas df
    screener_df = curr_price_df.toPandas()
    # return Pandas df
    return screener_df
