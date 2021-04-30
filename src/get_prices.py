import pandas as pd
import yfinance as yf
import os, contextlib
import glob
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType 
from pyspark.sql.types import ArrayType, DoubleType, DateType
from pyspark.sql import functions as F

def download_ticker_prices(symbol_list, period='10y'):
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
    
def load_ticker_prices(spark, symbol_list):
    """[summary]

    Args:
        spark ([type]): [description]
        symbol_list ([type]): [description]

    Returns:
        [type]: [description]
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
    df.printSchema()
    return df

def calculate_annual_price(spark, price_df, time_period):
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
    # group by Ticker and aggregate on median
    ann_price_df = price_df.select('year', 'Ticker', 'Low')\
                            .groupBy('year', 'Ticker')\
                            .agg(F.mean('Low')\
                                    .alias('mean_low_price'))\
                            .orderBy('year', 'Ticker')
    # convert Spark Dataframe to Pandas Dataframe
    ann_price_df = ann_price_df.toPandas()
    return ann_price_df


