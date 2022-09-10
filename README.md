# Rule #1 Stock Screener
### Data Engineering Capstone Project

#### Project Summary
There are various ways to make investment decisions on the stockmarket. Many are based on data analysis.

One investment strategy which became wide-known due to well-known proponents such as Benjamin Graham and Warren Buffet is called [value investing] (https://en.wikipedia.org/wiki/Value_investing). In layman's terms it assumes that:
- via fundamental analysis an investor can buy stocks at less than their intrinsic value. 
- the intrinsic value will however in the long time be recognised by the market.

[Fundamental analysis] (https://en.wikipedia.org/wiki/Fundamental_analysis) is  done by calculating and evaluating KPIs from the financial statements of businesses. From these KPIs the intrinsic value of the stock is then derived. One guideline on how to do this is provided by [Phil Town in his book "Rule #1"] (https://en.wikipedia.org/wiki/Phil_Town).

He breaks the [relevant KPIs for value growth down to 5] (https://medium.datadriveninvestor.com/the-rule-1-of-long-term-investing-5e34c5702e49):
- Return on Investment Capital (ROIC)
- Sales growth rate
- Earnings per Share (EPS) growth rate
- Book Value per Share (BVPS) or Equity, growth rate
- Free Cash Flow (FCF or Cash) growth rate

Additionally Phil Town provides a formula on how to calculate the intrinisic value. This value he calles the "sticker price". An example calculation is available [here] (https://meetinvest.com/glossary/sticker-price#:~:text=where%20future%20market%20price%20%3D%20future,%2FE%20*%20estimated%20future%20EPS.).


This project aims to provide curated data assets on stocks traded in the NASDAQ exchange for an investment analyst in order to:
- inform on the industry background of a company and its peers.
- conduct fundamental analysis based on the rule #1 kpi set. 
- evaluate value growth KPIs and derive the sticker price.
- screen markets for stocks whose prices is under their intrinsic value.
- enable further optimization and backtesting via historic market price data.


The project follows the follow steps:
* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up


```python
# Do all imports and installs here
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import simfin as sf
import os
import shutil
```


```python
# Local spark cluster specific imports for Windows
import findspark
findspark.init()
```


```python
# spark specific import. 
# Note: wait until run of previous cell is complete to avoid start-up issues.
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
spark = SparkSession.builder.getOrCreate()
df = spark.sql("select 'spark' as hello ")
df.show()
```

    22/02/26 12:01:02 WARN Utils: Your hostname, STH-4 resolves to a loopback address: 127.0.1.1; using 172.28.52.241 instead (on interface eth0)
    22/02/26 12:01:02 WARN Utils: Set SPARK_LOCAL_IP if you need to bind to another address
    Using Spark's default log4j profile: org/apache/spark/log4j-defaults.properties
    Setting default log level to "WARN".
    To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
    22/02/26 12:01:04 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
    [Stage 0:>                                                          (0 + 1) / 1]

    +-----+
    |hello|
    +-----+
    |spark|
    +-----+
    


                                                                                    


```python
# import of functions from custom modules. 
# Note: wait until run of previous cell is complete to avoid start-up issues.
from get_prices import *
from get_fundamentals import *
from get_peers import *
from get_company_info import *
from analysis_utils import *
from pipelines import *
```

## Step 1: Scope the Project and Gather Data

### Scope 
Explain what you plan to do in the project in more detail. What data do you use? What is your end solution look like? What tools did you use? etc>

After some initial research it was found that the required data on 
- company information
- financial statements 
- historical prices

is available via APIs of various providers. 

The financial statement and price data is then used to create the following additional data sets:
- growth KPIs: KPIs relevant for the growth #1 investment strategy
- screener results: identified stock which current prices is below the calculated intrinsic value.

The data engineering pipelines built in this project process the data in two steps:

1. Extraction from source and load to staging folder.
2. Extraction from staging folder, transformation and load to target folders.

The goal is to provide all tables as files in a target folder from which they will be loaded to an analytical tool for the evaluation.

Since all providers chosen for this project offer a python API the first pipeline for source data extraction is purely realized via python shell scripts.

However since the amount of data is quite largely (ca. 10 million records on historical stock pices), the second data pipeline which processes the data and creates the output files uses a combination of python and PySpark.

The project was entirely developed to run locally on a Windows computer. Consequently some specific imports and start-up procedures need to be followed in order to ensure a smooth run.

### Describe and Gather Data 
Below the various data sets and sources are described in the categories 
- company information
- financial statements 
- historical prices

Furthermore an overview is given on the definition of the content in the data sets:
- growth KPIs
- screener results

Describe the data sets you're using. Where did it come from? What type of information is included? 

#### 1.1 Company information
- Ticker symbol list: "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    - [Symbol Look-Up/Directory Data Fields & Definitions] (http://www.nasdaqtrader.com/trader.aspx?id=symboldirdefs)
    - "Ticker" symbol of a stock is the primary/ foreign key which connects all of the tables with each other.
    - it is filtered to only those Ticker symbols which
        - are not test issues
        - are stocks and not ETFs
        - are not bankrupt
- Company information: Kaggle dataset from 2019 https://www.kaggle.com/marketahead/all-us-stocks-tickers-company-info-logos 
    - this data was retrieved from the IEX Cloud API.
    - in order to save costs it was decided to re-use this dataset instead of downloading the data fresh from the API.
- Peer group information: IEX Cloud API https://iexcloud.io/docs/api/#peer-groups
    - the corresponding python API package is [pyEX] (https://pyex.readthedocs.io/en/latest/#)
    - this data is merged to the company information data set.

#### 1.2 Fundamental indicators 

- all fundamental data is retrieved via the [SimFin] API (https://simfin.com/contribute/overview#/)
    - the corresponding python API package is [simfin] (https://github.com/SimFin/simfin)
- the used financial statements are:
    - Income statement
    - Balance sheet
    - Cashflow statement
- via the free API key used for this project, the data of the latest year (currently 2020) is not available. This data is only available via a paid subscription.

From these data sets the 5 relevant  KPIs listed by Phil Town are calculated:
- Return on Invested Capital (ROIC)
    - Source: Income Statement
        - net income: n_i -> column 'Net Income'
        - dividend: di -> column 'Dividends Paid'
        - debt: de -> summed up with equity in column 'Total Liabilities & Equity'
        - equity: eq
    - Definition: ROIC = (ni - di) / (de + eq)
- Sales Growth Rate
    - Source: Income Statement
    - Definition:  Sales is equal to column 'revenue'
- Earnings per Share Growth Rate
    - Source: Income Statement
        - Earnings: ea -> column 'Net Income'
        - number of shares : sh -> column 'Shares (Basic)'
    - Definition: ea / sh
- Book Value per Share Growth Rate
    - Source: Balance Sheet
        - Total Equity: t_e -> column 'Total Equity'
        - Prefered Equity: p_e (not available)
        - number of shares : sh -> column 'Shares (Basic)'
    - Definition: (t_e - p_e) / sh
- Free Cash Flow Growth Rate
    - Source: 
        - Cash Flow: 
            - Cashflow from Operating Activities: cf_oa -> column 'Net Cash from Operating Activities'
            - Capital Expenditure: capex -> column 'Net Cash from Investing Activities'
        - Income Statement:
            - Interest Expenses: i_e -> excluded for simplicity, column 'Interest Expense, Net'
            - Tax shield on Interest Expense: t_i_e  -> excluded for simplicity
    - Definition: Free Cashflow (f_cf) = cf_oa + i_e - t_i_e - capex


#### 1.3 Pricing information ####
The purpose of this data is to evaluate the so-called sticker prices and margin of safety based on current stock prices. It can also be used for backtesting criteria on historic data.
- all historic price data is retrieve via the Yahoo Finance API
    - the corresponding python package is [yfinance] (https://pypi.org/project/yfinance/)
    - for the data extraction an [existing script by Oleh Onyshchak] (https://www.kaggle.com/jacksoncrow/download-nasdaq-historical-data) was used and adapted.


#### 1.4 Growth KPI ####

From the fundamental KPIs growth indicators are derived which represent the performance over time. From these the intrinsic value of a stock is calculated via the "sticker price". An additional margin of safety is added to that.
- Sticker price calculation
    - future period fp, by default 10 years
    - Sticker price = future market price / (1 + exp_rr)^fp
    - expected annual return rate exp_rr, by default 15%
    - future market price = future P/E * estimated future EPS
        - future P/E = min(pe_default, pe_5yr_avg)
            - default price per earnings pe_default: 2* rule #1 growth rate (see below)
            - 5 year average of annual price per earnings pe_5yr_avg
                - annual price per earnings pe = price/ eps
                    - annual price = mean of daily low prices in month December
        - estimated future EPS f_eps = current EPS * (1+ rule1_gr)^fp
            - rule #1 growth rate rule1_gr = min(bvps_gr_5yr, eps_gr_5yr)
- Margin of safety: half the the sticker price.

#### 1.5 Screener results ####
This table is where things get interesting. The purpose of this table is to show all stocks which latest low price is below the calculated intrinsic value (including a margin of safety). For these stocks we get "value" for our bucks.

The table is generated by:
- extracting the latest stock price data for all stocks.
- joining the intrinsic values for each stock from the growth kpi table.
- filtering and keeping only those stocks which price is below the margin of safety value.


## Step 2: Explore and Assess the Data
#### Explore the Data ####
For the data exploration purposes the python package pandas profiling is used.
It generates a so called profiling report for a Pandas Dataframe.

Identify data quality issues, like missing values, duplicate data, etc.

#### Cleaning Steps
Document steps necessary to clean the data

### 2.1 Company information
#### 2.1.1 Ticker Symbol List


```python
# get ticket symbol list
symbol_df = pd.read_csv("http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt", sep='|')
symbol_df.head(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Nasdaq Traded</th>
      <th>Symbol</th>
      <th>Security Name</th>
      <th>Listing Exchange</th>
      <th>Market Category</th>
      <th>ETF</th>
      <th>Round Lot Size</th>
      <th>Test Issue</th>
      <th>Financial Status</th>
      <th>CQS Symbol</th>
      <th>NASDAQ Symbol</th>
      <th>NextShares</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>Y</td>
      <td>A</td>
      <td>Agilent Technologies, Inc. Common Stock</td>
      <td>N</td>
      <td></td>
      <td>N</td>
      <td>100.0</td>
      <td>N</td>
      <td>NaN</td>
      <td>A</td>
      <td>A</td>
      <td>N</td>
    </tr>
    <tr>
      <th>1</th>
      <td>Y</td>
      <td>AA</td>
      <td>Alcoa Corporation Common Stock</td>
      <td>N</td>
      <td></td>
      <td>N</td>
      <td>100.0</td>
      <td>N</td>
      <td>NaN</td>
      <td>AA</td>
      <td>AA</td>
      <td>N</td>
    </tr>
    <tr>
      <th>2</th>
      <td>Y</td>
      <td>AAA</td>
      <td>Listed Funds Trust AAF First Priority CLO Bond...</td>
      <td>P</td>
      <td></td>
      <td>Y</td>
      <td>100.0</td>
      <td>N</td>
      <td>NaN</td>
      <td>AAA</td>
      <td>AAA</td>
      <td>N</td>
    </tr>
  </tbody>
</table>
</div>




```python
print('Number of symbol in NASDAQ list before any filters: {}'\
    .format(symbol_df['NASDAQ Symbol'].nunique()))
# exclude test issues
symbol_df = symbol_df[(symbol_df['Test Issue'] == 'N')]
# exclude companies that are bankrupt
symbol_df = symbol_df[symbol_df['Financial Status'].isna() | (symbol_df['Financial Status']=='N')]
# exclude ETFs
symbol_df = symbol_df[symbol_df['ETF']=='N']
symbol_list = symbol_df['NASDAQ Symbol'].tolist()
print('Number of symbol in NASDAQ list after filters: {}'\
    .format(len(symbol_list)))
```

    Number of symbol in NASDAQ list before any filters: 12037
    Number of symbol in NASDAQ list after filters: 9035


#### 2.1.2 Company Info
From the Pandas Profiling report the following insights can be gathered:
- data for 4559 ticker symbols is available
- the amount of missing data is quite low (5.3%) and mostly on the columns logo, ceo and tag.
- the data quality is considered high, further cleaning is not required.


```python
company_info_df = load_company_info_from_disk(symbol_list)
company_info_df.head(1)
```

    Number of stocks symbols in list: 4243
    Company data loaded from disk...





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ticker</th>
      <th>company name</th>
      <th>short name</th>
      <th>industry</th>
      <th>description</th>
      <th>website</th>
      <th>logo</th>
      <th>ceo</th>
      <th>exchange</th>
      <th>market cap</th>
      <th>sector</th>
      <th>tag 1</th>
      <th>tag 2</th>
      <th>tag 3</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>Agilent Technologies Inc.</td>
      <td>Agilent</td>
      <td>Medical Diagnostics &amp; Research</td>
      <td>Agilent Technologies Inc is engaged in life sc...</td>
      <td>http://www.agilent.com</td>
      <td>A.png</td>
      <td>Michael R. McMullen</td>
      <td>New York Stock Exchange</td>
      <td>2.421807e+10</td>
      <td>Healthcare</td>
      <td>Healthcare</td>
      <td>Diagnostics &amp; Research</td>
      <td>Medical Diagnostics &amp; Research</td>
    </tr>
  </tbody>
</table>
</div>




```python
create_pandas_profiling_report(company_info_df, 'company_info_df')
```


    Summarize dataset:   0%|          | 0/5 [00:00<?, ?it/s]



    Generate report structure:   0%|          | 0/1 [00:00<?, ?it/s]



    Render HTML:   0%|          | 0/1 [00:00<?, ?it/s]



    Export report to file:   0%|          | 0/1 [00:00<?, ?it/s]


    
    Pandas profiling report of file company_info_df created
    
    



```python
# reduce symbol_list to those where company information is available
symbol_list = company_info_df['ticker'].unique().tolist()
```

#### 2.1.3 Peer group information
For retrieving this data via the API there is a cost per ticker symbol. In order to limit these costs it was decided to limit data retrieval to only those symbols which are in the company information data set (4545 ticker symbols in total).

From Pandas Profiling report the following insights can be gathered:
- an API call was successfull for 4536 out of the 4545 ticker symbols.
- For 3375 of these ticker symbols the peer data is available.

Since the peer group is considered a supplementary information, it was decided to proceed in the project with the total number of symbols in the company information data set. The amount of missing values for peer group is acceptable.


```python
# initial download of peer data from API
# Note: requires a valid API key
#peer_df = download_peer_data(symbol_list)
```




```python
# load downloaded peer data from disk
peer_df = get_peer_data_from_disk(symbol_list)
peer_df_shape = peer_df.shape
print(f'Shape of peer_df: {peer_df_shape}')
```

    Shape of peer_df: (4187, 3)



```python
create_pandas_profiling_report(peer_df, 'peer_df')
```


    Summarize dataset:   0%|          | 0/5 [00:00<?, ?it/s]



    Generate report structure:   0%|          | 0/1 [00:00<?, ?it/s]



    Render HTML:   0%|          | 0/1 [00:00<?, ?it/s]



    Export report to file:   0%|          | 0/1 [00:00<?, ?it/s]


    
    Pandas profiling report of file peer_df created
    
    



```python
# add peer data to company info
company_info_df = company_info_df.merge(peer_df, 
                                        on = 'ticker',
                                        how='left',
                                        validate='1:1')
company_info_df.head(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>ticker</th>
      <th>company name</th>
      <th>short name</th>
      <th>industry</th>
      <th>description</th>
      <th>website</th>
      <th>logo</th>
      <th>ceo</th>
      <th>exchange</th>
      <th>market cap</th>
      <th>sector</th>
      <th>tag 1</th>
      <th>tag 2</th>
      <th>tag 3</th>
      <th>peer_string</th>
      <th>peer_list</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>A</td>
      <td>Agilent Technologies Inc.</td>
      <td>Agilent</td>
      <td>Medical Diagnostics &amp; Research</td>
      <td>Agilent Technologies Inc is engaged in life sc...</td>
      <td>http://www.agilent.com</td>
      <td>A.png</td>
      <td>Michael R. McMullen</td>
      <td>New York Stock Exchange</td>
      <td>2.421807e+10</td>
      <td>Healthcare</td>
      <td>Healthcare</td>
      <td>Diagnostics &amp; Research</td>
      <td>Medical Diagnostics &amp; Research</td>
      <td>TMO,PKI,DHR,TER,NATI,ILMN,AME,BRKR,GE,SPMYY</td>
      <td>[TMO, PKI, DHR, TER, NATI, ILMN, AME, BRKR, GE...</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AA</td>
      <td>Alcoa Corporation</td>
      <td>Alcoa</td>
      <td>Metals &amp; Mining</td>
      <td>Alcoa Corp is an integrated aluminum company. ...</td>
      <td>http://www.alcoa.com</td>
      <td>AA.png</td>
      <td>Roy Christopher Harvey</td>
      <td>New York Stock Exchange</td>
      <td>5.374967e+09</td>
      <td>Basic Materials</td>
      <td>Basic Materials</td>
      <td>Aluminum</td>
      <td>Metals &amp; Mining</td>
      <td>ACH,KALU,CENX,NHYDY,AWCMY,BBL,BHP</td>
      <td>[ACH, KALU, CENX, NHYDY, AWCMY, BBL, BHP]</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AAC</td>
      <td>AAC Holdings Inc.</td>
      <td>AAC</td>
      <td>Health Care Providers</td>
      <td>AAC Holdings Inc provides inpatient and outpat...</td>
      <td>http://www.americanaddictioncenters.org</td>
      <td>NaN</td>
      <td>Michael T. Cartwright</td>
      <td>New York Stock Exchange</td>
      <td>6.372010e+07</td>
      <td>Healthcare</td>
      <td>Healthcare</td>
      <td>Medical Care</td>
      <td>Health Care Providers</td>
      <td>SEM,ACHC,USPH,BICX</td>
      <td>[SEM, ACHC, USPH, BICX]</td>
    </tr>
  </tbody>
</table>
</div>



### 2.2 Fundamental indicators
The Simfin API others a bulk download of fundamental data traded on American stock exchanges.
From the Pandas Profiling report the following insights can be gathered:
- Income statement: 
    - data for 2296 ticker symbols available.
    - column "Revenue" has low number of missing values.
    - Fiscal Years: data availability after 2010 is good, but no before.
- Balance sheet:
    - data for 2297 ticker symbols available.
    - Fiscal Years: data availability after 2010 is good, but no before.
- Cashflow statement:
    - data for 2296 ticker symbols available.
    - Fiscal Years: data availability after 2010 is good, but no before.
    - column "Shares (Basic)" has low number of missing values.
    - column "Dividents Paid" has high number of missing values (50%).
        - data cleansing: NA values are replaced with Zero.
        - this way the missing data has no effect on the overall ROIC calculation.
        - this is however a cause of potential errors in the KPI.
    - column "Net Cash from Investing Activities" has low number of missing values.

However the number of Ticker symbols with data overlapping with those in the symbol list derived from the company information data set is considerably lower:
- Available symbols in data set cashflow_df: 1660 of total 4545
- Available symbols in data set income_sm_df: 1660 of total 4545
- Available symbols in data set balance_st_df: 1661 of total 4545

Since the fundamental data is at the core of value investing strategies, the symbol list will be reduced to the 1660 tickers with available fundamental data


```python
# initialize simfin API
init_simfin_api()
sf.set_api_key("63QS1kmWV1MEWj7lnQNJUc9T2C1yN3gu")
```


```python
market='us'
variant='annual'
# download cashflow data from the SimFin server and load into a Pandas DataFrame.
cashflow_df = sf.load_cashflow(variant=variant, market=market)
cashflow_df = cashflow_df.reset_index()
# Download the data from the SimFin server and load into a Pandas DataFrame.
income_sm_df = sf.load_income(variant=variant, market=market)
income_sm_df = income_sm_df.reset_index()
# Download the data from the SimFin server and load into a Pandas DataFrame.
balance_st_df = sf.load_balance(variant=variant, market=market)
balance_st_df = balance_st_df.reset_index()
```

    Dataset "us-cashflow-annual" on disk (286 days old).
    - Downloading ... 100.0%
    - Extracting zip-file ... Done!
    - Loading from disk ... Done!
    Dataset "us-income-annual" on disk (286 days old).
    - Downloading ... 100.0%
    - Extracting zip-file ... Done!
    - Loading from disk ... Done!
    Dataset "us-balance-annual" on disk (264 days old).
    - Downloading ... 100.0%
    - Extracting zip-file ... Done!
    - Loading from disk ... Done!



```python
# create Pandas Profiling Report for each DataFrame
for df, df_name in zip([cashflow_df, income_sm_df, balance_st_df],
                        ['cashflow_df', 'income_sm_df', 'balance_st_df']):
    create_pandas_profiling_report(df, df_name)
```


    Summarize dataset:   0%|          | 0/5 [00:00<?, ?it/s]



    Generate report structure:   0%|          | 0/1 [00:00<?, ?it/s]



    Render HTML:   0%|          | 0/1 [00:00<?, ?it/s]



    Export report to file:   0%|          | 0/1 [00:00<?, ?it/s]


    
    Pandas profiling report of file cashflow_df created
    
    



    Summarize dataset:   0%|          | 0/5 [00:00<?, ?it/s]



    Generate report structure:   0%|          | 0/1 [00:00<?, ?it/s]



    Render HTML:   0%|          | 0/1 [00:00<?, ?it/s]



    Export report to file:   0%|          | 0/1 [00:00<?, ?it/s]


    
    Pandas profiling report of file income_sm_df created
    
    



    Summarize dataset:   0%|          | 0/5 [00:00<?, ?it/s]



    Generate report structure:   0%|          | 0/1 [00:00<?, ?it/s]



    Render HTML:   0%|          | 0/1 [00:00<?, ?it/s]



    Export report to file:   0%|          | 0/1 [00:00<?, ?it/s]


    
    Pandas profiling report of file balance_st_df created
    
    



```python
# check availability of fundamental data for stocks in symbol list
for df, df_name in zip([cashflow_df, income_sm_df, balance_st_df],
                        ['cashflow_df', 'income_sm_df', 'balance_st_df']):
    symbol_cnt = len(symbol_list)
    available_symbol_cnt = df[df['Ticker'].isin(symbol_list)]['Ticker'].nunique()
    print(f'Available symbols in data set {df_name}: {available_symbol_cnt} of total {symbol_cnt}')
```

    Available symbols in data set cashflow_df: 1714 of total 4243
    Available symbols in data set income_sm_df: 1714 of total 4243
    Available symbols in data set balance_st_df: 1714 of total 4243



```python
# determine time period for analysis
period_dict = {'start_date':2010,
                'end_date':2019}
```


```python
# combine fundamentals and calculate top5 kpis
fundamental_df = combine_fundamentals(symbol_list)
# filter on relevant time period
fundamental_df = filter_df(fundamental_df, period_dict)
fundamental_df = calculate_top5_kpi(fundamental_df)
# reduce symbol_list to those where fundamental data is available
symbol_list = fundamental_df['Ticker'].unique().tolist()
fundamental_df.tail(3)
```

    Dataset "us-cashflow-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Dataset "us-income-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Dataset "us-balance-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Symbols with available fundamental data: 1714
    Combined all fundamental data from financial statements to one Dataframe.
    Calculated roic and added it to Dataframe
    Calculated eps and added it to Dataframe
    Calculated bvps and added it to Dataframe
    Calculated fcf and added it to Dataframe
    top5 KPIs added to fundamental data





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Ticker</th>
      <th>Report Date_is</th>
      <th>SimFinId</th>
      <th>Currency</th>
      <th>Fiscal Year</th>
      <th>Fiscal Period_is</th>
      <th>Publish Date_is</th>
      <th>Restated Date_is</th>
      <th>Shares (Basic)_is</th>
      <th>Shares (Diluted)_is</th>
      <th>...</th>
      <th>Share Capital &amp; Additional Paid-In Capital</th>
      <th>Treasury Stock</th>
      <th>Retained Earnings</th>
      <th>Total Equity</th>
      <th>Total Liabilities &amp; Equity</th>
      <th>Dividends Paid_clean</th>
      <th>roic</th>
      <th>eps</th>
      <th>bvps</th>
      <th>fcf</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>6634</th>
      <td>ZYNE</td>
      <td>2017-12-31</td>
      <td>901704</td>
      <td>USD</td>
      <td>2017</td>
      <td>FY</td>
      <td>2018-03-12</td>
      <td>2019-03-11</td>
      <td>12914814.0</td>
      <td>12914814.0</td>
      <td>...</td>
      <td>138930454.0</td>
      <td>NaN</td>
      <td>-77980866.0</td>
      <td>60949588</td>
      <td>69054309</td>
      <td>0.0</td>
      <td>-0.463582</td>
      <td>-2.478727</td>
      <td>4.719355</td>
      <td>-25727095.0</td>
    </tr>
    <tr>
      <th>6635</th>
      <td>ZYNE</td>
      <td>2018-12-31</td>
      <td>901704</td>
      <td>USD</td>
      <td>2018</td>
      <td>FY</td>
      <td>2019-03-11</td>
      <td>2021-03-10</td>
      <td>15308886.0</td>
      <td>15308886.0</td>
      <td>...</td>
      <td>175493702.0</td>
      <td>NaN</td>
      <td>-117892041.0</td>
      <td>57601661</td>
      <td>67327443</td>
      <td>0.0</td>
      <td>-0.592792</td>
      <td>-2.607059</td>
      <td>3.762629</td>
      <td>-32110693.0</td>
    </tr>
    <tr>
      <th>6636</th>
      <td>ZYNE</td>
      <td>2019-12-31</td>
      <td>901704</td>
      <td>USD</td>
      <td>2019</td>
      <td>FY</td>
      <td>2020-03-10</td>
      <td>2021-03-10</td>
      <td>22000203.0</td>
      <td>22000203.0</td>
      <td>...</td>
      <td>226432367.0</td>
      <td>NaN</td>
      <td>-150835624.0</td>
      <td>75596743</td>
      <td>87764596</td>
      <td>0.0</td>
      <td>-0.375363</td>
      <td>-1.497422</td>
      <td>3.436184</td>
      <td>-34688586.0</td>
    </tr>
  </tbody>
</table>
<p>3 rows × 83 columns</p>
</div>



### 2.3 Pricing information
Since the amount of available data is quite large, it is not possible to use Pandas Profiling here for an evaluation. Instead descriptive statistics are derived from PySpark computations
- Number of rows in entire price data:  9.702.060
- Data for 1655 out of 1660 ticker symbols is available via the API.
- the price data is essential for the growth KPI calculation. Consequently the processed data will again be restricted to the ticker symbols with available price data.


```python
%%time
# Download historic stock prices for symbols
# Note: this might take a long time, consequently the code line below is commented.
#download_ticker_prices(symbol_list)
```

    CPU times: user 6 µs, sys: 1e+03 ns, total: 7 µs
    Wall time: 13.4 µs



```python
%%time
# load ticker prices for symbols
price_df = load_ticker_prices(spark, symbol_list)
# print Schema
price_df.printSchema()
```

                                                                                    

    root
     |-- Date: date (nullable = true)
     |-- Ticker: string (nullable = true)
     |-- Open: double (nullable = true)
     |-- High: double (nullable = true)
     |-- Low: double (nullable = true)
     |-- Close: double (nullable = true)
     |-- Adj Close: double (nullable = true)
     |-- Volume: double (nullable = true)
    
    CPU times: user 462 ms, sys: 81.1 ms, total: 543 ms
    Wall time: 17 s



```python
# print tail and visualize in Pandas
price_df.limit(3).toPandas()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Date</th>
      <th>Ticker</th>
      <th>Open</th>
      <th>High</th>
      <th>Low</th>
      <th>Close</th>
      <th>Adj Close</th>
      <th>Volume</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>1962-01-02</td>
      <td>HPQ</td>
      <td>0.131273</td>
      <td>0.131273</td>
      <td>0.124177</td>
      <td>0.124177</td>
      <td>0.046594</td>
      <td>2480333.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>1962-01-03</td>
      <td>HPQ</td>
      <td>0.124177</td>
      <td>0.124177</td>
      <td>0.121516</td>
      <td>0.122846</td>
      <td>0.046094</td>
      <td>507341.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>1962-01-04</td>
      <td>HPQ</td>
      <td>0.122846</td>
      <td>0.126838</td>
      <td>0.117968</td>
      <td>0.120185</td>
      <td>0.045096</td>
      <td>845568.0</td>
    </tr>
  </tbody>
</table>
</div>




```python
%%time
# count number of rows
price_df_row_count = price_df.count()
print(f'Number of rows in price data: {price_df_row_count}')
```

    [Stage 4:========================================================>(60 + 1) / 61]

    Number of rows in price data: 9967746
    CPU times: user 51.4 ms, sys: 2.76 ms, total: 54.2 ms
    Wall time: 14.1 s


                                                                                    


```python
%%time
price_df.select(F.countDistinct('Ticker')).show()
```

    [Stage 7:=======================================================> (59 + 2) / 61]

    +----------------------+
    |count(DISTINCT Ticker)|
    +----------------------+
    |                  1716|
    +----------------------+
    
    CPU times: user 54 ms, sys: 8.91 ms, total: 62.9 ms
    Wall time: 23.7 s


                                                                                    


```python
%%time
price_df.select(F.min('Date'),F.max('Date')).show()
```

    [Stage 13:=====================================================>  (58 + 2) / 61]

    +----------+----------+
    | min(Date)| max(Date)|
    +----------+----------+
    |1962-01-02|2021-06-04|
    +----------+----------+
    
    CPU times: user 57.8 ms, sys: 23.3 ms, total: 81.1 ms
    Wall time: 25.3 s


                                                                                    


```python
%%time
# restrict symbol_list to those with available price data
symbol_list = price_df.select('Ticker').distinct().toPandas()['Ticker'].tolist()
# filter on relevant time period
fundamental_df = filter_symbols(fundamental_df, symbol_list)
```

    [Stage 16:======================================================> (59 + 2) / 61]

    CPU times: user 102 ms, sys: 3.8 ms, total: 106 ms
    Wall time: 20.5 s


                                                                                    

### 2.4 Growth KPIs
The growth KPI calculation is calculated from indicators in the fundamental and price data sets. Two more KPIs need to be added to the fundamental data set as prequisite:
- annual price data. In this project: mean low price of all trading days in month December.
- annual price per earning ratio.

Once this is done, alle growth KPIs that are necessary for a calculation of the intrinsic stock value can be calculated from the fundamental data set.

From the Pandas Profiling report the following insights can be covered:
- generally not many missing values with the exception of the columns rule1_gr_5yr and rule1_gr_10yr.
- the high amount of missing values in these two columsn is created on purpose by a rule in the calculate_growth_summary function. 
    - This is done to exclude all ticker symbols with a negative growth rate in the following computations.
    - If such an exclusion is not done, the results of the screener can be misleading. Negative growth rates can mathematically result in positive intrinsic values, even though from a business perspective this is not possible.
- additionally there is a rule #1 quality check in place which sets the future market price (price_future) to NULL if either the roic or fcf growth rate is negative
- Data cleansing:
    - Since the methodolody of the intrinsic value calculation is based on the performance of the last 5 years, a rule was set to include only those symbols with data for the last 5 years.
        - 1507 out of 1655 ticker symbols fulfill this condition.
        - Additionally most indicators were also calculated for a 10 year time period.
        - 10 years: 71.1% of ticker symbols have data for the entire period.
    - it was found that the growth KPIs had infinite values which is caused by unexpected handling of missing values. For these cases it was decided to replace these infinite numbers with Null values (np.nan).
    - future earnings per share (eps_future): negative values are replaced with null values since they would cause intrinsic stock values which cannot occur in reality.
    - future price per earnings (pe_future): negative values are replaced with null values since they would cause intrinsic stock values which cannot occur in reality.
- the intrinsic stock value (columns future price, sticker price, mos) is calculated for 385 symbols out of the 1507 in the dataset (25.5%). 
    - The value distribution shows that at at least 95% of the results appear to be in a realistic value range.
    - about 1% of the intrinsic values appear to be unrealistically high.
    - this is caused by unrealistically high growth rates which in turn appear to be cause by changes that are not yet taken into account (e.g. mergers which dramatically change shares and turnover values).

    


```python
%%time
# calculate annual price from historic price data
ann_price_df = calculate_annual_price(spark, price_df, period_dict)
ann_price_df.tail(3)
```

                                                                                    

    CPU times: user 180 ms, sys: 21.7 ms, total: 202 ms
    Wall time: 36.8 s





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>year</th>
      <th>Ticker</th>
      <th>mean_low_price</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>15010</th>
      <td>2019</td>
      <td>ZUMZ</td>
      <td>31.880000</td>
    </tr>
    <tr>
      <th>15011</th>
      <td>2019</td>
      <td>ZUO</td>
      <td>14.341429</td>
    </tr>
    <tr>
      <th>15012</th>
      <td>2019</td>
      <td>ZYNE</td>
      <td>5.738524</td>
    </tr>
  </tbody>
</table>
</div>




```python
# calculate annual price per earnings
fundamental_df = calculate_annual_pe(ann_price_df, fundamental_df)
fundamental_df.tail(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Ticker</th>
      <th>Report Date_is</th>
      <th>SimFinId</th>
      <th>Currency</th>
      <th>Fiscal Year</th>
      <th>Fiscal Period_is</th>
      <th>Publish Date_is</th>
      <th>Restated Date_is</th>
      <th>Shares (Basic)_is</th>
      <th>Shares (Diluted)_is</th>
      <th>...</th>
      <th>Retained Earnings</th>
      <th>Total Equity</th>
      <th>Total Liabilities &amp; Equity</th>
      <th>Dividends Paid_clean</th>
      <th>roic</th>
      <th>eps</th>
      <th>bvps</th>
      <th>fcf</th>
      <th>mean_low_price</th>
      <th>pe</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>6163</th>
      <td>ZYNE</td>
      <td>2017-12-31</td>
      <td>901704</td>
      <td>USD</td>
      <td>2017</td>
      <td>FY</td>
      <td>2018-03-12</td>
      <td>2019-03-11</td>
      <td>12914814.0</td>
      <td>12914814.0</td>
      <td>...</td>
      <td>-77980866.0</td>
      <td>60949588</td>
      <td>69054309</td>
      <td>0.0</td>
      <td>-0.463582</td>
      <td>-2.478727</td>
      <td>4.719355</td>
      <td>-25727095.0</td>
      <td>11.937900</td>
      <td>-4.816141</td>
    </tr>
    <tr>
      <th>6164</th>
      <td>ZYNE</td>
      <td>2018-12-31</td>
      <td>901704</td>
      <td>USD</td>
      <td>2018</td>
      <td>FY</td>
      <td>2019-03-11</td>
      <td>2021-03-10</td>
      <td>15308886.0</td>
      <td>15308886.0</td>
      <td>...</td>
      <td>-117892041.0</td>
      <td>57601661</td>
      <td>67327443</td>
      <td>0.0</td>
      <td>-0.592792</td>
      <td>-2.607059</td>
      <td>3.762629</td>
      <td>-32110693.0</td>
      <td>3.816737</td>
      <td>-1.464001</td>
    </tr>
    <tr>
      <th>6165</th>
      <td>ZYNE</td>
      <td>2019-12-31</td>
      <td>901704</td>
      <td>USD</td>
      <td>2019</td>
      <td>FY</td>
      <td>2020-03-10</td>
      <td>2021-03-10</td>
      <td>22000203.0</td>
      <td>22000203.0</td>
      <td>...</td>
      <td>-150835624.0</td>
      <td>75596743</td>
      <td>87764596</td>
      <td>0.0</td>
      <td>-0.375363</td>
      <td>-1.497422</td>
      <td>3.436184</td>
      <td>-34688586.0</td>
      <td>5.738524</td>
      <td>-3.832270</td>
    </tr>
  </tbody>
</table>
<p>3 rows × 85 columns</p>
</div>




```python
# calculate growth kpi df
growth_df = calculate_growth_rates(fundamental_df, agg_func='mean')
growth_df.head(3)
```

    Calculated KPI growth from year to year.
    Calculated 5 and 10 year growth rate





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Ticker</th>
      <th>revenue_gr_curr</th>
      <th>eps_curr</th>
      <th>roic_gr_5yr</th>
      <th>revenue_gr_5yr</th>
      <th>eps_gr_5yr</th>
      <th>bvps_gr_5yr</th>
      <th>fcf_gr_5yr</th>
      <th>pe_5yr</th>
      <th>yrs_in_5yr</th>
      <th>...</th>
      <th>pe_default_5yr</th>
      <th>roic_gr_10yr</th>
      <th>revenue_gr_10yr</th>
      <th>eps_gr_10yr</th>
      <th>bvps_gr_10yr</th>
      <th>fcf_gr_10yr</th>
      <th>pe_10yr</th>
      <th>yrs_in_10yr</th>
      <th>rule1_gr_10yr</th>
      <th>pe_default_10yr</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ABMD</td>
      <td>0.092862</td>
      <td>4.493437</td>
      <td>0.24</td>
      <td>0.27</td>
      <td>0.62</td>
      <td>0.29</td>
      <td>0.38</td>
      <td>70.63</td>
      <td>5</td>
      <td>...</td>
      <td>58.0</td>
      <td>0.24</td>
      <td>0.27</td>
      <td>0.62</td>
      <td>0.29</td>
      <td>0.38</td>
      <td>70.63</td>
      <td>5</td>
      <td>0.29</td>
      <td>58.0</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AGYS</td>
      <td>0.141400</td>
      <td>-1466.319459</td>
      <td>1.11</td>
      <td>0.08</td>
      <td>641.86</td>
      <td>176.47</td>
      <td>-0.11</td>
      <td>-28.11</td>
      <td>5</td>
      <td>...</td>
      <td>35294.0</td>
      <td>1.11</td>
      <td>0.08</td>
      <td>641.86</td>
      <td>176.47</td>
      <td>-0.11</td>
      <td>-28.11</td>
      <td>5</td>
      <td>176.47</td>
      <td>35294.0</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AMSC</td>
      <td>0.135766</td>
      <td>-0.814677</td>
      <td>-0.56</td>
      <td>-0.07</td>
      <td>-0.84</td>
      <td>-0.09</td>
      <td>0.08</td>
      <td>-2.04</td>
      <td>5</td>
      <td>...</td>
      <td>NaN</td>
      <td>-0.56</td>
      <td>-0.07</td>
      <td>-0.84</td>
      <td>-0.09</td>
      <td>0.08</td>
      <td>-2.04</td>
      <td>5</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>3 rows × 21 columns</p>
</div>




```python
growth_df = calculate_sticker_price(growth_df, fp=10, exp_rr=0.15)
growth_df.head(3)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Ticker</th>
      <th>revenue_gr_curr</th>
      <th>eps_curr</th>
      <th>roic_gr_5yr</th>
      <th>revenue_gr_5yr</th>
      <th>eps_gr_5yr</th>
      <th>bvps_gr_5yr</th>
      <th>fcf_gr_5yr</th>
      <th>pe_5yr</th>
      <th>yrs_in_5yr</th>
      <th>...</th>
      <th>fcf_gr_10yr</th>
      <th>pe_10yr</th>
      <th>yrs_in_10yr</th>
      <th>rule1_gr_10yr</th>
      <th>pe_default_10yr</th>
      <th>pe_future</th>
      <th>eps_future</th>
      <th>price_future</th>
      <th>sticker_price</th>
      <th>mos</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>ABMD</td>
      <td>0.092862</td>
      <td>4.493437</td>
      <td>0.24</td>
      <td>0.27</td>
      <td>0.62</td>
      <td>0.29</td>
      <td>0.38</td>
      <td>70.63</td>
      <td>5</td>
      <td>...</td>
      <td>0.38</td>
      <td>70.63</td>
      <td>5</td>
      <td>0.29</td>
      <td>58.0</td>
      <td>58.0</td>
      <td>57.342389</td>
      <td>3325.85855</td>
      <td>822.101368</td>
      <td>411.050684</td>
    </tr>
    <tr>
      <th>1</th>
      <td>AGYS</td>
      <td>0.141400</td>
      <td>-1466.319459</td>
      <td>1.11</td>
      <td>0.08</td>
      <td>641.86</td>
      <td>176.47</td>
      <td>-0.11</td>
      <td>-28.11</td>
      <td>5</td>
      <td>...</td>
      <td>-0.11</td>
      <td>-28.11</td>
      <td>5</td>
      <td>176.47</td>
      <td>35294.0</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>2</th>
      <td>AMSC</td>
      <td>0.135766</td>
      <td>-0.814677</td>
      <td>-0.56</td>
      <td>-0.07</td>
      <td>-0.84</td>
      <td>-0.09</td>
      <td>0.08</td>
      <td>-2.04</td>
      <td>5</td>
      <td>...</td>
      <td>0.08</td>
      <td>-2.04</td>
      <td>5</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
  </tbody>
</table>
<p>3 rows × 26 columns</p>
</div>




```python
# create pandas profiling report of growth kpi dataset
create_pandas_profiling_report(growth_df, 'growth_df')
```


    Summarize dataset:   0%|          | 0/5 [00:00<?, ?it/s]



    Generate report structure:   0%|          | 0/1 [00:00<?, ?it/s]



    Render HTML:   0%|          | 0/1 [00:00<?, ?it/s]



    Export report to file:   0%|          | 0/1 [00:00<?, ?it/s]


    
    Pandas profiling report of file growth_df created
    
    



```python
# analysis of extreme values for margin of safety based on findings in profiling report
growth_df['mos'].describe(percentiles=[.03, .04, .05, .1, .2, .3, .4, .5, .6, .7, .8, .9, .95, .98, .99])
```




    count       15.000000
    mean      4501.978221
    std      17107.361232
    min          7.385125
    3%           8.006221
    4%           8.213253
    5%           8.420285
    10%          9.632734
    20%         19.114548
    30%         32.349187
    40%         35.103926
    50%         38.509094
    60%         58.976125
    70%        101.355129
    80%        144.168356
    90%        341.378396
    95%      20189.783863
    98%      47880.010314
    99%      57110.085798
    max      66340.161282
    Name: mos, dtype: float64




```python
# analysis of extreme cases: negative mos
# growth_df[growth_df['mos']<=1e-01].iloc[0]
```


    ---------------------------------------------------------------------------

    IndexError                                Traceback (most recent call last)

    /tmp/ipykernel_14694/379819996.py in <module>
          1 # analysis of extreme cases: negative mos
    ----> 2 growth_df[growth_df['mos']<=1e-01].iloc[0]
    

    ~/01_projects/10_py37_dataeng_venv/.venv/lib/python3.7/site-packages/pandas/core/indexing.py in __getitem__(self, key)
        929 
        930             maybe_callable = com.apply_if_callable(key, self.obj)
    --> 931             return self._getitem_axis(maybe_callable, axis=axis)
        932 
        933     def _is_scalar_access(self, key: tuple):


    ~/01_projects/10_py37_dataeng_venv/.venv/lib/python3.7/site-packages/pandas/core/indexing.py in _getitem_axis(self, key, axis)
       1564 
       1565             # validate the location
    -> 1566             self._validate_integer(key, axis)
       1567 
       1568             return self.obj._ixs(key, axis=axis)


    ~/01_projects/10_py37_dataeng_venv/.venv/lib/python3.7/site-packages/pandas/core/indexing.py in _validate_integer(self, key, axis)
       1498         len_axis = len(self.obj._get_axis(axis))
       1499         if key >= len_axis or key < -len_axis:
    -> 1500             raise IndexError("single positional indexer is out-of-bounds")
       1501 
       1502     # -------------------------------------------------------------------


    IndexError: single positional indexer is out-of-bounds



```python
# analyis of extreme cases: unrealistically high mos
# analysis of extreme cases: negative mos
# growth_df[growth_df['mos']>=(1.0e10)].iloc[0]
```


    ---------------------------------------------------------------------------

    IndexError                                Traceback (most recent call last)

    /tmp/ipykernel_14694/2404710621.py in <module>
          1 # analyis of extreme cases: unrealistically high mos
          2 # analysis of extreme cases: negative mos
    ----> 3 growth_df[growth_df['mos']>=(1.0e10)].iloc[0]
    

    ~/01_projects/10_py37_dataeng_venv/.venv/lib/python3.7/site-packages/pandas/core/indexing.py in __getitem__(self, key)
        929 
        930             maybe_callable = com.apply_if_callable(key, self.obj)
    --> 931             return self._getitem_axis(maybe_callable, axis=axis)
        932 
        933     def _is_scalar_access(self, key: tuple):


    ~/01_projects/10_py37_dataeng_venv/.venv/lib/python3.7/site-packages/pandas/core/indexing.py in _getitem_axis(self, key, axis)
       1564 
       1565             # validate the location
    -> 1566             self._validate_integer(key, axis)
       1567 
       1568             return self.obj._ixs(key, axis=axis)


    ~/01_projects/10_py37_dataeng_venv/.venv/lib/python3.7/site-packages/pandas/core/indexing.py in _validate_integer(self, key, axis)
       1498         len_axis = len(self.obj._get_axis(axis))
       1499         if key >= len_axis or key < -len_axis:
    -> 1500             raise IndexError("single positional indexer is out-of-bounds")
       1501 
       1502     # -------------------------------------------------------------------


    IndexError: single positional indexer is out-of-bounds



```python
# fundamental_df[fundamental_df['Ticker']=='FCPT']
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Ticker</th>
      <th>Report Date_is</th>
      <th>SimFinId</th>
      <th>Currency</th>
      <th>Fiscal Year</th>
      <th>Fiscal Period_is</th>
      <th>Publish Date_is</th>
      <th>Restated Date_is</th>
      <th>Shares (Basic)_is</th>
      <th>Shares (Diluted)_is</th>
      <th>...</th>
      <th>Retained Earnings</th>
      <th>Total Equity</th>
      <th>Total Liabilities &amp; Equity</th>
      <th>Dividends Paid_clean</th>
      <th>roic</th>
      <th>eps</th>
      <th>bvps</th>
      <th>fcf</th>
      <th>mean_low_price</th>
      <th>pe</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>5015</th>
      <td>FCPT</td>
      <td>2014-12-31</td>
      <td>623532</td>
      <td>USD</td>
      <td>2014</td>
      <td>FY</td>
      <td>2015-03-23</td>
      <td>2017-02-27</td>
      <td>6206375.0</td>
      <td>6263921.0</td>
      <td>...</td>
      <td>0.0</td>
      <td>8998000.0</td>
      <td>11949000</td>
      <td>0.0</td>
      <td>0.002678</td>
      <td>0.005156</td>
      <td>1.449800</td>
      <td>1016000.0</td>
      <td>NaN</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>5016</th>
      <td>FCPT</td>
      <td>2015-12-31</td>
      <td>623532</td>
      <td>USD</td>
      <td>2015</td>
      <td>FY</td>
      <td>2016-02-04</td>
      <td>2018-02-27</td>
      <td>6206375.0</td>
      <td>6263921.0</td>
      <td>...</td>
      <td>5257000.0</td>
      <td>441642000.0</td>
      <td>929437000</td>
      <td>0.0</td>
      <td>0.006132</td>
      <td>0.918249</td>
      <td>71.159413</td>
      <td>22249000.0</td>
      <td>21.840909</td>
      <td>23.785379</td>
    </tr>
    <tr>
      <th>5017</th>
      <td>FCPT</td>
      <td>2016-12-31</td>
      <td>623532</td>
      <td>USD</td>
      <td>2016</td>
      <td>FY</td>
      <td>2017-02-27</td>
      <td>2018-02-27</td>
      <td>56984561.0</td>
      <td>59568067.0</td>
      <td>...</td>
      <td>25943000.0</td>
      <td>470117000.0</td>
      <td>937151000</td>
      <td>-121604000.0</td>
      <td>0.037566</td>
      <td>2.751780</td>
      <td>8.249901</td>
      <td>130261000.0</td>
      <td>19.338095</td>
      <td>7.027485</td>
    </tr>
    <tr>
      <th>5018</th>
      <td>FCPT</td>
      <td>2017-12-31</td>
      <td>623532</td>
      <td>USD</td>
      <td>2017</td>
      <td>FY</td>
      <td>2018-02-27</td>
      <td>2018-02-27</td>
      <td>60627423.0</td>
      <td>60695834.0</td>
      <td>...</td>
      <td>36318000.0</td>
      <td>522268000.0</td>
      <td>1068659000</td>
      <td>-58695000.0</td>
      <td>0.011883</td>
      <td>1.177586</td>
      <td>8.614386</td>
      <td>154454000.0</td>
      <td>25.626500</td>
      <td>21.761894</td>
    </tr>
    <tr>
      <th>5019</th>
      <td>FCPT</td>
      <td>2018-12-31</td>
      <td>623532</td>
      <td>USD</td>
      <td>2018</td>
      <td>FY</td>
      <td>2019-02-20</td>
      <td>2019-02-20</td>
      <td>64041255.0</td>
      <td>64388929.0</td>
      <td>...</td>
      <td>46018000.0</td>
      <td>698964000.0</td>
      <td>1343098000</td>
      <td>-69494000.0</td>
      <td>0.009608</td>
      <td>1.286639</td>
      <td>10.914277</td>
      <td>327928000.0</td>
      <td>26.643158</td>
      <td>20.707557</td>
    </tr>
    <tr>
      <th>5020</th>
      <td>FCPT</td>
      <td>2019-12-31</td>
      <td>623532</td>
      <td>USD</td>
      <td>2019</td>
      <td>FY</td>
      <td>2020-02-27</td>
      <td>2020-02-27</td>
      <td>68430841.0</td>
      <td>68632010.0</td>
      <td>...</td>
      <td>38401000.0</td>
      <td>726741000.0</td>
      <td>1446070000</td>
      <td>-78488000.0</td>
      <td>-0.004061</td>
      <td>1.061159</td>
      <td>10.620080</td>
      <td>312026000.0</td>
      <td>27.397143</td>
      <td>25.818133</td>
    </tr>
  </tbody>
</table>
<p>6 rows × 85 columns</p>
</div>



### 2.4 Screener results
The processing of the screener results table is comparatively easy:
- the latest prices are extracted from the price data set
- the relevant columns for the intrinsic value from the growth kpi data set are joined.

From the Pandas Profiling report the following insights can be gathered:
- per May-14, 2021: 35 ticker symbols where identified which intrinsic value is below the latest market price.
- out of these 35 symbols three appear to have unrealistically high intrinsic values.


```python
%%time
# find stocks which latest price is under the mos
screener_df = find_stocks_below_mos(spark, price_df, growth_df)
```

    [Stage 35:======================================>                   (2 + 1) / 3]

    CPU times: user 224 ms, sys: 69.3 ms, total: 294 ms
    Wall time: 1min 39s


                                                                                    


```python
# create pandas profiling report of growth kpi dataset
create_pandas_profiling_report(screener_df, 'screener_df')
```


    Summarize dataset:   0%|          | 0/5 [00:00<?, ?it/s]



    Generate report structure:   0%|          | 0/1 [00:00<?, ?it/s]



    Render HTML:   0%|          | 0/1 [00:00<?, ?it/s]



    Export report to file:   0%|          | 0/1 [00:00<?, ?it/s]


    
    Pandas profiling report of file screener_df created
    
    



```python
# analysis of extreme values for margin of safety based on findings in profiling report
screener_df['mos'].describe(percentiles=[.05, .1, .2, .3, .4, .5, .6, .7, .8, .9, .95])
```




    count        3.000000
    mean     22329.360643
    std      38114.570893
    min        236.869963
    5%         254.288035
    10%        271.706107
    20%        306.542251
    30%        341.378396
    40%        376.214540
    50%        411.050684
    60%      13596.872804
    70%      26782.694923
    80%      39968.517043
    90%      53154.339162
    95%      59747.250222
    max      66340.161282
    Name: mos, dtype: float64




```python
screener_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Ticker</th>
      <th>last_date</th>
      <th>last_low_price</th>
      <th>price_future</th>
      <th>sticker_price</th>
      <th>mos</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>EA</td>
      <td>2021-06-04</td>
      <td>142.639999</td>
      <td>1916.542222</td>
      <td>473.739926</td>
      <td>236.869963</td>
    </tr>
    <tr>
      <th>1</th>
      <td>ABMD</td>
      <td>2021-06-04</td>
      <td>284.260010</td>
      <td>3325.858550</td>
      <td>822.101368</td>
      <td>411.050684</td>
    </tr>
    <tr>
      <th>2</th>
      <td>RH</td>
      <td>2021-06-04</td>
      <td>602.219971</td>
      <td>536765.905322</td>
      <td>132680.322563</td>
      <td>66340.161282</td>
    </tr>
  </tbody>
</table>
</div>



## Step 3: Define the Data Model
#### 3.1 Conceptual Data Model
Map out the conceptual data model and explain why you chose that model
- Dimensional tables
    - "company_info": company information, including peers
- Fact tables:
    - "price": historical stock price data (equal to staging data)
    - "fundamental": fundamental indicators from financial statements
    - "growth": rule #1 growth KPIs
    - "screener" screener results

This data model was chosen because it enables an analyst:
- to quickly analyize the screener results with the table "screener".
- to add context such as peer, industry, company name via the table "company_info".
- to deeper analyze the screener results via the table "growth".
- to enable backtesting of KPI based investment strategies via the table "price".

#### 3.2 Mapping Out Data Pipelines
List the steps necessary to pipeline the data into the chosen data model
- Data extraction pipeline: 
    - extract data from sources via APIs and store results in staging tables.
        - NASDAAQ: stock symbol list
        - IEX Cloud source:
            - Company information data
            - Peer group data
        - Simfin source: fundamental data
            - Annual cashflow data
            - Annual income statement data
            - Annual balance sheet data
        - Yfinance source: historical stock price data
    - transform: reduce the list of symbols to only those where the information is available on
        - company information data
        - fundamental data
        - price data
    - load: store data on disk
        - company information data: folder 1_company_info
        - fundamental data: folder 2_fundamentals
        - price data: folder 3_prices
- Data processing pipeline: process data from staging tables to dimension and fact tables.
    - extract:
        - load company information data, fundamental data and price data
    - transform:
        - create company information dimension table with symbol list.
        - create fundamental facts table
        - create growth KPI facts table
        - create screener results KPI table
    - load: store data on disk
        - save tables 'company_info', 'fundamental', 'growth', 'screener' to folder 5_results

## Step 4: Run Pipelines to Model the Data 
### 4.1 Create the data model
Build the data pipelines to create the data model.


```python
%%time
# download data from sources to staging folders
symbol_list = pipeline_staging(period='10y')
print('Number of symbols in list after staging pipeline ran: {}'.format(len(symbol_list)))
```

    Symbol data extracted...
    total number of symbols traded = 9030
    Number of stocks symbols in list: 4240
    Company data loaded from disk...
    Dataset "us-cashflow-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Dataset "us-income-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Dataset "us-balance-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Symbols with available fundamental data: 1713
    Combined all fundamental data from financial statements to one Dataframe.
    Start download of historic price data



```python
%%time
period_dict = {'start_date':2010,
                'end_date':2021}

price_df, company_info_df, fundamental_df, growth_df, screener_df = pipeline_processing(spark, period_dict, agg_func='median')

screener_df.head()
```

    Number of stocks symbols in list: 6368
    Company data loaded from disk...
    Dataset "us-cashflow-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Dataset "us-income-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Dataset "us-balance-annual" on disk (0 days old).
    - Loading from disk ... Done!
    Symbols with available fundamental data: 1957
    Combined all fundamental data from financial statements to one Dataframe.
    Calculated roic and added it to Dataframe
    Calculated eps and added it to Dataframe
    Calculated bvps and added it to Dataframe
    Calculated fcf and added it to Dataframe
    top5 KPIs added to fundamental data


                                                                                    

    Calculated KPI growth from year to year.
    Calculated 5 and 10 year growth rate


                                                                                    

    CPU times: user 4.17 s, sys: 451 ms, total: 4.62 s
    Wall time: 2min 14s





<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Ticker</th>
      <th>last_date</th>
      <th>last_low_price</th>
      <th>price_future</th>
      <th>sticker_price</th>
      <th>mos</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>REGN</td>
      <td>2021-06-04</td>
      <td>505.799988</td>
      <td>9238.164179</td>
      <td>2283.532898</td>
      <td>1141.766449</td>
    </tr>
    <tr>
      <th>1</th>
      <td>VRTX</td>
      <td>2021-06-04</td>
      <td>209.389999</td>
      <td>7607.924412</td>
      <td>1880.562560</td>
      <td>940.281280</td>
    </tr>
    <tr>
      <th>2</th>
      <td>STMP</td>
      <td>2021-06-04</td>
      <td>188.279999</td>
      <td>2031.299375</td>
      <td>502.106139</td>
      <td>251.053070</td>
    </tr>
    <tr>
      <th>3</th>
      <td>NFLX</td>
      <td>2021-06-04</td>
      <td>490.950012</td>
      <td>21128.776957</td>
      <td>5222.710523</td>
      <td>2611.355261</td>
    </tr>
    <tr>
      <th>4</th>
      <td>TPX</td>
      <td>2021-06-04</td>
      <td>37.900002</td>
      <td>844.466497</td>
      <td>208.739203</td>
      <td>104.369601</td>
    </tr>
  </tbody>
</table>
</div>



#### Showcase of potential further analysis by data analyst


```python
eval_df = screener_df.merge(company_info_df.rename(columns={'ticker':'Ticker'}),
                        how='left',
                        on='Ticker',
                        validate='1:1')
eval_df.head(2)
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>Ticker</th>
      <th>last_date</th>
      <th>last_low_price</th>
      <th>price_future</th>
      <th>sticker_price</th>
      <th>mos</th>
      <th>company name</th>
      <th>short name</th>
      <th>industry</th>
      <th>description</th>
      <th>...</th>
      <th>logo</th>
      <th>ceo</th>
      <th>exchange</th>
      <th>market cap</th>
      <th>sector</th>
      <th>tag 1</th>
      <th>tag 2</th>
      <th>tag 3</th>
      <th>peer_string</th>
      <th>peer_list</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>REGN</td>
      <td>2021-06-04</td>
      <td>505.799988</td>
      <td>9238.164179</td>
      <td>2283.532898</td>
      <td>1141.766449</td>
      <td>Regeneron Pharmaceuticals Inc.</td>
      <td>Regeneron Pharmaceuticals</td>
      <td>Biotechnology</td>
      <td>Regeneron Pharmaceuticals Inc is an integrated...</td>
      <td>...</td>
      <td>REGN.png</td>
      <td>Leonard S. Schleifer</td>
      <td>Nasdaq Global Select</td>
      <td>4.603378e+10</td>
      <td>Healthcare</td>
      <td>Healthcare</td>
      <td>Biotechnology</td>
      <td>NaN</td>
      <td>AMGN,ALNY,BIIB,JNJ,NVS,SNY</td>
      <td>[AMGN, ALNY, BIIB, JNJ, NVS, SNY]</td>
    </tr>
    <tr>
      <th>1</th>
      <td>VRTX</td>
      <td>2021-06-04</td>
      <td>209.389999</td>
      <td>7607.924412</td>
      <td>1880.562560</td>
      <td>940.281280</td>
      <td>Vertex Pharmaceuticals Incorporated</td>
      <td>Vertex Pharmaceuticals Incorporated</td>
      <td>Biotechnology</td>
      <td>Vertex Pharmaceuticals Inc is engaged in the b...</td>
      <td>...</td>
      <td>VRTX.png</td>
      <td>Jeffrey M Leiden</td>
      <td>Nasdaq Global Select</td>
      <td>4.783791e+10</td>
      <td>Healthcare</td>
      <td>Healthcare</td>
      <td>Biotechnology</td>
      <td>NaN</td>
      <td>GILD,ABBV,MRK,BMY,JNJ,ENZN,ENTA</td>
      <td>[GILD, ABBV, MRK, BMY, JNJ, ENZN, ENTA]</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 21 columns</p>
</div>




```python
eval_df.to_excel('../data/4_data_analysis/' + str(pd.to_datetime('today'))[:10] + '_eval_df.xlsx', index=False)
```

#### 4.2 Data Quality Checks
Explain the data quality checks you'll perform to ensure the pipeline ran as expected. These could include:
 * Integrity constraints on the relational database (e.g., unique key, data type, etc.)
 * Unit tests for the scripts to ensure they are doing the right thing
 * Source/Count checks to ensure completeness
 
Run Quality Checks

Several data quality checks have already been included in the data pipeline, e.g.:
- merge operations: validate argument always set to interrupt pipeline in case of unexpected entries (e.g. duplicates).
- print out of ticker symbol count at various stages of the data pipelines. This is an indicator or data availability.

Additional data quality checks that are done here:
- check data "freshness": check latest data in fundamental and price tables.
- check processing pipeline functionality: re-check count of symbols in tables 'price', 'company_info', 'fundamental', 'growth', 'screener' to ensure they match with print-outs in processing pipeline.


```python
%%time
# Perform quality checks for freshness
# print latest date in price table
print('Date range in price table:')
price_df.select(F.min('Date'),F.max('Date')).show()
# print latest date in fundamental table
print('Date range in fundamental table:')
print('min(Fiscal Year): {}'.format(fundamental_df['Fiscal Year'].min()))
print('max(Fiscal Year): {}'.format(fundamental_df['Fiscal Year'].max()))
```

    Date range in price table:


    [Stage 43:======================================================> (59 + 2) / 61]

    +----------+----------+
    | min(Date)| max(Date)|
    +----------+----------+
    |1962-01-02|2022-02-25|
    +----------+----------+
    
    Date range in fundamental table:
    min(Fiscal Year): 2015
    max(Fiscal Year): 2020
    CPU times: user 64.4 ms, sys: 69.2 ms, total: 134 ms
    Wall time: 30.2 s


                                                                                    


```python
%%time
# Perform data quality check for processing pipeline functionality
price_symbol_list = price_df.select('Ticker').distinct().toPandas()['Ticker'].tolist()
price_symbol_list_cnt = len(price_symbol_list)
print(f'Count of symbols in table price: {price_symbol_list_cnt}')
for df, df_name in zip([company_info_df.rename(columns={'ticker':'Ticker'}), fundamental_df, growth_df, screener_df],
                        ['company_info', 'fundamental', 'growth', 'screener']):
    print('Count of symbols in table {}: {}'.format(df_name, df['Ticker'].nunique()))

```

    [Stage 46:=====================================================>  (58 + 2) / 61]

    Count of symbols in table price: 1776
    Count of symbols in table company_info: 6368
    Count of symbols in table fundamental: 1724
    Count of symbols in table growth: 1412
    Count of symbols in table screener: 28
    CPU times: user 112 ms, sys: 9.46 ms, total: 121 ms
    Wall time: 14 s


                                                                                    


```python
# filter for verification of mean calculation
# price_df.filter(
#                     (price_df['Date']>=F.to_date(F.lit('2020-12-01'))) &\
#                     (price_df['Date']<=F.to_date(F.lit('2020-12-31'))) &\
#                     (price_df['Ticker']=='AAPL')
#                 ).toPandas()['Low'].mean()
```

#### 4.3 Data dictionary 
Create a data dictionary for your data model. For each field, provide a brief description of what the data is and where it came from. You can include the data dictionary in the notebook or in a separate file.


```python
# read-in data dictionary from csv
results_data_dict_df = pd.read_csv('..//data//5_results//' + 'results_data_dict_tab' + '.txt', sep='\t')
results_data_dict_df.head()
```




<div>
<style scoped>
    .dataframe tbody tr th:only-of-type {
        vertical-align: middle;
    }

    .dataframe tbody tr th {
        vertical-align: top;
    }

    .dataframe thead th {
        text-align: right;
    }
</style>
<table border="1" class="dataframe">
  <thead>
    <tr style="text-align: right;">
      <th></th>
      <th>table</th>
      <th>column</th>
      <th>dtype</th>
      <th>source</th>
      <th>description</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>0</th>
      <td>fundamental</td>
      <td>Ticker</td>
      <td>object</td>
      <td>SimFin API</td>
      <td>primary key. Symbol to identify stock.</td>
    </tr>
    <tr>
      <th>1</th>
      <td>fundamental</td>
      <td>Report Date_is</td>
      <td>datetime64[ns]</td>
      <td>SimFin API</td>
      <td>primary key. Date of annual financial report</td>
    </tr>
    <tr>
      <th>2</th>
      <td>fundamental</td>
      <td>SimFinId</td>
      <td>int64</td>
      <td>SimFin API</td>
      <td>ID assigned to Stock within SimFin API</td>
    </tr>
    <tr>
      <th>3</th>
      <td>fundamental</td>
      <td>Currency</td>
      <td>object</td>
      <td>SimFin API</td>
      <td>Currency in which stock is traded.</td>
    </tr>
    <tr>
      <th>4</th>
      <td>fundamental</td>
      <td>Fiscal Year</td>
      <td>int64</td>
      <td>SimFin API</td>
      <td>Year of report.</td>
    </tr>
  </tbody>
</table>
</div>



#### Step 5: Complete Project Write Up
* Clearly state the rationale for the choice of tools and technologies for the project:
    - staging pipeline: since all providers chosen for this project offer a python API the first pipeline for source data extraction is purely realized via python shell scripts.
    - processing pipeline: since the amount of data to be processed is quite largely (ca. 10 million records on historical stock pices), the second data pipeline uses a combination of python and PySpark scripts.
* Propose how often the data should be updated and why:
    - growth KPI table generation: once a year, since fundamental data is published annually.
    - screener table generation: daily, since changing market prices result in different daily results.
* Write a description of how you would approach the problem differently under the following scenarios:
 * The data was increased by 100x:
    - in general: at this scale, the pipelines could no longer be run from a local computer. Instead they should be rebuild on cloud infrastructure, e.g. AWS. One possibility would be to the use the AWS GLUE services for this purpose.
    - staging pipeline:
        - if API technology remains the same, request could be parallelized via python shell scripts.
    - processing pipeline:
        - switch to Pyspark instead of Python scripts for all processing.
 * The data populates a dashboard that must be updated on a daily basis by 7am every day.
    - also in this scenario I would recommend to rebuild the pipeline on cloud infrastructure.
    - Again AWS offers various services for this purpose, e.g. AWS Glue workflows.
 * The database needed to be accessed by 100+ people:
    - also in this case I would recommend to switch to cloud infrastructure.
    - the tables could be saved to AWS S3 and make accessible as a database via the service AWS Athena.
    - AWS Athena


```python
# remove old documentation
try:
    os.remove('README.md')
except:
    print('no README.md file found')
# export notebook to markdown for documentation
!jupyter nbconvert --to markdown capstone_stockscreener.ipynb
# rename markdown file to README.md
os.rename('capstone_stockscreener.md', 'README.md')
# move readme to top folder
shutil.move('README.md', '../README.md')
```

    no README.md file found
    [NbConvertApp] Converting notebook capstone_stockscreener.ipynb to markdown
    [NbConvertApp] Writing 73844 bytes to capstone_stockscreener.md





    '../README.md'


