# Stock screener result evaluation

## Imports


```python
# Do all imports and installs here
import pandas as pd
import numpy as np
from analysis_utils import read_result_tables
```

## Extract screener results data


```python
company_info_df, fundamental_df, growth_df, screener_df = read_result_tables()
```

## Data Exploration


```python
# find extreme mos calculation values which are unlikely to achieve
extremes_df = screener_df[(screener_df.mos/ screener_df.last_low_price)>10]
extremes_df
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
      <th>1</th>
      <td>SABR</td>
      <td>2021-05-12</td>
      <td>12.210000</td>
      <td>1.193005e+04</td>
      <td>2.948925e+03</td>
      <td>1.474463e+03</td>
    </tr>
    <tr>
      <th>6</th>
      <td>HSY</td>
      <td>2021-05-13</td>
      <td>167.100006</td>
      <td>5.406446e+04</td>
      <td>1.336391e+04</td>
      <td>6.681954e+03</td>
    </tr>
    <tr>
      <th>12</th>
      <td>RH</td>
      <td>2021-05-12</td>
      <td>616.809998</td>
      <td>2.422068e+05</td>
      <td>5.986982e+04</td>
      <td>2.993491e+04</td>
    </tr>
    <tr>
      <th>14</th>
      <td>JYNT</td>
      <td>2021-05-13</td>
      <td>49.790001</td>
      <td>1.368686e+04</td>
      <td>3.383182e+03</td>
      <td>1.691591e+03</td>
    </tr>
    <tr>
      <th>15</th>
      <td>CCOI</td>
      <td>2021-05-13</td>
      <td>74.800003</td>
      <td>4.395772e+04</td>
      <td>1.086568e+04</td>
      <td>5.432838e+03</td>
    </tr>
    <tr>
      <th>17</th>
      <td>TNET</td>
      <td>2021-05-13</td>
      <td>76.820000</td>
      <td>1.846708e+04</td>
      <td>4.564781e+03</td>
      <td>2.282390e+03</td>
    </tr>
    <tr>
      <th>18</th>
      <td>UAL</td>
      <td>2021-05-13</td>
      <td>51.299999</td>
      <td>1.724174e+04</td>
      <td>4.261894e+03</td>
      <td>2.130947e+03</td>
    </tr>
    <tr>
      <th>23</th>
      <td>IT</td>
      <td>2021-05-13</td>
      <td>224.149994</td>
      <td>1.035805e+08</td>
      <td>2.560351e+07</td>
      <td>1.280175e+07</td>
    </tr>
    <tr>
      <th>24</th>
      <td>RDNT</td>
      <td>2021-05-12</td>
      <td>22.680000</td>
      <td>2.213361e+03</td>
      <td>5.471089e+02</td>
      <td>2.735545e+02</td>
    </tr>
    <tr>
      <th>25</th>
      <td>FCPT</td>
      <td>2021-05-13</td>
      <td>25.709999</td>
      <td>3.425919e+11</td>
      <td>8.468348e+10</td>
      <td>4.234174e+10</td>
    </tr>
  </tbody>
</table>
</div>




```python
growth_df[growth_df.Ticker.isin(extremes_df.Ticker.unique())].iloc[:,0:20]
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
      <th>rule1_gr_5yr</th>
      <th>pe_default_5yr</th>
      <th>roic_gr_10yr</th>
      <th>revenue_gr_10yr</th>
      <th>eps_gr_10yr</th>
      <th>bvps_gr_10yr</th>
      <th>fcf_gr_10yr</th>
      <th>pe_10yr</th>
      <th>yrs_in_10yr</th>
      <th>rule1_gr_10yr</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <th>260</th>
      <td>CCOI</td>
      <td>0.049916</td>
      <td>0.823849</td>
      <td>0.06</td>
      <td>0.08</td>
      <td>2.18</td>
      <td>0.79</td>
      <td>0.09</td>
      <td>184.04</td>
      <td>5.0</td>
      <td>0.79</td>
      <td>158.0</td>
      <td>-0.24</td>
      <td>0.08</td>
      <td>0.48</td>
      <td>0.40</td>
      <td>0.06</td>
      <td>362.30</td>
      <td>10</td>
      <td>0.40</td>
    </tr>
    <tr>
      <th>518</th>
      <td>FCPT</td>
      <td>0.115557</td>
      <td>1.061159</td>
      <td>0.82</td>
      <td>0.77</td>
      <td>35.69</td>
      <td>9.50</td>
      <td>5.40</td>
      <td>19.82</td>
      <td>5.0</td>
      <td>9.50</td>
      <td>1900.0</td>
      <td>0.82</td>
      <td>0.77</td>
      <td>35.69</td>
      <td>9.50</td>
      <td>5.40</td>
      <td>19.82</td>
      <td>6</td>
      <td>9.50</td>
    </tr>
    <tr>
      <th>666</th>
      <td>HSY</td>
      <td>0.025052</td>
      <td>18.967433</td>
      <td>0.92</td>
      <td>0.01</td>
      <td>0.68</td>
      <td>0.66</td>
      <td>0.14</td>
      <td>17.94</td>
      <td>5.0</td>
      <td>0.66</td>
      <td>132.0</td>
      <td>0.56</td>
      <td>0.04</td>
      <td>0.45</td>
      <td>0.44</td>
      <td>0.15</td>
      <td>20.95</td>
      <td>10</td>
      <td>0.44</td>
    </tr>
    <tr>
      <th>723</th>
      <td>IT</td>
      <td>0.067883</td>
      <td>2.597392</td>
      <td>8.47</td>
      <td>0.16</td>
      <td>7.09</td>
      <td>2.14</td>
      <td>2.36</td>
      <td>702.82</td>
      <td>5.0</td>
      <td>2.14</td>
      <td>428.0</td>
      <td>4.74</td>
      <td>0.14</td>
      <td>4.03</td>
      <td>1.23</td>
      <td>1.41</td>
      <td>367.25</td>
      <td>10</td>
      <td>1.23</td>
    </tr>
    <tr>
      <th>752</th>
      <td>JYNT</td>
      <td>0.524128</td>
      <td>0.240515</td>
      <td>1.61</td>
      <td>0.49</td>
      <td>2.22</td>
      <td>0.91</td>
      <td>3.80</td>
      <td>88.07</td>
      <td>5.0</td>
      <td>0.91</td>
      <td>182.0</td>
      <td>0.06</td>
      <td>0.44</td>
      <td>-1.48</td>
      <td>-1.49</td>
      <td>3.42</td>
      <td>71.54</td>
      <td>7</td>
      <td>NaN</td>
    </tr>
    <tr>
      <th>1131</th>
      <td>RDNT</td>
      <td>0.183596</td>
      <td>0.297052</td>
      <td>95.56</td>
      <td>0.10</td>
      <td>118.87</td>
      <td>0.53</td>
      <td>0.15</td>
      <td>1763.52</td>
      <td>5.0</td>
      <td>0.53</td>
      <td>106.0</td>
      <td>52.92</td>
      <td>0.09</td>
      <td>66.52</td>
      <td>0.13</td>
      <td>0.05</td>
      <td>910.71</td>
      <td>10</td>
      <td>0.13</td>
    </tr>
    <tr>
      <th>1146</th>
      <td>RH</td>
      <td>0.056586</td>
      <td>11.548658</td>
      <td>9.43</td>
      <td>0.07</td>
      <td>16.96</td>
      <td>0.66</td>
      <td>0.12</td>
      <td>304.07</td>
      <td>5.0</td>
      <td>0.66</td>
      <td>132.0</td>
      <td>5.75</td>
      <td>0.14</td>
      <td>10.58</td>
      <td>0.57</td>
      <td>0.53</td>
      <td>200.03</td>
      <td>9</td>
      <td>0.57</td>
    </tr>
    <tr>
      <th>1173</th>
      <td>SABR</td>
      <td>0.027937</td>
      <td>0.578448</td>
      <td>1.01</td>
      <td>0.09</td>
      <td>1.04</td>
      <td>0.96</td>
      <td>0.11</td>
      <td>24.65</td>
      <td>5.0</td>
      <td>0.96</td>
      <td>192.0</td>
      <td>0.56</td>
      <td>0.08</td>
      <td>0.61</td>
      <td>0.60</td>
      <td>0.16</td>
      <td>31.62</td>
      <td>7</td>
      <td>0.60</td>
    </tr>
    <tr>
      <th>1325</th>
      <td>TNET</td>
      <td>0.100771</td>
      <td>3.028571</td>
      <td>0.74</td>
      <td>0.12</td>
      <td>0.74</td>
      <td>1.62</td>
      <td>1.40</td>
      <td>23.97</td>
      <td>5.0</td>
      <td>0.74</td>
      <td>148.0</td>
      <td>0.57</td>
      <td>0.16</td>
      <td>0.49</td>
      <td>1.19</td>
      <td>1.11</td>
      <td>38.22</td>
      <td>7</td>
      <td>0.49</td>
    </tr>
    <tr>
      <th>1375</th>
      <td>UAL</td>
      <td>0.047357</td>
      <td>11.626739</td>
      <td>0.83</td>
      <td>0.02</td>
      <td>1.08</td>
      <td>0.68</td>
      <td>0.22</td>
      <td>8.28</td>
      <td>5.0</td>
      <td>0.68</td>
      <td>136.0</td>
      <td>0.44</td>
      <td>0.08</td>
      <td>0.41</td>
      <td>0.77</td>
      <td>-0.28</td>
      <td>10.21</td>
      <td>10</td>
      <td>0.41</td>
    </tr>
  </tbody>
</table>
</div>




```python
fundamental_df[fundamental_df.Ticker=='RDNT'].sort_values('Fiscal Year')
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
      <th>10844</th>
      <td>RDNT</td>
      <td>2010-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2010</td>
      <td>FY</td>
      <td>2011-03-11</td>
      <td>2013-03-18</td>
      <td>36853477.0</td>
      <td>36853477.0</td>
      <td>...</td>
      <td>-242841000.0</td>
      <td>-82473000.0</td>
      <td>539514000</td>
      <td>0.0</td>
      <td>-0.023821</td>
      <td>-0.348732</td>
      <td>-2.237862</td>
      <td>168266000.0</td>
      <td>2.558636</td>
      <td>-7.336963</td>
    </tr>
    <tr>
      <th>10845</th>
      <td>RDNT</td>
      <td>2011-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2011</td>
      <td>FY</td>
      <td>2012-03-13</td>
      <td>2014-03-17</td>
      <td>37367736.0</td>
      <td>38785675.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>33099000.0</td>
      <td>43900000</td>
      <td>0.0</td>
      <td>0.164715</td>
      <td>0.193509</td>
      <td>0.885764</td>
      <td>145375000.0</td>
      <td>2.159048</td>
      <td>11.157339</td>
    </tr>
    <tr>
      <th>10846</th>
      <td>RDNT</td>
      <td>2012-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2012</td>
      <td>FY</td>
      <td>2013-02-13</td>
      <td>2015-03-16</td>
      <td>37751170.0</td>
      <td>39244686.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>35970000.0</td>
      <td>42103000</td>
      <td>0.0</td>
      <td>1.421134</td>
      <td>1.584957</td>
      <td>0.952818</td>
      <td>162317000.0</td>
      <td>2.405300</td>
      <td>1.517580</td>
    </tr>
    <tr>
      <th>10847</th>
      <td>RDNT</td>
      <td>2013-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2013</td>
      <td>FY</td>
      <td>2014-03-17</td>
      <td>2016-03-15</td>
      <td>39140480.0</td>
      <td>39814535.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>35012000.0</td>
      <td>40652000</td>
      <td>0.0</td>
      <td>0.052150</td>
      <td>0.054164</td>
      <td>0.894521</td>
      <td>117082000.0</td>
      <td>1.708381</td>
      <td>31.540967</td>
    </tr>
    <tr>
      <th>10848</th>
      <td>RDNT</td>
      <td>2014-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2014</td>
      <td>FY</td>
      <td>2015-03-16</td>
      <td>2017-03-17</td>
      <td>41070077.0</td>
      <td>43149196.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>33176000.0</td>
      <td>37316000</td>
      <td>0.0</td>
      <td>0.036874</td>
      <td>0.033504</td>
      <td>0.807790</td>
      <td>114646000.0</td>
      <td>8.464454</td>
      <td>252.642293</td>
    </tr>
    <tr>
      <th>10849</th>
      <td>RDNT</td>
      <td>2015-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2015</td>
      <td>FY</td>
      <td>2016-03-15</td>
      <td>2017-03-17</td>
      <td>43805794.0</td>
      <td>45171372.0</td>
      <td>...</td>
      <td>NaN</td>
      <td>32790000.0</td>
      <td>35494000</td>
      <td>0.0</td>
      <td>0.217192</td>
      <td>0.175981</td>
      <td>0.748531</td>
      <td>163833000.0</td>
      <td>6.055727</td>
      <td>34.411200</td>
    </tr>
    <tr>
      <th>10850</th>
      <td>RDNT</td>
      <td>2016-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2016</td>
      <td>FY</td>
      <td>2017-03-17</td>
      <td>2017-03-17</td>
      <td>46244188.0</td>
      <td>46655032.0</td>
      <td>...</td>
      <td>-150211000.0</td>
      <td>52053000.0</td>
      <td>849476000</td>
      <td>0.0</td>
      <td>0.008511</td>
      <td>0.156344</td>
      <td>1.125612</td>
      <td>157131000.0</td>
      <td>6.189524</td>
      <td>39.589143</td>
    </tr>
    <tr>
      <th>10851</th>
      <td>RDNT</td>
      <td>2017-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2017</td>
      <td>FY</td>
      <td>2018-03-19</td>
      <td>2018-03-19</td>
      <td>46880775.0</td>
      <td>47401921.0</td>
      <td>...</td>
      <td>-150158000.0</td>
      <td>69925000.0</td>
      <td>868979000</td>
      <td>0.0</td>
      <td>0.000061</td>
      <td>0.001131</td>
      <td>1.491550</td>
      <td>221545000.0</td>
      <td>9.792250</td>
      <td>8661.665422</td>
    </tr>
    <tr>
      <th>10852</th>
      <td>RDNT</td>
      <td>2018-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2018</td>
      <td>FY</td>
      <td>2019-03-18</td>
      <td>2019-03-18</td>
      <td>48114275.0</td>
      <td>48678999.0</td>
      <td>...</td>
      <td>-117915000.0</td>
      <td>200253000.0</td>
      <td>1109330000</td>
      <td>0.0</td>
      <td>0.029065</td>
      <td>0.670134</td>
      <td>4.162029</td>
      <td>263503000.0</td>
      <td>11.449211</td>
      <td>17.084963</td>
    </tr>
    <tr>
      <th>10853</th>
      <td>RDNT</td>
      <td>2019-12-31</td>
      <td>143984</td>
      <td>USD</td>
      <td>2019</td>
      <td>FY</td>
      <td>2020-03-16</td>
      <td>2020-03-16</td>
      <td>49674858.0</td>
      <td>50244006.0</td>
      <td>...</td>
      <td>-103159000.0</td>
      <td>233139000.0</td>
      <td>1646986000</td>
      <td>0.0</td>
      <td>0.008959</td>
      <td>0.297052</td>
      <td>4.693300</td>
      <td>203787000.0</td>
      <td>19.259048</td>
      <td>64.833997</td>
    </tr>
  </tbody>
</table>
<p>10 rows × 85 columns</p>
</div>



## Data Transformation


```python

```


```python
# merge company info to growth kpi dataset
growth_eval_df = growth_df.merge(company_info_df.rename(columns={'ticker':'Ticker'}),
                        how='left',
                        on='Ticker',
                        validate='1:1')
growth_eval_df.head(2)
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
      <td>0.050672</td>
      <td>3.410828</td>
      <td>0.12</td>
      <td>0.05</td>
      <td>0.18</td>
      <td>0.04</td>
      <td>0.16</td>
      <td>31.95</td>
      <td>5.0</td>
      <td>...</td>
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
      <td>-0.221592</td>
      <td>-6.081081</td>
      <td>-1.03</td>
      <td>-0.01</td>
      <td>-1.04</td>
      <td>-0.16</td>
      <td>-0.07</td>
      <td>-3.39</td>
      <td>5.0</td>
      <td>...</td>
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
  </tbody>
</table>
<p>2 rows × 41 columns</p>
</div>




```python
screener_eval_df = screener_df.merge(company_info_df.rename(columns={'ticker':'Ticker'}),
                        how='left',
                        on='Ticker',
                        validate='1:1')
screener_eval_df.head(2)
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
      <td>FIZZ</td>
      <td>2021-06-04</td>
      <td>49.400002</td>
      <td>493.871839</td>
      <td>122.077565</td>
      <td>61.038783</td>
      <td>National Beverage Corp.</td>
      <td>National Beverage</td>
      <td>Beverages - Non-Alcoholic</td>
      <td>National Beverage Corp is engaged in the devel...</td>
      <td>...</td>
      <td>None</td>
      <td>Nick A. Caporella</td>
      <td>Nasdaq Global Select</td>
      <td>3.922616e+09</td>
      <td>Consumer Defensive</td>
      <td>Consumer Defensive</td>
      <td>Beverages - Soft Drinks</td>
      <td>Beverages - Non-Alcoholic</td>
      <td>COKE,MNST,KO,PEP,REED,JSDA</td>
      <td>[COKE, MNST, KO, PEP, REED, JSDA]</td>
    </tr>
    <tr>
      <th>1</th>
      <td>VZ</td>
      <td>2021-06-04</td>
      <td>56.900002</td>
      <td>522.162754</td>
      <td>129.070647</td>
      <td>64.535323</td>
      <td>Verizon Communications Inc.</td>
      <td>Verizon Communications</td>
      <td>Communication Services</td>
      <td>Verizon Communications Inc is a provider of co...</td>
      <td>...</td>
      <td>VZ.png</td>
      <td>Hans Erik Vestberg</td>
      <td>New York Stock Exchange</td>
      <td>2.235627e+11</td>
      <td>Communication Services</td>
      <td>Communication Services</td>
      <td>Telecom Services</td>
      <td>None</td>
      <td>T,TMUS,SHEN,DTEGY,TDS,USM,VOD,SPOK,GOOGL,AAPL</td>
      <td>[T, TMUS, SHEN, DTEGY, TDS, USM, VOD, SPOK, GO...</td>
    </tr>
  </tbody>
</table>
<p>2 rows × 21 columns</p>
</div>




```python
fundamental_eval_df = fundamental_df.merge(company_info_df.rename(columns={'ticker':'Ticker'}),
                        how='left',
                        on='Ticker',
                        validate='m:1')
fundamental_eval_df.head(2)
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
      <td>2010-10-31</td>
      <td>45846</td>
      <td>USD</td>
      <td>2010</td>
      <td>FY</td>
      <td>2010-12-20</td>
      <td>2012-12-20</td>
      <td>347000000.0</td>
      <td>353000000.0</td>
      <td>...</td>
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
      <td>A</td>
      <td>2011-10-31</td>
      <td>45846</td>
      <td>USD</td>
      <td>2011</td>
      <td>FY</td>
      <td>2011-12-16</td>
      <td>2013-12-19</td>
      <td>347000000.0</td>
      <td>355000000.0</td>
      <td>...</td>
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
  </tbody>
</table>
<p>2 rows × 100 columns</p>
</div>



## Load analysis results to disk


```python
growth_eval_df.to_excel('../data/4_data_analysis/' + str(pd.to_datetime('today'))[:10] + '_growth_eval_df.xlsx', index=False)
```


```python
screener_eval_df.to_excel('../data/4_data_analysis/' + str(pd.to_datetime('today'))[:10] + '_screener_eval_df.xlsx', index=False)
```


```python
fundamental_eval_df.to_excel('../data/4_data_analysis/' + str(pd.to_datetime('today'))[:10] + '_fundamental_eval_df.xlsx', index=False)
```

## Document notebook as markdown file


```python
# export notebook to markdown for documentation
!jupyter nbconvert --to markdown result_evaluation.ipynb
```
