**COURSE MGMTMFE 431 LEC 3**
**QUANTITATIVE ASSET MANAGEMENT**

# Problem Set 1

In this project, we attempt to replicate the monthly market return time series available in Kenneth French website by constructing the value-weighted market return using CRSP data between July-1926 to December 2023.

Link to CRPS data website at WRDS and Kenneth French website are given below:

- https://wrds-www.wharton.upenn.edu/pages/about/data-vendors/center-for-research-in-security-prices-crsp/
- https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html

## Step 1 - Download and save raw CRSP data and Fama/French 3 Factor data

- Create a WRDS account to download CRSP data
- Install wrds package in your python environment
- Please refer to following link for sample code for how to connect and download data from WRDS:
    https://wrds-www.wharton.upenn.edu/documents/1443/wrds_connection.html

### Download CRSP monthly returns

- Download following fields from monthly return table “crspq.msf”:
        permno, permco, date, ret, retx, shrout, prc, cfacshr and cfacpr
- Download following fields from security names table “crspq.msenames”:
        permno, date, shrcd, exchcd
- While running the sql query to download the data use left join on permno and date
- Download full dataset available on WRDS; do not pre-filter by SHRCD, EXCHCD, or date
- Store downloaded data in pickle format

### Download CRSP monthly delisting returns

- Use following fields from monthly delisting return table “crspq.msedelist”:
        permno, dlret, dlstdt, dlstcd
- Store downloaded data in pickle format

### Download Fama/French 3 Factor monthly data

- Download Fama/French 3 Factors monthly data from:
        https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/data_library.html
- We need Date, Market_minus_Rf and Rf columns from this dataset
- Store downloaded data in pickle format

## Step 2 - Processes the saved CRSP raw returns and delisted returns data to create a merged dataframe

### Process raw CRSP returns
   
- Sometimes there is redundant indices in the downloaded data so sort the data by permno and date and reset the index
- We need to store 'permno', 'permco', 'shrcd', 'exchcd' and 'shrout' columns as integers and nan values lead to exceptions, so drop rows with missing 'shrcd', 'exchcd' and 'shrout' values
- Change the datatype of the above mentioned columns to integer
- Format the date column as datetime
- Again sort the data by permno and date and reset index because we dropped rows above

### Process raw CRSP delisting return

- Sometimes there is redundant indices in the downloaded data so sort the data by permno and 'dlstdt' and reset the index
- Change the datatype of the 'permno' columns to integer
- Rename 'dlstdt' column  to ‘date’ and format it as datetime

### Merge CRSP returns with the delisting returns

- To keep all the values use outer join and join on date and permno
- Store merged CRSP data in pickle format

## Step 3 – Calculate value-weighted return, equal-weighted return and lagged total market cap using the merged data

### Filter unrequired data

- Kenneth R. French uses value-weight return of all CRSP firms incorporated in the US and listed on the NYSE, AMEX, or NASDAQ that have a CRSP share code of 10 or 11
- The above mentioned exchanges have exchange-code (exchcd) 1, 2 and 3, so we need to drop all the other rows
- Similarly, we need to drop the rows which don’t have share-code (shrcd) 10 or 11
- There is a complexity while filtering exchcd/shrcd, exchcd/shrcd are nan for delisted return rows, so filtering rows on required exchcd/shrcd removes all the delisted return rows
- But dlstcd is not-nan for all the delisted return rows, so use it as a proxy to identify delisted return rows and drop unrequired exchcd/shrcd rows only if those are non delisted return rows
- Drop all the rows outside January 1926 to December 2023 timeframe
- Move all the dates to last day of the month
- Again sort the data by permno and date and reset index because we dropped rows above

### Calculate market equity (in USD Billions)

- In CRSP data, bid/ask average prices are used as a proxy for trading prices when trading prices are not available but a negative sign is put in front such prices to indicate so
- To handle this use absolute price ('prc') multiplied by outstanding shares ('shrout') to compute market equity and divide it by a million to convert it to billions

### Adjust for Delisting Returns

To incorporate delisting returns, we use compounded return if both return and delisted return are available. If any one of them is not available but the other one is, we use the other one i.e. 

$
r_{i,t} = 
\begin{cases} 
(1 + r_{i,t}^h)(1 + r_{i,t}^d) - 1 \,\,\,if\, both\, r_{i,t}^h\, and\, r_{i,t}^d\, are\, available\\
r_{i,t}^d                          \,\,\,if\, r_{i,t}^h\, is\, not\, available\\
r_{i,t}^h                          \,\,\,if\, r_{i,t}^d\, is\, not\, available
\end{cases}
$

After this if any ret values are still missing we drop them and sort/reset index.

    
### Find Cumulative Market Cap
    
A given 'permco' has multiple 'permno', so to find the correct market cap, we use below steps:
  
- For a given date and permco, we sum market cap ('me') values across different permno to find cumulative market cap for the permco    
- For a given date and permco, among multiple market-caps for different permno, we find the largest one  
- We merge Merge CRSP_Stocks and CRSP_Stocks_ME_MAX on 'date', 'permco' and 'me' to find correct 'permno'
- Replace market cap ('me') with cumulative market cap
- Sort by permno and date and drop duplicates
  

### Lagged market cap

- In order to have a portfolio which is not forward looking, we need to use lagged market cap, otherwise it would lead to artifical performance gain. So, at a given time t, we use market cap at t-1.
- If a permno is the first permno, it will have a nan value, so we replace the missing value by me/(1+retx)
- After this is we drop all the rows with missing lagged market cap and reset index
    
### Data integrity checkes

- CRSP uses missing codes -66, -77, -88 and -99, so make sure none of the returns have these values
- Also, make sure that non of the return or lagged market cap are nan

### Compute required values

- For a given month, we sum across lagged market caps to find lagged total market cap i.e 'Stock_lag_MV' series
- For a given month, we find simple mean of returns to find equal-weighted return i.e. 'Stock_Ew_Ret' series
- For a given month, we find lagged market cap-weighted returns to find  value-weighted return i.e 'Stock_Vw_Ret' series


## Setp 4 - Comparing Estimated FF Market Excess Return with Actual FF Market Excess Return
    
- Use 'Rf' series from Fama/French 3 Factor data and subtract it from 'Stock_Vw_Ret' series to find Estimated FF Market Excess Return
- Use 'Market_minus_Rf' Fama/French 3 Factor data as Actual FF Market Excess Return
- Compute montly Mean and Standard deviation for both the series
- Compute annualised Mean and Standard Deviation by multiplying them with 12 and $\sqrt{12}$ repectively
- Compute annualised Sharpe Ratio by dividing annualised Mean with annualised Standard Deviation
- Compute Skewness and Excess Kurtosis for both the series
    
The results thus obtained are shown below:

![q2.jpg](attachment:q2.jpg)

## Setp 5 - Compute Correlation and maximum absolute difference between the two time series

The results are shown below:

The correlation between the two time series: 0.99998757

The maximum absolute difference between the two time series: 0.00275303

The correleation is matching upto 4th decimal place and the maximum absolute difference between the two time series is also very small. Given the deviation is very small the difference is economically negligible.

We made a lot of different choices during the replication and we do not have the information about how Famma-French handled the same, so the discrepancy could be because of those reasons.

All such choices are listed below:

- Dropped missing 'shrcd', 'exchcd' and 'shrout'
- 'outer' join for merging non-desliting returns with delisted returns
- Used a specific way to handle delisting return conflict while filtering exchcd/shrcd
- Delisting return handling
- Missing return handling
- Replacing negative price ('prc') with it's absolute values, while computing market cap
- Handling of 'permno' and 'permco' to find market-cap
- Handling of missing lagged market-cap values
- Other joining choices we made while merging dataframes
- How delisting reason was used to modify returns
- Effect of cfacshr and cfacpr
