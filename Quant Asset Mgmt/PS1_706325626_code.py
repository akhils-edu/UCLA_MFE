# MGMTMFE 431 - Quantitative Asset Management
# Problem Set 1
# Akhil Srivastava

# Downloads and saves CRSP raw data and FF3 data
def download_raw_crsp_data(data_dir, wrds_id):
    # Download CRSP monthly returns

    # Reference - Assignment Intruction:
    # "This should be the full dataset available on WRDS; do not pre-filter by SHRCD, EXCHCD, or date."

    conn = wrds.Connection(wrds_username=wrds_id)
    mcrsp_raw = conn.raw_sql("""
                          select a.permno, a.permco, a.date, b.shrcd, b.exchcd,
                          a.ret, a.retx, a.shrout, a.prc, a.cfacshr, a.cfacpr
                          from crspq.msf as a
                          left join crspq.msenames as b
                          on a.permno=b.permno
                          and b.namedt<=a.date
                          and a.date<=b.nameendt
                          """)
    conn.close()
    
    # Store downloaded data in pickle format
    mcrsp_raw.to_pickle(data_dir + 'mcrsp_raw.pkl')
    
    # Download CRSP monthly delisting returns
    conn = wrds.Connection(wrds_username=wrds_id)
    dlret_raw = conn.raw_sql("""
                          select permno, dlret, dlstdt, dlstcd
                          from crspq.msedelist
                          """)
    conn.close()
    
    # Store downloaded data in pickle format
    dlret_raw.to_pickle(data_dir + 'dlret_raw.pkl')
    
def download_ff3_monthly_data(data_dir):
    # Download and save FF3 monthly data
    FF_mkt = pandas_datareader.famafrench.FamaFrenchReader('F-F_Research_Data_Factors',
                                                           start='1900',
                                                           end=str(datetime.datetime.now().year+1))
    FF_mkt = FF_mkt.read()[0]/100

    FF_mkt['Date'] = pd.to_datetime(FF_mkt.index, format='%Y-%m-%d', errors='ignore')
    FF_mkt['Year'] = FF_mkt['Date'].dt.year
    FF_mkt['Month'] = FF_mkt['Date'].dt.month
    FF_mkt.drop(['Date'], axis=1, inplace=True)

    FF_mkt.columns = ['Market_minus_Rf', 'SMB', 'HML', 'Rf', 'Year', 'Month']
    FF_mkt = FF_mkt[['Year', 'Month', 'Market_minus_Rf', 'SMB', 'HML', 'Rf']]

    # Store downloaded data in pickle format
    FF_mkt.to_pickle(data_dir + 'ff3_monthly.pkl')
    
# Processes the saved CRSP raw returns and delisted returns data to create a merged dataframe
def process_raw_crsp_data(data_dir, mcrsp_raw, dlret_raw): 
    
    ############################### Process raw CRSP returns ###############################
    
    # Sort the data by permno and date and reset index
    # Sometimes there is redundant indices in the downloaded data
    mcrsp_raw = mcrsp_raw.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()
    
    # Drop rows with missing 'shrcd', 'exchcd' and 'shrout'
    # Otherwise conversion to integer leads to exception
    mcrsp_raw = mcrsp_raw[mcrsp_raw['shrcd'].notna() & mcrsp_raw['exchcd'].notna() & mcrsp_raw['shrout'].notna()].copy()

    # Reference - Assignment Intruction: PERMNO, SHRCD, EXCHCD and SHROUT variables have type integer
    int_columns = ['permno', 'permco', 'shrcd', 'exchcd', 'shrout']
    mcrsp_raw[int_columns] = mcrsp_raw[int_columns].astype(int)

    # Reference - Assignment Intruction: Format the date column as a datetime
    mcrsp_raw['date'] = pd.to_datetime(mcrsp_raw['date'], format='%Y-%m-%d', errors='ignore')

    # Sort the data by permno and date and reset index because we dropped rows above
    mcrsp_raw = mcrsp_raw.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()
    
    ########################## Process raw CRSP delisting returns ##########################
    
    # Sort the data by permno and date and reset index
    # Sometimes there is redundant indices in the downloaded data
    dlret_raw = dlret_raw.sort_values(by=['permno', 'dlstdt']).reset_index(drop=True).copy()
    
    # Reference - Assignment Intruction: PERMNO variable has type integer
    dlret_raw['permno'] = dlret_raw['permno'].astype(int)

    # Reference - Assignment Intruction: Format the date column as a datetime
    dlret_raw = dlret_raw.rename(columns={"dlstdt": "date"}).copy()
    dlret_raw['date'] = pd.to_datetime(dlret_raw['date'], format='%Y-%m-%d', errors='ignore')

    # Sort the data by permno and date and reset index
    dlret_raw = dlret_raw.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()
    
    ##################### Merge CRSP returns with the delisting returns #####################
    
    # Merging non-desliting returns with delisted returns
    mcrsp = mcrsp_raw.merge(dlret_raw, how='outer', on=['date', 'permno'])
    mcrsp = mcrsp.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()
    
    # Store merged CRSP data in pickle format
    mcrsp.to_pickle(data_dir + 'mcrsp_ret_dret_merged.pkl')
    
# Implements Q1 requirements: Inputs - CRSP_Stocks
def PS1_Q1(CRSP_Stocks):
    # Reference - Kenneth R. French:
    # "Rm-Rf, the excess return on the market, value-weight return of all CRSP firms incorporated in the US and
    # listed on the NYSE, AMEX, or NASDAQ that have a CRSP share code of 10 or 11 at the beginning of month t,
    # good shares and price data at the beginning of t, and good return data for t minus the one-month Treasury bill rate."

    # Filter relevant exchcd - Reference - Kenneth R. French: "listed on the NYSE, AMEX, or NASDAQ"
    exchcd_set = [1, 2, 3]

    # Filter relevant shrcd - Reference - Kenneth R. French: "that have a CRSP share code of 10 or 11"
    shrcd_set = [10, 11]

    # Filter relevant date - Reference - Assignment Instruction:
    # "Your output should be from January 1926 to December 2023, at a monthly frequency"
    min_date = '1926-01-31'
    max_date = '2023-12-31'

    # Move all dates to last day of the month
    CRSP_Stocks['date'] = CRSP_Stocks['date'] + MonthEnd(0)
    CRSP_Stocks = CRSP_Stocks.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()

    # exchcd/shrcd are nan for delisted returns, so filtering rows on required exchcd/shrcd removes delisted return rows
    # dlstcd is not-nan for all the delisted return rows, so it has been used as a proxy to identify delisted return rows
    # Rows with unrequired exchcd/shrcd are removed only if those are non delisted return row
    CRSP_Stocks = CRSP_Stocks[(CRSP_Stocks['dlstcd'].notna()) |
                              ((CRSP_Stocks['dlstcd'].isna()) & (CRSP_Stocks['exchcd'].isin(exchcd_set)))]

    CRSP_Stocks = CRSP_Stocks[(CRSP_Stocks['dlstcd'].notna()) |
                              ((CRSP_Stocks['dlstcd'].isna()) & (CRSP_Stocks['shrcd'].isin(shrcd_set)))]

    # Drop unrequired columns
    CRSP_Stocks.drop(['exchcd', 'shrcd'], axis=1, inplace=True)

    # Filter dates
    CRSP_Stocks = CRSP_Stocks[CRSP_Stocks['date'] >= min_date]
    CRSP_Stocks = CRSP_Stocks[CRSP_Stocks['date'] <= max_date]

    # Calculate market equity (in USD Billions)
    # Use absolute price because if price is bid/ask average it has a negative sign to indicate so
    CRSP_Stocks['me'] = CRSP_Stocks['prc'].abs()*CRSP_Stocks['shrout']*1e-6
    # Drop unrequired columns
    CRSP_Stocks.drop(['prc', 'shrout'], axis=1, inplace=True)

    # Adjust for Delisting Returns
    # Use compounded return if both return and delisted return are available
    CRSP_Stocks['ret'] = np.where(CRSP_Stocks['ret'].notna() & CRSP_Stocks['dlret'].notna(),
                          (1 + CRSP_Stocks['ret'])*(1 + CRSP_Stocks['dlret']) - 1,
                          CRSP_Stocks['ret'])

    # Use delisted return if return is not available but delited return is
    CRSP_Stocks['ret'] = np.where(CRSP_Stocks['ret'].isna() & CRSP_Stocks['dlret'].notna(),
                          CRSP_Stocks['dlret'],
                          CRSP_Stocks['ret'])

    # Drop missing returns
    CRSP_Stocks = CRSP_Stocks[CRSP_Stocks['ret'].notna()].copy()
    # Reset index
    CRSP_Stocks = CRSP_Stocks.sort_values(by=['permno','date']).reset_index(drop=True).copy()
   
    # Aggregate Market Cap
    # For a given date and permco, sum me across different permno to find cumulative market-cap for the permco
    CRSP_Stocks_ME_SUM = CRSP_Stocks.groupby(['date','permco'])['me'].sum().reset_index()

    # For a given date and permco, among multiple market-caps for different permno find the largest one
    CRSP_Stocks_ME_MAX = CRSP_Stocks.groupby(['date','permco'])['me'].max().reset_index()

    # Merge CRSP_Stocks and CRSP_Stocks_ME_MAX
    CRSP_Stocks = pd.merge(CRSP_Stocks, CRSP_Stocks_ME_MAX, how='inner', on=['date', 'permco', 'me'])

    # Replace me with cumulative me
    # Drop existing me
    CRSP_Stocks = CRSP_Stocks.drop(['me'], axis=1)
    # Merge CRSP_Stocks and CRSP_Stocks_ME_SUM to use cmumulative market cap
    CRSP_Stocks = pd.merge(CRSP_Stocks, CRSP_Stocks_ME_SUM, how='inner', on=['date', 'permco'])

    # Sort by permno and date and drop duplicates
    CRSP_Stocks = CRSP_Stocks.sort_values(by=['permno', 'date']).drop_duplicates()

    # Add column with lagged market cap
    CRSP_Stocks['lme'] = CRSP_Stocks.groupby(['permno'])['me'].shift(1)

    # If a permno is the first permno, use me/(1+retx) to replace the missing value
    CRSP_Stocks['1+retx'] = 1 + CRSP_Stocks['retx']
    CRSP_Stocks['count'] = CRSP_Stocks.groupby(['permno']).cumcount()
    CRSP_Stocks['lme'] = np.where(CRSP_Stocks['count'] == 0, CRSP_Stocks['me']/CRSP_Stocks['1+retx'], CRSP_Stocks['lme'])
    
    # Drop missing lme
    CRSP_Stocks = CRSP_Stocks[CRSP_Stocks['lme'].notna()].copy()
    # Reset index
    CRSP_Stocks = CRSP_Stocks.sort_values(by=['permno','date']).reset_index(drop=True).copy()    
    
    # Data integrity checkes
    assert (CRSP_Stocks['ret'] == -66).any() == False
    assert (CRSP_Stocks['ret'] == -77).any() == False
    assert (CRSP_Stocks['ret'] == -88).any() == False
    assert (CRSP_Stocks['ret'] == -99).any() == False
    assert CRSP_Stocks['ret'].isna().any() == False
    assert CRSP_Stocks['lme'].isna().any() == False

    Monthly_CRSP_Stocks = CRSP_Stocks[['date']].groupby(['date']).sum()
    Monthly_CRSP_Stocks['Date'] = pd.to_datetime(Monthly_CRSP_Stocks.index, format='%Y-%m-%d', errors='ignore')
    Monthly_CRSP_Stocks['Year'] = Monthly_CRSP_Stocks['Date'].dt.year
    Monthly_CRSP_Stocks['Month'] = Monthly_CRSP_Stocks['Date'].dt.month
    Monthly_CRSP_Stocks.drop(['Date'], axis=1, inplace=True)

    Monthly_CRSP_Stocks['Stock_lag_MV'] = CRSP_Stocks[['date', 'lme']].groupby(['date']).sum()
    Monthly_CRSP_Stocks['Stock_Ew_Ret'] = CRSP_Stocks[['date', 'ret']].groupby(['date']).mean()
    Monthly_CRSP_Stocks['Stock_Vw_Ret'] = CRSP_Stocks.groupby(['date']).apply(lambda x: np.average(x.ret, weights=x.lme))

    return Monthly_CRSP_Stocks

# Implements Q2 requirements:: Inputs - Monthly_CRSP_Stocks, FF_mkt
def PS1_Q2(Monthly_CRSP_Stocks, FF_mkt):
    
    # Merge Monthly_CRSP_Stocks with FF_mkt
    df_merged = Monthly_CRSP_Stocks.merge(FF_mkt, how='inner', on=['Year', 'Month'])
    
    # Compute Esti_Market_minus_Rf
    df_merged['Esti_Market_minus_Rf'] = df_merged['Stock_Vw_Ret'] - df_merged['Rf']
    
    # Create dataframe to store required stats
    df_q2 = pd.DataFrame()
    
    str_mean = "Annualized Mean"
    str_std = "Annualized Standard Deviation"
    str_sr = "Annualized Sharpe Ratio"
    req_columns = ['Esti_Market_minus_Rf', 'Market_minus_Rf']
    
    # Compute required stats
    df_q2[str_mean] = 100*12*df_merged[req_columns].mean(axis=0)
    df_q2[str_std] = 100*np.sqrt(12)*df_merged[req_columns].std(axis=0)
    df_q2[str_sr] = df_q2[str_mean]/df_q2[str_std]
    df_q2["Skewness"] = df_merged[req_columns].skew(axis=0)
    df_q2["Excess Kurtosis"] = df_merged[req_columns].kurtosis(axis=0)
    
    # Convert dataframe to the desired format
    df_q2 = df_q2.T
    df_q2.columns = ['Estimated FF Market Excess Return', 'Actual FF Market Excess Return']

    return df_q2

# Implements Q3 requirements: Inputs - Monthly_CRSP_Stocks, FF_mkt
def PS1_Q3(Monthly_CRSP_Stocks, FF_mkt):    
    # Merge Monthly_CRSP_Stocks with FF_mkt
    df_merged = Monthly_CRSP_Stocks.merge(FF_mkt, how='inner', on=['Year', 'Month'])
    
    # Compute Esti_Market_minus_Rf
    df_merged['Esti_Market_minus_Rf'] = df_merged['Stock_Vw_Ret'] - df_merged['Rf']
    
    # Create dataframe to store required stats
    df_q3 = pd.Series(dtype=float)
    
    # Compute required metrics
    df_q3["Correlation"] = df_merged[['Esti_Market_minus_Rf', 'Market_minus_Rf']].corr().iloc[0, 1]
    df_q3["Max_Abs_Diff"] = abs(df_merged['Esti_Market_minus_Rf'] - df_merged['Market_minus_Rf']).max()
    
    return df_q3.values


# Runs all the functions and prints the required results
def driver(download_data=False):
    # Download data only if needed
    if download_data == True:
        download_raw_crsp_data(data_dir, wrds_id)
        download_ff3_monthly_data(data_dir)
    
    # Load stored raw CRSP returns data as a dataframe
    mcrsp_raw = pd.read_pickle(data_dir + 'mcrsp_raw.pkl')

    # Load stored raw CRSP delisting returns data as a dataframe
    dlret_raw = pd.read_pickle(data_dir + 'dlret_raw.pkl')
    
    # Load stored FF3 data as a dataframe
    FF_mkt = pd.read_pickle(data_dir + 'ff3_monthly.pkl')
    
    # Process raw CRSP returns and delisting returns to create and store a merged dataframe
    process_raw_crsp_data(data_dir, mcrsp_raw, dlret_raw)
    
    # Load stored merged CRSP data as a dataframe
    CRSP_Stocks = pd.read_pickle(data_dir + 'mcrsp_ret_dret_merged.pkl')

    # Calculate value-weighted return, equal-weighted return and lagged total market cap.
    Monthly_CRSP_Stocks = PS1_Q1(CRSP_Stocks)

    # Compute required return stats for Q2
    results_ps1_q2 = PS1_Q2(Monthly_CRSP_Stocks, FF_mkt)
    
    # Display Q2 results    
    print(results_ps1_q2)

    # Compute required metrics for Q3
    result_ps1_q3 = PS1_Q3(Monthly_CRSP_Stocks, FF_mkt)

    # Display Q3 results
    print("The correlation between the two time series: {:.8f}".format(result_ps1_q3[0]))
    print("The maximum absolute difference between the two time series: {:.8f}".format(result_ps1_q3[1]))
    
# Import packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas_datareader
import wrds
import os
from pandas.tseries.offsets import *
import datetime

# Directory to store the downloaded data
data_dir = 'data\\'

# WRDS login id
wrds_id = 'smarty_iitian'

# Specify whether we need to download the raw data or not
download_data = False

driver(download_data)