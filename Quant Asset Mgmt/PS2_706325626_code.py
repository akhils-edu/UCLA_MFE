# MGMTMFE 431 - Quantitative Asset Management
# Problem Set 2
# Akhil Srivastava

# Downloads and saves CRSP raw data
def download_raw_crsp_data(data_dir, wrds_id):
    ###################################### Download CRSP stock data ######################################
    
    # Download CRSP stock monthly returns
    # Reference - Assignment Instruction:
    # "This should be the full dataset available on WRDS; do not pre-filter by SHRCD, EXCHCD, or date."
    conn = wrds.Connection(wrds_username=wrds_id)
    mscrsp_raw = conn.raw_sql("""select a.permno, a.permco, a.date, b.shrcd, b.exchcd, a.ret, a.retx, a.shrout, a.prc
                                from crspq.msf as a
                                left join crspq.msenames as b
                                on a.permno=b.permno and b.namedt<=a.date and a.date<=b.nameendt""")    
    # Store downloaded data in pickle format
    mscrsp_raw.to_pickle(data_dir + 'mscrsp_raw.pkl')

    # Download CRSP stock monthly delisting returns
    msdelcrsp_raw = conn.raw_sql("""select permno, dlret, dlstdt, dlstcd from crspq.msedelist""")
    # Store downloaded data in pickle format
    msdelcrsp_raw.to_pickle(data_dir + 'msdelcrsp_raw.pkl')

    
    ###################################### Download CRSP bond data ######################################
    
    # Download CRSP bond monthly data
    # Reference - Assignment Instruction:
    # "This should be the full dataset available on WRDS; do not pre-filter by MCALDT."
    mbcrsp_raw = conn.raw_sql("""select kycrspid, mcaldt, tmretnua, tmtotout from crspq.tfz_mth""")
    # Store downloaded data in pickle format
    mbcrsp_raw.to_pickle(data_dir + 'mbcrsp_raw.pkl')

    # Download CRSP t-Bill monthly data
    # Reference - Assignment Instruction:
    # "This should be the full dataset available on WRDS; do not pre-filter by caldt."
    mtbcrsp_raw = conn.raw_sql("""select caldt, t30ret, t90ret from crspq.mcti""")
    # Store downloaded data in pickle format
    mtbcrsp_raw.to_pickle(data_dir + 'mtbcrsp_raw.pkl')
    
    # Close WRDS API connection
    conn.close()
    
# Processes and saves raw CRSP stock returns and delisted returns data to create a merged dataframe
def process_raw_crsp_stock_data(data_dir, mscrsp_raw, msdelcrsp_raw):
    print("      Processing raw data for stocks ...")
    
    ############################### Process raw CRSP returns ###############################
    
    # Sort the data by permno and date and reset index as sometimes there are redundant indices
    mscrsp_raw = mscrsp_raw.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()
    
    # Drop rows with missing 'shrcd', 'exchcd' and 'shrout'
    # Otherwise conversion to integer leads to exception
    mscrsp_raw = mscrsp_raw[mscrsp_raw['shrcd'].notna() &
                            mscrsp_raw['exchcd'].notna() &
                            mscrsp_raw['shrout'].notna()].copy()

    # Reference - Assignment Instruction: PERMNO, SHRCD, EXCHCD and SHROUT variables have type integer
    int_columns = ['permno', 'permco', 'shrcd', 'exchcd', 'shrout']
    mscrsp_raw[int_columns] = mscrsp_raw[int_columns].astype(int)

    # Reference - Assignment Instruction: Format the date column as a datetime
    mscrsp_raw['date'] = pd.to_datetime(mscrsp_raw['date'], format='%Y-%m-%d', errors='ignore')

    # Sort the data by permno and date and reset index because we dropped rows above
    mscrsp_raw = mscrsp_raw.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()
    
    ########################## Process raw CRSP delisting returns ##########################
    
    # Sort the data by permno and date and reset index as sometimes there are redundant indices
    msdelcrsp_raw = msdelcrsp_raw.sort_values(by=['permno', 'dlstdt']).reset_index(drop=True).copy()
    
    # Reference - Assignment Instruction: PERMNO variable has type integer
    msdelcrsp_raw['permno'] = msdelcrsp_raw['permno'].astype(int)

    # Reference - Assignment Instruction: Format the date column as a datetime
    msdelcrsp_raw = msdelcrsp_raw.rename(columns={"dlstdt": "date"}).copy()
    msdelcrsp_raw['date'] = pd.to_datetime(msdelcrsp_raw['date'], format='%Y-%m-%d', errors='ignore')

    # Sort the data by permno and date and reset index
    msdelcrsp_raw = msdelcrsp_raw.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()
    
    ##################### Merge CRSP stock returns with the delisting returns #####################

    # Keep all the rows, don't drop anything yet
    mscrsp_processed = mscrsp_raw.merge(msdelcrsp_raw, how='outer', on=['date', 'permno'])
    # Sort the data by permno and date and reset index
    mscrsp_processed = mscrsp_processed.sort_values(by=['permno', 'date']).reset_index(drop=True).copy()
    
    # Store Processed merged CRSP data in pickle format
    mscrsp_processed.to_pickle(data_dir + 'mscrsp_processed.pkl')

# Processes and saves raw CRSP bond and t-bill data
def process_raw_crsp_bond_data(data_dir, mbcrsp_raw, mtbcrsp_raw): 
    print("      Processing raw data for bonds ...")

    ############################### Process raw CRSP bond data ###############################
    
    # Sort the data by kycrspid and mcaldt and reset index as sometimes there are redundant indices
    mbcrsp_processed = mbcrsp_raw.sort_values(by=['kycrspid', 'mcaldt']).reset_index(drop=True).copy()    
   
    # Reference - Assignment Instruction: Format the MCALDT column as a datetime    
    mbcrsp_processed['mcaldt'] = pd.to_datetime(mbcrsp_processed['mcaldt'], format='%Y-%m-%d', errors='ignore')
    
    # Sort the data by crsp_id and mcaldt and reset index
    mbcrsp_processed = mbcrsp_processed.sort_values(by=['kycrspid', 'mcaldt']).reset_index(drop=True).copy()
    
    ############################## Process raw CRSP t-bill data ##############################
    
    # Sort the data by caldt and reset index as sometimes there are redundant indices
    mtbcrsp_processed = mtbcrsp_raw.sort_values(by=['caldt']).reset_index(drop=True).copy()

    # Reference - Assignment Instruction: Format the caldt column as a datetime
    mtbcrsp_processed['caldt'] = pd.to_datetime(mtbcrsp_processed['caldt'], format='%Y-%m-%d', errors='ignore')

    # Sort the data by caldt and reset index
    mtbcrsp_processed = mtbcrsp_processed.sort_values(by=['caldt']).reset_index(drop=True).copy()
    
    # Store processed data in pickle format
    mbcrsp_processed.to_pickle(data_dir + 'mbcrsp_processed.pkl')
    mtbcrsp_processed.to_pickle(data_dir + 'mtbcrsp_processed.pkl')
    
# Implements PS1-Q1 requirements: Inputs - CRSP_Stocks
def PS1_Q1(CRSP_Stocks):
    print("      Recomputing monthly returns for stocks ...")
    # Reference - Kenneth R. French:
    # "Rm-Rf, the excess return on the market, value-weight return of all CRSP firms incorporated in the US and
    # listed on the NYSE, AMEX, or NASDAQ that have a CRSP share code of 10 or 11 at the beginning of month t,
    # good shares and price data at the beginning of t, and good return data for t minus the one-month Treasury bill rate."

    # Filter relevant exchcd - Reference - Kenneth R. French: "listed on the NYSE, AMEX, or NASDAQ"
    exchcd_set = [1, 2, 3, 31, 32, 33]
    # Filter relevant shrcd - Reference - Kenneth R. French: "that have a CRSP share code of 10 or 11"
    shrcd_set = [10, 11]

    # Move all dates to the last day of the month
    CRSP_Stocks['date'] = CRSP_Stocks['date'] + MonthEnd(0)
    # Sort again as we changed date values
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

    # Calculate market equity in USD millions
    # Use absolute price because if price is bid/ask average it has a negative sign to indicate so
    CRSP_Stocks['me'] = CRSP_Stocks['prc'].abs()*CRSP_Stocks['shrout']*1e-3
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
   
    # Aggregate Market Cap. computation
    # For a given date and permco, sum me across different permno to find cumulative market-cap for the permco
    CRSP_Stocks_ME_SUM = CRSP_Stocks.groupby(['date','permco'])['me'].sum().reset_index()
    # For a given date and permco, among multiple market-caps for different permno find the largest one
    CRSP_Stocks_ME_MAX = CRSP_Stocks.groupby(['date','permco'])['me'].max().reset_index()
    # Merge CRSP_Stocks and CRSP_Stocks_ME_MAX
    CRSP_Stocks = pd.merge(CRSP_Stocks, CRSP_Stocks_ME_MAX, how='inner', on=['date', 'permco', 'me'])
    # Replace me with cumulative me
    # Drop existing me
    CRSP_Stocks = CRSP_Stocks.drop(['me'], axis=1)
    # Merge CRSP_Stocks and CRSP_Stocks_ME_SUM to use cumulative market cap
    CRSP_Stocks = pd.merge(CRSP_Stocks, CRSP_Stocks_ME_SUM, how='inner', on=['date', 'permco'])
    # Sort by permno and date and drop duplicates
    CRSP_Stocks = CRSP_Stocks.sort_values(by=['permno', 'date']).drop_duplicates()

    # lagged Market Cap. computation
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

    # Compute required monthly values
    Monthly_CRSP_Stocks = CRSP_Stocks[['date']].groupby(['date']).sum()
    Monthly_CRSP_Stocks['Date'] = pd.to_datetime(Monthly_CRSP_Stocks.index, format='%Y-%m-%d', errors='ignore')
    Monthly_CRSP_Stocks['Year'] = Monthly_CRSP_Stocks['Date'].dt.year
    Monthly_CRSP_Stocks['Month'] = Monthly_CRSP_Stocks['Date'].dt.month
    Monthly_CRSP_Stocks.drop(['Date'], axis=1, inplace=True)

    Monthly_CRSP_Stocks['Stock_lag_MV'] = CRSP_Stocks[['date', 'lme']].groupby(['date']).sum()
    Monthly_CRSP_Stocks['Stock_Ew_Ret'] = CRSP_Stocks[['date', 'ret']].groupby(['date']).mean()
    Monthly_CRSP_Stocks['Stock_Vw_Ret'] = CRSP_Stocks.groupby(['date']).apply(lambda x: np.average(x.ret, weights=x.lme))

    # Store final data in pickle format
    Monthly_CRSP_Stocks.to_pickle(data_dir + 'Monthly_CRSP_Stocks.pkl')
    
    return Monthly_CRSP_Stocks
    
# Implements PS2-Q1 requirements: Inputs - CRSP_Bonds
def PS2_Q1(CRSP_Bonds):
    print("      Recomputing monthly returns for bonds ...")
    # Rename columns
    CRSP_Bonds = CRSP_Bonds.rename(columns={"kycrspid":"crsp_id",
                                            "mcaldt":"date",
                                            "tmretnua":"ret",
                                            "tmtotout":"me"}).copy()

    # Move all dates to the last day of the month
    CRSP_Bonds['date'] = CRSP_Bonds['date'] + MonthEnd(0)
    # Sort again as we changed date values
    CRSP_Bonds = CRSP_Bonds.sort_values(by=['crsp_id', 'date']).reset_index(drop=True).copy()

    # Filter dates
    CRSP_Bonds = CRSP_Bonds[CRSP_Bonds['date'] >= min_date]
    CRSP_Bonds = CRSP_Bonds[CRSP_Bonds['date'] <= max_date]

    # Drop missing returns
    CRSP_Bonds = CRSP_Bonds[CRSP_Bonds['ret'].notna()].copy()
    # Reset index
    CRSP_Bonds = CRSP_Bonds.sort_values(by=['crsp_id', 'date']).reset_index(drop=True).copy()
    
    # Add column with lagged market value
    CRSP_Bonds['lme'] = CRSP_Bonds.groupby(['crsp_id'])['me'].shift(1)
    # Drop missing lme
    CRSP_Bonds = CRSP_Bonds[CRSP_Bonds['lme'].notna()].copy()
    # Reset index
    CRSP_Bonds = CRSP_Bonds.sort_values(by=['crsp_id', 'date']).reset_index(drop=True).copy()
    
    # Data integrity checkes
    # CRSP_US_Treasury_Database_Guide:
    # "tmretnua is set to -99 when the price is missing for either this month or the previous month."
    assert (CRSP_Bonds['ret'] == -99).any() == False
    assert CRSP_Bonds['ret'].isna().any() == False
    assert CRSP_Bonds['lme'].isna().any() == False

    # Compute required monthly values
    Monthly_CRSP_Bonds = CRSP_Bonds[['date']].groupby(['date']).sum()
    Monthly_CRSP_Bonds['Date'] = pd.to_datetime(Monthly_CRSP_Bonds.index, format='%Y-%m-%d', errors='ignore')
    Monthly_CRSP_Bonds['Year'] = Monthly_CRSP_Bonds['Date'].dt.year
    Monthly_CRSP_Bonds['Month'] = Monthly_CRSP_Bonds['Date'].dt.month
    Monthly_CRSP_Bonds.drop(['Date'], axis=1, inplace=True)

    Monthly_CRSP_Bonds['Bond_lag_MV'] = CRSP_Bonds[['date', 'lme']].groupby(['date']).sum()
    Monthly_CRSP_Bonds['Bond_Ew_Ret'] = CRSP_Bonds[['date', 'ret']].groupby(['date']).mean()
    Monthly_CRSP_Bonds['Bond_Vw_Ret'] = CRSP_Bonds.groupby(['date']).apply(lambda x: np.average(x.ret, weights=x.lme))
    
    # Store final data in pickle format
    Monthly_CRSP_Bonds.to_pickle(data_dir + 'Monthly_CRSP_Bonds.pkl')

    return Monthly_CRSP_Bonds
    
# Implements PS2-Q2 t-bill requirements: Inputs - Monthly_CRSP_Riskless
def PS2_Q2_TBILL(Monthly_CRSP_Riskless):
    # Rename columns
    Monthly_CRSP_Riskless = Monthly_CRSP_Riskless.rename(columns={"caldt": "date",
                                                                  "t30ret": "rf30",
                                                                  "t90ret": "rf90"}).copy()

    # Move all dates to the last day of the month
    Monthly_CRSP_Riskless['date'] = Monthly_CRSP_Riskless['date'] + MonthEnd(0)
    Monthly_CRSP_Riskless = Monthly_CRSP_Riskless.sort_values(by=['date']).reset_index(drop=True).copy()

    # Filter dates
    Monthly_CRSP_Riskless = Monthly_CRSP_Riskless[Monthly_CRSP_Riskless['date'] >= min_date]
    Monthly_CRSP_Riskless = Monthly_CRSP_Riskless[Monthly_CRSP_Riskless['date'] <= max_date]    

    # Drop missing returns
    Monthly_CRSP_Riskless = Monthly_CRSP_Riskless[Monthly_CRSP_Riskless['rf30'].notna()].copy()
    # Reset index
    Monthly_CRSP_Riskless = Monthly_CRSP_Riskless.sort_values(by=['date']).reset_index(drop=True).copy()   
    
    # Data integrity checkes    
    assert Monthly_CRSP_Riskless['rf30'].isna().any() == False

    # Compute required monthly values
    Monthly_CRSP_Riskless['Date'] = pd.to_datetime(Monthly_CRSP_Riskless.date, format='%Y-%m-%d', errors='ignore')
    Monthly_CRSP_Riskless['Year'] = Monthly_CRSP_Riskless['Date'].dt.year
    Monthly_CRSP_Riskless['Month'] = Monthly_CRSP_Riskless['Date'].dt.month
    Monthly_CRSP_Riskless.drop(['date', 'Date'], axis=1, inplace=True)

    return Monthly_CRSP_Riskless

# Implements PS2-Q2 requirements: Inputs - Monthly_CRSP_Stocks, Monthly_CRSP_Bonds, Monthly_CRSP_Riskless
def PS2_Q2(Monthly_CRSP_Stocks, Monthly_CRSP_Bonds, Monthly_CRSP_Riskless):
    # For tbills filter dates, handle missing returns, check data integrity and split date to year and month
    Monthly_CRSP_Riskless = PS2_Q2_TBILL(Monthly_CRSP_Riskless)

    # Merge Monthly_CRSP_Stocks with Monthly_CRSP_Bonds
    df_merged = Monthly_CRSP_Stocks.merge(Monthly_CRSP_Bonds, how='outer', on=['Year', 'Month'])
    
    # Further merge Monthly_CRSP_Riskless with the merged data
    Monthly_CRSP_Universe = df_merged.merge(Monthly_CRSP_Riskless, how='outer', on=['Year', 'Month'])
    
    # Drop unrequired columns
    Monthly_CRSP_Universe.drop(['Stock_Ew_Ret', 'Bond_Ew_Ret'], axis=1, inplace=True)
    
    # For each year-month, calculate excess value-weighted returns for both stocks and bonds
    Monthly_CRSP_Universe['Stock_Excess_Vw_Ret'] = Monthly_CRSP_Universe['Stock_Vw_Ret'] - Monthly_CRSP_Universe['rf30']
    Monthly_CRSP_Universe['Bond_Excess_Vw_Ret'] = Monthly_CRSP_Universe['Bond_Vw_Ret'] - Monthly_CRSP_Universe['rf30']
    
    # Drop unrequired columns
    Monthly_CRSP_Universe.drop(['Stock_Vw_Ret', 'Bond_Vw_Ret', 'rf30', 'rf90'], axis=1, inplace=True)
    
    # Store final data in pickle format
    Monthly_CRSP_Universe.to_pickle(data_dir + 'Monthly_CRSP_Universe.pkl')

    return Monthly_CRSP_Universe
    
# Implements PS2-Q3 requirements:: Inputs - Monthly_CRSP_Universe
def PS2_Q3(Monthly_CRSP_Universe):
    # Create a copy of the dataframe to be used locally
    Port_Rets = Monthly_CRSP_Universe.copy()

    # Compute value-weighted portfolio return above riskless rate
    Port_Rets["Excess_Vw_Ret"] = np.average(Port_Rets[["Stock_Excess_Vw_Ret", "Bond_Excess_Vw_Ret"]],
                                            weights=Port_Rets[["Stock_lag_MV", "Bond_lag_MV"]],
                                            axis=1)
    # Drop unrequired columns
    Port_Rets.drop(['Stock_lag_MV', 'Bond_lag_MV'], axis=1, inplace=True)
    
    # Compute 60-40 portfolio return above riskless rate
    Port_Rets["Excess_60_40_Ret"] = 0.6*Port_Rets["Stock_Excess_Vw_Ret"] + 0.4*Port_Rets["Bond_Excess_Vw_Ret"]

    # Compute stock and bond inverse sigma hat
    # Reference - Asness et al. (2012)
    # "We estimate sigma_hat(t, i) as the 3-year rolling volatility of monthly excess returns"
    for i in range(36, len(Port_Rets)):
        Port_Rets.loc[i, "Stock_inverse_sigma_hat"] = 1/Port_Rets["Stock_Excess_Vw_Ret"][i-36:i].std()
        Port_Rets.loc[i, "Bond_inverse_sigma_hat"] = 1/Port_Rets["Bond_Excess_Vw_Ret"][i-36:i].std()
    
    # To ensure that we calculate σˆ for both the portfolios for the matching holding period, drop nan rows
    Port_Rets.dropna(inplace=True)
    # Reset index as we dropped rows
    Port_Rets = Port_Rets.sort_values(by=['Year', 'Month']).reset_index(drop=True).copy()

    # Compute unlevered k
    # Reference - Asness et al. (2012)
    # "The first portfolio is an unlevered RP, obtained by setting k(t) = 1/(sum of inverse_sigma_hat(t, i)))"
    Port_Rets["Unlevered_k"] = 1/Port_Rets[["Stock_inverse_sigma_hat", "Bond_inverse_sigma_hat"]].sum(axis=1)

    # Compute unlevered RP portfolio return above riskless rate
    # Reference - Asness et al. (2012)
    # "set the portfolio weight in asset class i to, w(t, i) = k(t)*inverse_sigma_hat(t, i)"
    stock_inv_sigma_wtd_ret = Port_Rets["Stock_Excess_Vw_Ret"].multiply(Port_Rets["Stock_inverse_sigma_hat"], axis="index")
    bond_inv_sigma_wtd_ret = Port_Rets["Bond_Excess_Vw_Ret"].multiply(Port_Rets["Bond_inverse_sigma_hat"], axis="index")
    port_inv_sigma_wtd_ret = stock_inv_sigma_wtd_ret + bond_inv_sigma_wtd_ret    
    Port_Rets["Excess_Unlevered_RP_Ret"] = port_inv_sigma_wtd_ret.multiply(Port_Rets["Unlevered_k"], axis="index")

    # Compute Levered k
    # Reference - Asness et al. (2012)
    # "we set k so that the annualized volatility of this portfolio matches the ex-post realized vol of the benchmark"
    # Reference - Assignment Instruction
    # "match the value-weighted portfolio’s σˆ over the longest matched holding period of both"    
    Port_Rets["Levered_k"] = Port_Rets['Excess_Vw_Ret'].std()/port_inv_sigma_wtd_ret.std()

    # Compute Levered RP portfolio return above riskless rate
    # Reference - Asness et al. (2012)
    # "set the portfolio weight in asset class i to, w(t, i) = k(t)*inverse_sigma_hat(t, i)"
    Port_Rets["Excess_Levered_RP_Ret"] = port_inv_sigma_wtd_ret.multiply(Port_Rets["Levered_k"], axis="index")

    # Validate that the volatility of this portfolio indeed matches the volatility of the value-weighted portfolio
    assert math.isclose(Port_Rets['Excess_Vw_Ret'].std(), Port_Rets["Excess_Levered_RP_Ret"].std(), rel_tol=1e-6) == True

    return Port_Rets
    
# Implements PS2-Q4 requirements:: Inputs - Port_Rets
def PS2_Q4(Port_Rets):
    # Filter dates
    Port_Rets = Port_Rets[(Port_Rets['Year'] > min_report_year) |
                          ((Port_Rets['Year'] == min_report_year) & (Port_Rets['Month'] >= min_report_month))]
    Port_Rets = Port_Rets[(Port_Rets['Year'] < max_report_year) |
                          ((Port_Rets['Year'] == max_report_year) & (Port_Rets['Month'] <= max_report_month))]

    # Create dataframe to store required stats
    df_ps2_q4 = pd.DataFrame()    

    # List of required potfolios for which we need performance stats
    req_columns = ['Stock_Excess_Vw_Ret', 'Bond_Excess_Vw_Ret',
                   'Excess_Vw_Ret', 'Excess_60_40_Ret',
                   'Excess_Unlevered_RP_Ret', 'Excess_Levered_RP_Ret']

    # Compute required stats
    df_ps2_q4["Annualized Mean"] = 100*12*Port_Rets[req_columns].mean(axis=0)
    df_ps2_q4["t-stat of Annualized Mean"] = ttest_1samp(Port_Rets[req_columns], 6*[0], axis=0).statistic
    df_ps2_q4["Annualized Standard Deviation"] = 100*np.sqrt(12)*Port_Rets[req_columns].std(axis=0)
    df_ps2_q4["Annualized Sharpe Ratio"] = df_ps2_q4["Annualized Mean"]/df_ps2_q4["Annualized Standard Deviation"]
    df_ps2_q4["Skewness"] = Port_Rets[req_columns].skew(axis=0)
    df_ps2_q4["Excess Kurtosis"] = Port_Rets[req_columns].kurtosis(axis=0)

    # Convert dataframe to the desired format
    df_ps2_q4.index = ['CRSP stocks', 'CRSP bonds',
                       'Value-weighted portfolio', '60/40 portfolio',
                       'unlevered RP', 'levered RP']

    return df_ps2_q4
    
# Processes raw CRSP data downloaded from WRDS for each asset class
def process_raw_data():
    # Load stored raw CRSP stock returns data as a dataframe
    mscrsp_raw = pd.read_pickle(data_dir + 'mscrsp_raw.pkl')

    # Load stored raw CRSP stock delisting returns data as a dataframe
    msdelcrsp_raw = pd.read_pickle(data_dir + 'msdelcrsp_raw.pkl')

    # Load stored raw CRSP bond data as a dataframe
    mbcrsp_raw = pd.read_pickle(data_dir + 'mbcrsp_raw.pkl')

    # Load stored raw CRSP t-bill data as a dataframe
    mtbcrsp_raw = pd.read_pickle(data_dir + 'mtbcrsp_raw.pkl')

    # Process and store raw CRSP stock returns and delisting returns
    process_raw_crsp_stock_data(data_dir, mscrsp_raw, msdelcrsp_raw)

    # Process and store raw CRSP bond and t-bill data
    process_raw_crsp_bond_data(data_dir, mbcrsp_raw, mtbcrsp_raw)
    
# Computes monthly returns for each asset class (Stocks, Bonds, T-Bills)
def compute_monthly_returns(recompute=False):
    # If recumpute is set to true, recompute monthly returns
    if recompute == True:
        print("Recomputing monthly returns for each asset class ...")

        # Load processed CRSP stock and bond data as dataframes
        CRSP_Stocks = pd.read_pickle(data_dir + 'mscrsp_processed.pkl')
        CRSP_Bonds = pd.read_pickle(data_dir + 'mbcrsp_processed.pkl')

        # Calculate stock and bond monthly equal-weighted return, value-weighted return and lagged total market cap.
        Monthly_CRSP_Stocks = PS1_Q1(CRSP_Stocks)
        Monthly_CRSP_Bonds = PS2_Q1(CRSP_Bonds)

    # Otherwise load pre-computed data from the stored pickle files
    else:
        print("Loading asset returns from the pickle files ...")
        
        # Load stored Monthly_CRSP_Stocks data as a dataframe
        Monthly_CRSP_Stocks = pd.read_pickle(data_dir + 'Monthly_CRSP_Stocks.pkl')

        # Load stored Monthly_CRSP_Bonds data as a dataframe
        Monthly_CRSP_Bonds = pd.read_pickle(data_dir + 'Monthly_CRSP_Bonds.pkl')

    # Load processed CRSP t-bill data as a dataframe
    Monthly_CRSP_Riskless = pd.read_pickle(data_dir + 'mtbcrsp_processed.pkl')
        
    return Monthly_CRSP_Stocks, Monthly_CRSP_Bonds, Monthly_CRSP_Riskless
    
# Runs all the functions and prints the required results
def driver(download_data=False, process_data=False, recompute_monthly_returns=False):
    # Download the data only if needed
    if download_data == True:
        print("Downloading data for each asset class ...")
        download_raw_crsp_data(data_dir, wrds_id)
    else:
        print("Skipped data downloading!")
    
    # Process raw data only if needed
    if process_data == True:
        print("Processing raw data for each asset class ...")
        process_raw_data()
    else:
        print("Skipped raw data processing!")      
    
    # Commpute monthly returns for each asset class (Stocks, Bonds, T-Bills)
    Monthly_CRSP_Stocks, Monthly_CRSP_Bonds, Monthly_CRSP_Riskless = compute_monthly_returns(recompute_monthly_returns)

    # Aggregate stock, bond and riskless monthly return datatables
    Monthly_CRSP_Universe = PS2_Q2(Monthly_CRSP_Stocks, Monthly_CRSP_Bonds, Monthly_CRSP_Riskless)
    
    # Calculate unlevered and levered risk-parity portfolio monthly returns
    Port_Rets = PS2_Q3(Monthly_CRSP_Universe)
    
    # Display Q4 results        
    result_ps2_q4 = PS2_Q4(Port_Rets)
    
    print(result_ps2_q4)
    

# Import packages
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import pandas_datareader
import wrds
import os
from pandas.tseries.offsets import *
import datetime
from scipy.stats import ttest_1samp
import math

# Directory to store the downloaded data
data_dir = 'data\\'

# WRDS login id
wrds_id = 'smarty_iitian'

# Filter relevant date - Reference - Assignment Instruction:
# "Your output should be from January 1926 to December 2023, at a monthly frequency"
min_date = '1926-01-31'
max_date = '2023-12-31'

# Select reporting sample dates
# "Your sample should be from January 1929 to June 2010, at monthly frequency"
min_report_year = 1929
max_report_year = 2010
min_report_month = 1
max_report_month = 6

# Specify whether we need to download the raw data or not
download_data = False

# Specify whether we need to process the raw data or not
process_data = True

# Specify whether we need to recompute monthly returns or not
recompute_monthly_returns = True

driver(download_data, process_data, recompute_monthly_returns)