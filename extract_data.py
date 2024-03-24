from datetime import date, timedelta
import ta
from ta import add_all_ta_features
from ta.utils import dropna
from jugaad_data.nse import bhavcopy_save
import pandas as pd  # Assuming you have pandas installed (optional)
from holidays import isHoliday 
from concurrent.futures import ThreadPoolExecutor
import functools
from functools import partial


# def download_bhavcopy_data(num_days, output_directory):  
#   today = date(2024, 3, 15)
#   count = 0
#   for days_back in range(num_days):
#     try:
#         date_to_download = today - timedelta(days=days_back)
#         if isHoliday(date_to_download):
#            print(f"Holiday!! {date_to_download}")
#            continue
#         full_bhavcopy_save(date_to_download, output_directory, True)
#         count+=1
#         print(f"downloading data for {date_to_download} {count}")
#     except Exception as e:  # Handle potential download errors
#         print(f"Error downloading data for {date_to_download}: {e}")


# output_directory = "C:\\Users\\veprasad\\Downloads\\BhavCopiesExclude"
# num_days_to_download = 60

# download_bhavcopy_data(num_days_to_download, output_directory)

from jugaad_data.nse import stock_df

# df_test= stock_df(symbol="INFY", from_date=date(2024,1,14),
#                     to_date=date(2024,3,15), series="EQ")
# print(df_test)

dateToday=date(2024, 3, 15)
output_directory = "C:\\Users\\veprasad\\Downloads\\BhavCopiesExclude"
bhavcopy_save(dateToday, output_directory, True)
bhav_path = output_directory +"\\cm"+ dateToday.strftime("%d%b%Y") + "bhav.csv"
df = pd.read_csv(bhav_path)


filtered_df = df[df['SERIES'] == "EQ"]
filtered_df = filtered_df[['SYMBOL']]
# for sym in filtered_df['SYMBOL']:
#     try:
#         df_raw = stock_df(symbol=sym, from_date=date(2024,1,14),
#                     to_date=date(2024,3,15), series="EQ")
#         df_sorted = df_raw.sort_values(by='DATE')
#         df_sorted['RSI'] = ta.momentum.RSIIndicator(close=df_sorted['CLOSE']).rsi()
#     except Exception as e:  # Handle potential download errors
#         print(f"Error handling data for {sym}: {e}")

# df_raw = stock_df(symbol="INFY", from_date=date(2024,3,14),
#                     to_date=date(2024,3,15), series="EQ")
# print(df_raw)
        

# Adjusted process_symbol function to accept from_date and to_date
def process_symbol(sym, from_date, to_date):
    try:
        df_raw = stock_df(symbol=sym, from_date=from_date, to_date=to_date, series="EQ")
        df_sorted = df_raw.sort_values(by='DATE')
        df_sorted['RSI'] = ta.momentum.RSIIndicator(close=df_sorted['CLOSE']).rsi()
        return df_sorted
    except Exception as e:
        print(f"Error handling data for {sym}: {e}")
        return None
    

def process_symbol_for_ant_indicator(sym, from_date, to_date, c):
    # print("processing "+sym)
    try:
        df_raw = stock_df(symbol=sym, from_date=from_date, to_date=to_date, series="EQ")
        df_raw = df_raw.sort_values(by='DATE')
        df_recent = df_raw.tail(15)

        # print(df_recent)
        
        momentum_count = (df_recent['CLOSE'].diff() > 0).sum()
        volume_change = ((df_recent['VOLUME'].iloc[-1] - df_recent['VOLUME'].iloc[0]) / df_recent['VOLUME'].iloc[0]) * 100
        price_change = ((df_recent['CLOSE'].iloc[-1] - df_recent['CLOSE'].iloc[0]) / df_recent['CLOSE'].iloc[0]) * 100
        print(f"Symbol: {sym}, Momentum count: {momentum_count}, Volume change: {volume_change}, Price change: {price_change}")

        momentum_days = 12
        volume_increase_threshold_min = 20
        volume_increase_threshold_max = 25
        price_increase_threshold = 15
        if momentum_count >= momentum_days and volume_change >= volume_increase_threshold_min and volume_change <= volume_increase_threshold_max and price_change >= price_increase_threshold:
            return sym, True
        else:
            return sym, False
    except Exception as e:
        print(f"Error handling data for {sym}: {e}")
        # process_symbol_for_ant_indicator(sym, from_date, to_date, c+1)
        return sym, False

symbols = filtered_df['SYMBOL'].tolist()
from_date = date(2024, 2, 14)
to_date = date(2024, 3, 18)

with ThreadPoolExecutor(max_workers=10) as executor:
    results = executor.map(lambda sym: process_symbol_for_ant_indicator(sym, from_date, to_date, 0), symbols)

for result in results:
    if(result[1]==True):
        print(f"Symbol: {result[0]}, ANT Indicator: {'Met' if result[1] else 'Not Met'}")


# symbols = filtered_df['SYMBOL'].tolist()
# from_date = date(2024, 1, 14)
# to_date = date(2024, 3, 15)

# with ThreadPoolExecutor(max_workers=10) as executor:
#     partial_process_symbol = partial(process_symbol, from_date=from_date, to_date=to_date)
#     results = executor.map(partial_process_symbol, symbols)

# dfs = [result for result in results if result is not None]
# final_df = pd.concat(dfs)

