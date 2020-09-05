#! /usr/bin/python3
from io import StringIO
import requests as r
import pandas as pd
import datetime as dt

def get_date_depth(date, ticker, exch='OSE'):
    quote_r = r.get(
        'https://www.netfonds.no/quotes/posdump.php?date={}&paper={}.{}&csv_format=csv'.format(
            date, ticker, exch
        )
    )
    if quote_r.status_code != 200:
        print('Bad status code:', quote_r.status_code)
        return
    quote_b = quote_r.content

    # Bytes to df
    s = str(quote_b, "ISO-8859-1")
    data = StringIO(s)
    df = pd.read_csv(data)
    
    return df


def parse_netfonds_time(date_str, format_str='%Y%m%dT%H%M%S'):
    """Parses a single time string into datetime.
    
    # Parameters:
        date_str: str
        format_str: str
            format of date_str. Read more about datetime formats here:
            https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    # Returns:
        datetime_obj: dt.datetime object
    """
    datetime_obj = dt.datetime.strptime(date_str, format_str)
    
    return datetime_obj


def all_duplicate_index_rows(df):
    """Get all rows with duplicate indices, including the first row of the index.
    
    # Parameters/Returns:
        df/_: pd.DataFrame
    """
    return df[df.index.duplicated(keep=False)]


def time_match(row):
    """Ad-hoc row operation
    """
    if row['time'] != row.name.replace(microsecond=0):
        row['bid_depth'] = 0
        row['offer_depth'] = 0
    
    return row


def add_zeroes(df):
    """If the first row of the minute does not start on second 0, 
    - add most recent row as 0-second row.
    
    # Parameters/Returns:
        df: pd.DataFrame
    """
    # Get all rows where datetime second is zero
    zeroes = df[df.index.second == 0].index
    
    # Insert zero-second datetime rows if minute misses one
    time_zero_old = None
    df_c = df.copy()
    for i in range(1, len(df_c.index)):
        time = df_c.index[i]
        time_zero = time.replace(second=0)
        if time_zero not in zeroes:
            if time_zero_old == time_zero:
                continue
            if time_zero < df_c.index[i-1]:
                raise ValueError()    
            df.loc[time_zero] = df_c.iloc[i-1]
            time_zero_old = time_zero
            
    # Unscramble index
    df = df.sort_index()
    
    return df


def ohlc_resample(df, period='1T', format_str='%Y%m%dT%H%M%S'):
    """Resample netfonds posdump data. Made for 1T but should work for more.
    
    # Parameters:
        df: pd.DataFrame
            DataFrame of netfonds data
        period: str
            Frequency indicator for pandas
        format_str: str
            Datetime format. Read more about datetime formats here:
            https://docs.python.org/2/library/datetime.html#strftime-and-strptime-behavior
    # Returns:
        df_concat: pd.DataFrame
            Resampled and processed df
    """
    # Parse datetime index
    df['time'] = pd.to_datetime(df['time'], format=format_str)
    df.index = pd.DatetimeIndex(df['time'])
    df['time'] = df.index
    
    # If 0th seconds not in minute, fill with last datapoint
    df = add_zeroes(df)
    # If the row is added to a minute, set x_depth to 0 (Those bids/offers have already happened)
    df = df.apply(time_match, axis=1)
    # if prev in same second, millisecond to current
    df.index += pd.to_timedelta(df.groupby(df.index).cumcount(), unit='ms')
    
    # RS bid price as price on OHLC
    bid_rs = df['bid'].resample(period).ohlc()
    # RS total depth on OHLC
    bid_depth_total_rs = df['bid_depth_total'].resample(period).ohlc()
    # RS total depth on OHLC
    offer_depth_total_rs = df['offer_depth_total'].resample(period).ohlc()
    # RS spread on median
    spread_rs = (df['offer'] - df['bid']).resample(period).median()
    # RS on sum
    bid_depth_rs = df['bid_depth'].resample(period).sum()
    # Drop cell if sum is zero, zero cells are irrelevant in this case and equivalent to nan
    bid_depth_rs = bid_depth_rs[bid_depth_rs != 0]
    # RS on sum
    offer_depth_rs = df['offer_depth'].resample(period).sum()
    # Drop cell if sum is zero, zero cells are irrelevant in this case and equivalent to nan
    offer_depth_rs = offer_depth_rs[offer_depth_rs != 0]

    df_concat = pd.concat(
        [
            bid_rs, bid_depth_rs, bid_depth_total_rs,
            offer_depth_rs, offer_depth_total_rs, spread_rs
        ], 
        axis=1, 
    )
    # If all columns in a row are nan, drop row
    df_concat = df_concat.dropna(how='all')
    # Rare nan cases are replaced with 0
    df_concat = df_concat.fillna(0)
    
    # Column names
    keys = [
        'bid_open', 'bid_high', 'bid_low', 'bid_close',
        'bid_depth', 
        'bid_depth_total_open', 'bid_depth_total_high', 
        'bid_depth_total_low', 'bid_depth_total_close',
        'offer_depth', 
        'offer_depth_total_open', 'offer_depth_total_high', 
        'offer_depth_total_low', 'offer_depth_total_close',
        'spread'
    ]
    # Rename df
    df_concat.columns = keys

    return df_concat


def get_assets(tickerfile='assets/OSE_tickers.csv', sep=';'):
    tickers = pd.read_csv(tickerfile, sep=sep)
    return list(tickers['paper'])