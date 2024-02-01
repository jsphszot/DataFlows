import datetime as dt
import numpy as np
import pandas as pd

def printnow(start=True):
    if start:
        return print(f'started running at {dt.datetime.now()}')
    else:
        return print(f'sfinished running at {dt.datetime.now()}')

def read_file(path):
    fd = open(path, 'r')
    file = fd.read()
    fd.close()
    return file

import json
def read_json(path):
    file=read_file(path)
    jsond=json.loads(file)
    return jsond

def fix_date_cols(df, tz='UTC'):
    """
    https://stackoverflow.com/questions/66664404/snowflake-write-pandas-is-not-inserting-dates-correctly
    https://stackoverflow.com/a/70834485
    """
    cols = df.select_dtypes(include=['datetime64[ns]']).columns
    for col in cols:
        df[col] = df[col].dt.tz_localize(tz)
    return df

def cols_to_object(df):
    cols = df.select_dtypes(include=['int64', 'float64']).columns
    for col in cols:
        df[col] = df[col].astype('object')
    return df


def df_rolling(df, gby, window, min_periods):
    df_rolling=df.set_index(gby)\
        .sort_values(gby, ascending=[True])\
        .rolling(window, min_periods).mean()\
        .sort_values(gby, ascending=[False])\
        .reset_index().iloc[0:-window]
    return df_rolling

def df_rolling_nonan(df, gby, window, min_periods):
    # removes 0s from rolling calculation
    df_rolling_nonan=df.set_index(gby)\
        .sort_values(gby, ascending=[True])\
        .replace(0, np.nan)\
        .rolling(window, min_periods).mean(skipna=True)\
        .sort_values(gby, ascending=[False])\
        .reset_index()\
        .replace(np.nan,0).iloc[0:-window]
    return df_rolling_nonan


def print_correlation(df, col1, col2):
    corr=df[col1].astype(float).corr(df[col2].astype(float))
    print("Correlation between ", col1, " and ", col2, "is: ", round(corr, 2))


def print_time(text=''):
    print(f"{text} {dt.datetime.now()}")    


def outpute_ranges(filldate='2023-03-02'):
    """
    returns how many days back a date (YYYY-MM-DD) happened. Use as start/end range for looping processes. Other end of range can be used as output+int
    """
    spec_date=(dt.datetime.today()-dt.datetime.strptime(filldate, '%Y-%m-%d')).days
    # rng_start=spec_date
    return spec_date
