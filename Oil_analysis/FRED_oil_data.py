"""
retrieves data from FRED
https://fred.stlouisfed.org
"""
from full_fred.fred import Fred
import pandas as pd
from datetime import datetime

fred = Fred('FRED_key.txt')
today = datetime.now().strftime("%Y-%m-%d")


def to_numeric(val):
    """
    Convert the values in the dataframe to numeric
    :param
    val: str, value to convert to numeric
    :return
    numeric value
    """
    if val == '.':
        return 0
    else:
        return float(val)


def get_fred_data(series_id, start_date="2000-01-01"):
    """
    Get the data from FRED
    :param
    series_id: str, series id from FRED
    start_date: str, "YYYY-MM-DD"
    :return
    pandas dataframe
    """

    data = fred.get_series_df(series_id, observation_start=start_date, observation_end=today)
    data = data[['date', 'value']]
    data = data.set_index('date')
    data['value'] = data['value'].apply(to_numeric)

    return data
