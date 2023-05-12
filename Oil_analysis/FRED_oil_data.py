"""
retrieves data from FRED
https://fred.stlouisfed.org
"""
from full_fred.fred import Fred
import pandas as pd
from datetime import datetime





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


def get_fred_data(series_id, start_date=None, end_date=None, api_key=None):
    """
    Get the data from FRED
    :param
    series_id: str, series id from FRED
    start_date: str, "YYYY-MM-DD"
    api_key: str, API key from FRED
    :return
    pandas dataframe
    """

    if api_key is None:
        fred = Fred('FRED_key.txt')

    else:
        fred = Fred(api_key)

    if start_date is None:
        start_date = "2000-01-01"

    if end_date is None:
        end_date = datetime.now().strftime("%Y-%m-%d")

    data = fred.get_series_df(series_id, observation_start=start_date, observation_end=end_date)
    data = data[['date', 'value']]
    data = data.set_index('date')
    data['value'] = data['value'].apply(to_numeric)

    return data
