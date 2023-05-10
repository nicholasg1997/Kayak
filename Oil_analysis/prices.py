"""
program to grab real time (or as close to real time as possible)
oil prices, copper prices and other commodities
"""

import pandas as pd
import os
from dotenv import load_dotenv
import requests

load_dotenv()


class Prices:
    FMP_URL = 'https://financialmodelingprep.com/api/v3'

    def __init__(self):
        self.load_keys()

    def short_term_data(self, real_time=False, frequency='daily', contract='CLUSD'):
        """
        get short term data from financialmodelingprep.com
        wti crude oil is CLUSD
        brent crude oil is BZUSD
        :param
        real_time: bool, if True, get real time data
        frequency: str, 'daily', '4hour', '1hour', '30min', '15min', '5min', '1min'
        contract: str, can be any commodity listed on FMP
        :return
        pandas dataframe
        """

        assert frequency in ['daily', '4hour', '1hour', '30min', '15min', '5min', '1min'], \
            "frequency must be one of 'daily', '4hour', '1hour', '30min', '15min', '5min', '1min'"

        if real_time:
            url = f"{self.FMP_URL}/quote/{contract}?apikey={self.FMP_key}"
            real_data = self.get_request(url)
            real_data_df = pd.DataFrame(real_data)
            real_data_df = real_data_df.T.iloc[2:].dropna(axis=0)
            return real_data_df

        if frequency == 'daily':
            url = f"{self.FMP_URL}/historical-price-full/CLUSD?apikey={self.FMP_key}"
            daily_data = self.get_request(url)['historical']
            daily_data_df = pd.DataFrame(daily_data)
            daily_data_df['date'] = pd.to_datetime(daily_data_df['date'])
            daily_data_df.set_index('date', inplace=True)
            return daily_data_df

        else:
            url = f'{self.FMP_URL}/historical-chart/{frequency}/{contract}?apikey={self.FMP_key}'
            intraday = self.get_request(url)
            intraday_df = pd.DataFrame(intraday)
            intraday_df['date'] = pd.to_datetime(intraday_df['date'])
            intraday_df.set_index('date', inplace=True)
            return intraday_df

    def long_term_data(self):
        pass

    def other_oil(self, location):
        pass

    def copper(self):
        pass

    def get_request(self, url):
        r = requests.get(url)
        data = r.json()
        return data

    def load_keys(self):
        """load keys from .env file"""
        self.EIA_key = os.getenv("EIA_TOKEN")
        self.FRED_key = os.getenv("FRED_API_KEY")
        self.FMP_key = os.getenv("FMP_KEY")
        self.no_key = os.getenv("NO_KEY")

        if self.EIA_key is None:
            print("No EIA key found")

        if self.FRED_key is None:
            print("No FRED key found")

        if self.FMP_key is None:
            print("No FMP key found")


p = Prices()
data = p.short_term_data(real_time=False, frequency='daily', contract='BZUSD')
print(data)
