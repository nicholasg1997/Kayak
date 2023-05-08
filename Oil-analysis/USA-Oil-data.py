"""
WTI oil production
source EIA.gov
"""

import pandas as pd
import requests
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("EIA_TOKEN")


def MBBL_production (frequency="monthly", API_KEY=API_KEY, start_date="2015-12"):
    """""
    Get the data from EIA.gov on crude oil production my month or year
    returns monthly MBBL data
    :param 
    frequency: str, "monthly" or "annual"
    API_KEY: str, API key from EIA.gov
    start_date: str, "YYYY-MM"
    :return 
    pandas dataframe
    """""

    url = f"https://api.eia.gov/v2/petroleum/crd/crpdn/data/?api_key={API_KEY}&\
    frequency={frequency}&start={start_date}&data[0]=value&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0"

    r = requests.get(url)
    data = r.json()
    data = data['response']['data']

    df = pd.DataFrame(data)

    df['period'] = pd.to_datetime(df['period'])
    df = df.set_index('period')

    df['barrels_per_month'] = df.apply(lambda x: int(x['value']) * 30 if x['units'] == 'MBBL/D'
                                       else int(x['value']), axis=1)

    df_MBBL = df['barrels_per_month']
    df_MBBL = df_MBBL.groupby('period').sum()
    df_MBBL = df_MBBL.sort_index(ascending=False)

    return df_MBBL

data = MBBL_production()
print(data)









