"""
WTI oil production
source EIA.gov
"""

import os

import pandas as pd
import requests
from dotenv import load_dotenv

load_dotenv()
API_KEY = os.getenv("EIA_TOKEN")

today = pd.Timestamp.today().strftime("%Y-%m")


def get_request(url, group=False, name='value'):
    """
    Returns a pandas dataframe from an API request to EIA.gov
    :param url: str, URL for API request
    :param group: bool, if True, group by period
    :return: pandas dataframe
    """
    response = requests.get(url)
    data = response.json()['response']['data']
    df = pd.DataFrame(data)
    df['period'] = pd.to_datetime(df['period'])
    df = df.set_index('period')

    if group:
        df = df.groupby('period').sum()['value']
        df = df.rename(name)

    return df


def crude_oil_stocks(frequency="monthly", api_key=API_KEY, cushing=False):
    """
    Get the data from EIA.gov on crude oil stocks by month or year
    returns monthly stocks in MBBL
    crude oil stocks at tank farms and pipelines
    :param 
    frequency: str, "monthly" or "annual"
    API_KEY: str, API key from EIA.gov
    cushing: bool, if True, returns Cushing, OK Ending Stocks of Crude Oil (Thousand Barrels), if False, return stocks
     at tank farms and pipelines for all 5 PADDs
    :return 
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/stoc/cu/data/?api_key={api_key}&\
    frequency={frequency}&data[0]=value&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)

    if cushing:
        df = df[df['area-name'] == 'NA']
    else:
        df = df[df['area-name'] != 'NA']

    storage_data = df['value']
    storage_data = storage_data.rename('storage')
    storage_data = storage_data.groupby('period').sum()
    storage_data = storage_data.sort_index(ascending=False)

    return storage_data


def imports_exports(api_key=API_KEY, only_crude=True):
    """
    Get the data from EIA.gov on crude oil imports and exports
    returns weekly imports and exports in MBBL/D
    :param
    API_KEY: str, API key from EIA.gov
    :return
    pandas dataframe
    """
    if only_crude:
        url = f"https://api.eia.gov/v2/petroleum/move/wkly/data/?api_key={api_key}&\
            frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[series][]=WCREXUS2&facets[series][]=WCRIMUS2&\
            sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"
    else:
        url = f"https://api.eia.gov/v2/petroleum/move/wkly/data/?api_key={api_key}&\
        frequency=weekly&data[0]=value&facets[product][]=EP00&facets[series][]=WTTEXUS2&\
        facets[series][]=WTTIMUS2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)

    exports = df[df['process-name'].str.contains('exports', case=False)]['value'].rename('exports')
    imports = df[df['process-name'].str.contains('imports', case=False)]['value'].rename('imports')

    return imports, exports


def proved_nonprod_reserves(api_key=API_KEY):
    """
    proved non producing reserves in MMBBL
    annual data only
    :param
    api_key: str, api key from EIA.gov
    :return
    pandas dataframe
    """
    url = f"https://api.eia.gov/v2/petroleum/crd/nprod/data/?api_key={api_key}&\
    frequency=annual&data[0]=value&facets[product][]=EPC0&facets[duoarea][]=NUS&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    r = requests.get(url)
    data = r.json()['response']['data']
    df = pd.DataFrame(data)
    df = df.set_index('period')
    df = df['value']
    df = df.rename('proved_nonprod_reserves')

    return df


def weekly_stocks(api_key=API_KEY):
    """
    Weekly stocks of crude oil and petroleum products in MBBL

    :param api_key: str, api key from EIA.gov
    :return:
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[series][]=WCRSTUS1&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)
    df.rename(columns={'value': 'stocks'}, inplace=True)

    return df['stocks']


def weekly_product_supplied(api_key=API_KEY):
    url = f"https://api.eia.gov/v2/petroleum/cons/wpsup/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='weekly_product_supplied')

    return df


def spr_reserves(api_key=API_KEY):
    url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[series][]=WCSSTUS1&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='spr_reserves')

    return df


def ending_stocks(api_key=API_KEY):
    url = f"https://api.eia.gov/v2/petroleum/stoc/typ/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[product][]=EPC0&facets[process][]=SAE&\
    facets[series][]=MCRSTUS1&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='ending_stocks')

    return df


def product_supplied(api_key=API_KEY):
    """
    total crude oil and petroleum product supplied by month in MBBL
    :param api_key: str, api key from EIA.gov
    :return:
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/cons/psup/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[duoarea][]=NUS&facets[product][]=EP00&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)
    df = df[df['units'] == 'MBBL']
    df = df.groupby('period').sum()['value']
    df.rename('product_supplied', inplace=True)

    return df


def gasoline_sales_end_user(api_key=API_KEY):
    """
    gasoline sales per month to end users in MGAL/D (total gasoline sales)
    :param api_key:
    :return:
    """
    url = f"https://api.eia.gov/v2/petroleum/cons/refmg/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[process][]=VTR&facets[duoarea][]=NUS&facets[product][]=EPM0&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)

    return df


def gasoline_sales_resale(api_key=API_KEY):
    """
    gasoline sales per month to resellers in MGAL/D
    total includes bulk sales, DTW sales, and rack sales
    :param api_key:
    :return:
    """

    url = f"https://api.eia.gov/v2/petroleum/cons/refmg/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[process][]=VBS&facets[process][]=VDS&\
    facets[process][]=VRK&facets[duoarea][]=NUS&facets[product][]=EPM0&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='gasoline_sales_resale')

    return df


def mbbl_production(api_key=API_KEY, daily=False):
    """
    crude oil production in the USA for PADD regions (1-5) in MBBL or MBBL/D
    :param
    api_key: str, api key from EIA.gov
    daily: bool, if True, return daily production, else return monthly production
    :return
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/crd/crpdn/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[duoarea][]=R10&facets[duoarea][]=R20&\
    facets[duoarea][]=R30&facets[duoarea][]=R40&facets[duoarea][]=R50&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    if daily:
        df = df[df['units'] == 'MBBL/D']
    else:
        df = df[df['units'] == 'MBBL']
    df = df.groupby('period').sum()['value']
    df.rename('production', inplace=True)

    return df

# TODO: get data from the following url on consumption

url = "https://api.eia.gov/v2/total-energy/data/?frequency=monthly&\
data[0]=value&facets[msn][]=COSQPUS&facets[msn][]=DFACPUS&facets[msn][]=DFCCPUS&facets[msn][]=DFICPUS&\
facets[msn][]=DFRCPUS&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

data = proved_nonprod_reserves()
print(data)