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
    :param name: data column name
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


# SUPPLY
# --------------------------------------------------------------------------------
def crude_oil_stocks(frequency="monthly", api_key=API_KEY, cushing=False):
    """
    Get the data from EIA.gov on crude oil stocks by month or year
    returns monthly stocks in MBBL
    crude oil stocks at tank farms and pipelines
    :param frequency: str, "monthly" or "annual"
    :param api_key: str, API key from EIA.gov
    :param cushing: bool, if True, returns Cushing, OK Ending Stocks of Crude Oil (Thousand Barrels), if False, return stocks
     at tank farms and pipelines for all 5 PADDs

    :return: pandas dataframe
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


def proved_nonprod_reserves(api_key=API_KEY):
    """
    proved non producing reserves in MMBBL
    annual data only
    :param api_key: str, api key from EIA.gov

    :return: pandas dataframe
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


def weekly_stocks(api_key=API_KEY, crude_only=True):
    """
    Weekly stocks of crude oil and petroleum products in MBBL
    :param api_key: str, api key from EIA.gov
    :param crude_only: bool, if True, only crude oil stocks are returned

    :return: pandas dataframe
    """
    if crude_only:
        product = 'EPC0'
    else:
        product = 'EP00'

    url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[duoarea][]=NUS&facets[product][]={product}&\
    facets[process][]=SAE&sort[0][column]=period&sort[0][direction]=desc&\
    offset=0&length=5000"

    df = get_request(url)
    df.rename(columns={'value': 'stocks'}, inplace=True)

    return df['stocks']


def spr_reserves(api_key=API_KEY):
    """
    Weekly SPR reserves in MBBL
    :param api_key: str, api key from EIA.gov

    :return: pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[series][]=WCSSTUS1&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='spr_reserves')

    return df


def ending_stocks(api_key=API_KEY, just_crude=True):
    """
    weekly ending stocks of crude oil or crude oil and petroleum products in MBBL
    :param api_key: str, api key from EIA.gov
    :param just_crude: bool, if True, only crude oil stocks are returned

    :return:pandas dataframe
    """
    if just_crude:
        product = "EPC0"
    else:
        product = "EP00"

    url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]={product}&facets[process][]=SAE&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='ending_stocks')

    return df


def mbbl_production(api_key=API_KEY, daily=False):
    """
    crude oil production in the USA for PADD regions (1-5) in MBBL or MBBL/D
    :param api_key: str, api key from EIA.gov
    :param daily: bool, if True, return daily production, else return monthly production

    :return: pandas dataframe
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


def imports(api_key=API_KEY, daily=False):
    url = f"https://api.eia.gov/v2/petroleum/move/imp/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[product][]=EPC0&facets[series][]=MCRIMUS1&\
    facets[series][]=MCRIMUS2&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    if daily:
        df = df[df['units'] == 'MBBL/D']
    else:
        df = df[df['units'] == 'MBBL']
    df = df['value'].rename('imports')
    return df


# DEMAND
# --------------------------------------------------------------------------------

def weekly_refinery_inputs(api_key=API_KEY):
    """"
    Refiner Net Input of Crude Oil (Thousand Barrels per Day) for PADDS 1-5
    :param api_key: str, api key from EIA.gov

    :return: pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/pnp/wiup/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]=EPC0&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='weekly_refinery_inputs')
    return df


def monthly_product_supplied(api_key=API_KEY):
    """
    U.S. Product Supplied of Crude Oil and Petroleum Products (Thousand Barrels per Day)
    :param api_key: str, api key from EIA.gov

    :return: pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/cons/psup/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[duoarea][]=NUS&facets[product][]=EP00&\
    facets[product][]=EPC0&facets[series][]=MTTUPUS2&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    df = df['value'].rename('product_supplied')
    return df


def imports_exports(api_key=API_KEY, only_crude=True):
    """
    Get the data from EIA.gov on crude oil imports and exports
    returns weekly imports and exports in MBBL/D
    :param api_key: str, API key from EIA.gov

    :return: pandas dataframe
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


def weekly_product_supplied(api_key=API_KEY):
    """
    USA Weekly product supplied in MBBL/D
    includes residual fuel oils, propane and propylene, other oils, kerosene-type jet fuel,
    distillate fuel oil, finished motor gasoline
    :param api_key: str, api key from EIA.gov

    :return: pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/cons/wpsup/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='weekly_product_supplied')

    return df


def gasoline_sales_end_user(api_key=API_KEY):
    """
    gasoline sales per month to end users in MGAL/D (total gasoline sales)
    :param api_key: str, api key from EIA.gov

    :return: pandas dataframe
    """
    url = f"https://api.eia.gov/v2/petroleum/cons/refmg/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[process][]=VTR&facets[duoarea][]=NUS&facets[product][]=EPM0&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    df = df['value']
    df = df.rename('gasoline_sales_end_user')

    return df


def gasoline_sales_resale(api_key=API_KEY):
    """
    gasoline sales per month to resellers in MGAL/D
    total includes bulk sales, DTW sales, and rack sales
    :param api_key: str, api key from EIA.gov

    :return: pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/cons/refmg/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[process][]=VBS&facets[process][]=VDS&\
    facets[process][]=VRK&facets[duoarea][]=NUS&facets[product][]=EPM0&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=True, name='gasoline_sales_resale')

    return df


def energy_consumption(api_key=API_KEY, end=None):
    """
    monthly energy consumption in the USA in quadrillion BTU
    :param api_key: str, api key from EIA.gov
    :param end: str, end date of data, format YYYY-MM, default is today

    :return: pandas dataframe
    """
    if end is None:
        end = pd.Timestamp.today().strftime('%Y-%m')

    url = f"https://api.eia.gov/v2/steo/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[seriesId][]=TETCFUEL&\
    end={today}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    df = df['value']
    df = df.rename('energy_consumption')

    return df


def refinery_net_input(api_key=API_KEY, daily=False):
    """
    monthly net inouts of crude oil to refineries in MBBL/D or MBBL
    :param api_key: str, api key from EIA.gov
    :param daily: bool, if True, returns monthly data in MBBL/D, if False, returns monthly data in MBBL

    :return: pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/pnp/inpt2/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[product][]=EPC0&facets[duoarea][]=NUS&\
    facets[process][]=YIY&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    if daily:
        df = df[df['units'] == 'MBBL/D']
    else:
        df = df[df['units'] == 'MBBL']
    df = df['value'].rename('refinery_net_input')
    return df


def exports(api_key=API_KEY, daily=False):
    url = f"https://api.eia.gov/v2/petroleum/move/exp/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[product][]=EPC0&facets[duoarea][]=NUS-Z00&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    if daily:
        df = df[df['units'] == 'MBBL/D']
    else:
        df = df[df['units'] == 'MBBL']
    df = df['value'].rename('exports')
    return df


# forecasts (provided by short term energy outlook on EIA.gov)
def crude_production_forecast(api_key=API_KEY, start='2000-01', end=None):
    """
    get STEO forecast for crude oil production in the USA in MBBL/D
    :param api_key: str, api key from EIA.gov
    :param start: str, start date of data, format YYYY-MM, default is 2000-01
    :param end: str, end date of data, format YYYY-MM, default is today

    :return: pandas dataframe
    """

    if end is None:
        end = pd.Timestamp.today().strftime('%Y-%m')

    url = f"https://api.eia.gov/v2/steo/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[seriesId][]=COPRPUS&start={start}&\
    end={end}&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    df = df['value']
    df = df.rename('crude_production_forecast')
    return df


def world_prod_cons(api_key=API_KEY, start='2000-01', end=None):
    """
    get STEO forecast for world liquid fuels production and consumption in millions of barrels per day
    :param api_key: str, api key from EIA.gov
    :param start: str, start date of data, format YYYY-MM, default is 2000-01
    :param end: str, end date of data, format YYYY-MM, default is today

    :return: pandas dataframe
    """
    if end is None:
        end = pd.Timestamp.today().strftime('%Y-%m')

    url = f"https://api.eia.gov/v2/steo/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[seriesId][]=PAPR_WORLD&\
    facets[seriesId][]=PATC_WORLD&start={start}&end={end}&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)

    production = df[df['seriesId'] == 'PAPR_WORLD']['value']
    consumption = df[df['seriesId'] == 'PATC_WORLD']['value']

    return production, consumption


def weekly_field_production(api_key=API_KEY):
    """
    get weekly field production in the USA in MBBL/D
    :param api_key: str, api key from EIA.gov

    :return: pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/sum/sndw/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]=EP00&facets[product][]=EPC0&\
    facets[duoarea][]=NUS&facets[duoarea][]=NUS-Z00&facets[series][]=WCRFPUS2&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url, group=False)
    df = df['value'].rename('weekly_field_production')
    return df
