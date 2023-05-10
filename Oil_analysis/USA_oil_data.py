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


def get_request(url):
    """
    Returns a pandas dataframe from an API request to EIA.gov
    :param url: str, URL for API request
    :return: pandas dataframe
    """
    response = requests.get(url)
    data = response.json()['response']['data']
    df = pd.DataFrame(data)
    df['period'] = pd.to_datetime(df['period'])
    df = df.set_index('period')
    return df


def mbbl_production(frequency="monthly", api_key=API_KEY, end_date=today, years=5):
    """
    Get the data from EIA.gov on crude oil production my month or year
    returns monthly MBBL data
    :param 
    frequency: str, "monthly" or "annual"
    API_KEY: str, API key from EIA.gov
    start_date: str, "YYYY-MM"
    years: int, number of years to retrieve data (must be divisible by 5)
    :return 
    pandas dataframe
    """
    assert years % 5 == 0, "years must be divisible by 5"

    #1000 for every year, increment in 5000
    offset = 0
    master_df = pd.DataFrame()

    while years > 0:

        url = f"https://api.eia.gov/v2/petroleum/crd/crpdn/data/?api_key={api_key}&\
        frequency={frequency}&end={end_date}&data[0]=value&sort[0][column]=period&\
        sort[0][direction]=desc&offset={offset}&length=5000"

        df = get_request(url)

        df['barrels_per_month'] = df.apply(lambda x: int(x['value']) * 30 if x['units'] == 'MBBL/D'
        else int(x['value']), axis=1)

        df_mbbl = df['barrels_per_month'].groupby('period').sum().sort_index(ascending=False)
        offset += 5000
        years -= 5

        master_df = pd.concat([master_df, df_mbbl]).drop_duplicates()

    master_df = master_df.rename(columns={0: 'barrels'})
    master_df.index.name = 'period'
    master_df = master_df.groupby(master_df.index).sum()

    return master_df


def crude_oil_stocks(frequency="monthly", api_key=API_KEY, start_date="2000-12"):
    """
    Get the data from EIA.gov on crude oil stocks by month or year
    returns monthly stocks in MBBL
    crude oil stocks at tank farms and pipelines
    :param 
    frequency: str, "monthly" or "annual"
    API_KEY: str, API key from EIA.gov
    start_date: str, "YYYY-MM"
    :return 
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/stoc/cu/data/?api_key={api_key}&\
        frequency={frequency}&start={start_date}&data[0]=value&sort[0][column]=period&\
        sort[0][direction]=desc&offset=0"

    df = get_request(url)

    storage_data = df['value']
    storage_data = storage_data.rename('storage')
    storage_data = storage_data.groupby('period').sum()
    storage_data = storage_data.sort_index(ascending=False)

    return storage_data


def imports_exports(api_key=API_KEY):
    """
    Get the data from EIA.gov on crude oil imports and exports
    returns weekly imports and exports in MBBL
    :param
    API_KEY: str, API key from EIA.gov
    :return
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/move/wkly/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[series][]=WCREXUS2&facets[series][]=WCRIMUS2&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)

    exports = df[df['process-name'].str.contains('exports', case=False)]['value'].rename('exports')
    imports = df[df['process-name'].str.contains('imports', case=False)]['value'].rename('imports')

    return imports, exports


def oil_imports(api_key=API_KEY, group=True):
    """
    Crude oil imports by country to destination,
    includes type, grade, quantity. Source: EIA-814 Interactive data
    product: www.eia.gov/petroleum/imports/companylevel/
    :param
    api_key: str, api key from EIA.gov
    :return:
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/crude-oil-imports/data/?api_key={api_key}&\
    frequency=monthly&data[0]=quantity&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)

    if group:
        df = df.groupby('period').sum()
        df = df.sort_index(ascending=False)
        df = df['quantity']
        df = df.rename('quantity_imported')

    return df


def oil_exports(api_key=API_KEY):
    """
    Crude oil exports by country to destination in MBBL
    :param
    api_key: str, api key from EIA.gov
    :return
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/move/exp/data/?api_key={api_key}&\
    frequency=monthly&data[0]=value&facets[product][]=EP00&\
    facets[product][]=EPC0&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)

    df['barrels_per_month'] = df.apply(lambda x: int(x['value']) * 30 if x['units'] == 'MBBL/D'
    else int(x['value']), axis=1)

    export = df['barrels_per_month']
    export = export.groupby('period').sum()
    export = export.sort_index(ascending=False)

    return export


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
    frequency=annual&data[0]=value&facets[product][]=EPC0&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    r = requests.get(url)
    data = r.json()
    data = data['response']['data']

    df = pd.DataFrame(data)
    df = df.set_index('period')['value'].dropna().rename('proved_nonprod_reserves')
    data = df.groupby('period').sum()

    return data


def weekly_stocks(api_key=API_KEY):
    """
    Weekly stocks of crude oil and petroleum products in MBBL

    :param api_key:
    :return:
    """

    url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[series][]=WCRSTUS1&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=500"

    df = get_request(url)
    #df = df.groupby('period').sum()
    df = df.sort_index(ascending=False)
    df = df['value']
    df = df.rename('weekly_stocks')

    return df


def weekly_product_supplied(api_key=API_KEY):

    url = f"https://api.eia.gov/v2/petroleum/cons/wpsup/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)
    df = df.groupby('period').sum()
    df = df.sort_index(ascending=False)
    df = df['value']
    df = df.rename('weekly_product_supplied')

    return df


def spr_reserves(api_key=API_KEY):

    url = f"https://api.eia.gov/v2/petroleum/stoc/wstk/data/?api_key={api_key}&\
    frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[series][]=WCSSTUS1&\
    sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

    df = get_request(url)
    df = df['value']
    df = df.rename('spr_reserves')

    return df

