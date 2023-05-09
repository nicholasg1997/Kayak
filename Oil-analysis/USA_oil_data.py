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


def get_request(url):
    """"
    returns request from EIA.gov
    :param
    url: str, url from EIA.gov
    :return
    pandas dataframe
    """

    r = requests.get(url)
    data = r.json()
    data = data['response']['data']

    df = pd.DataFrame(data)
    df['period'] = pd.to_datetime(df['period'])
    df = df.set_index('period')

    return df


def mbbl_production(frequency="monthly", api_key=API_KEY, start_date="2015-12"):
    """
    Get the data from EIA.gov on crude oil production my month or year
    returns monthly MBBL data
    :param 
    frequency: str, "monthly" or "annual"
    API_KEY: str, API key from EIA.gov
    start_date: str, "YYYY-MM"
    :return 
    pandas dataframe
    """

    url = f"https://api.eia.gov/v2/petroleum/crd/crpdn/data/?api_key={api_key}&\
    frequency={frequency}&start={start_date}&data[0]=value&sort[0][column]=period&\
    sort[0][direction]=desc&offset=0"

    df = get_request(url)

    df['barrels_per_month'] = df.apply(lambda x: int(x['value']) * 30 if x['units'] == 'MBBL/D'
    else int(x['value']), axis=1)

    df_mbbl = df['barrels_per_month']
    df_mbbl = df_mbbl.groupby('period').sum()
    df_mbbl = df_mbbl.sort_index(ascending=False)

    return df_mbbl


def crude_oil_stocks(frequency="monthly", api_key=API_KEY, start_date="2015-12"):
    """
    Get the data from EIA.gov on crude oil stocks by month or year
    returns monthly stocks in MBBL
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

    url = f"https://api.eia.gov/v2/petroleum/move/wkly/data/?api_key={API_KEY}&\
    frequency=weekly&data[0]=value&facets[product][]=EPC0&facets[process][]=EEX&\
    facets[process][]=IM0&sort[0][column]=period&sort[0][direction]=desc&offset=0&length=5000"

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


def oil_exports(api_key=API_KEY, group=True):
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

