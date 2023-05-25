import numpy as np
import pandas as pd
import USA_oil_data as oil
import prices
from FRED_oil_data import get_fred_data
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
#sns.set()
sns.set_style("whitegrid")

weekly_stocks = oil.weekly_stocks()
spr = oil.spr_reserves()


def total_stocks(weekly_stocks=weekly_stocks, spr=spr):

    merged = pd.merge(weekly_stocks, spr, on='period')
    merged.sort_index(inplace=True)

    ticks = [x for x in range(0, 1400000, 200000)]

    plt.plot(merged)

    plt.yticks(ticks)
    plt.ticklabel_format(axis='y', style='plain')
    for t in ticks:
        plt.axhline(t, color='black', alpha=0.1)

    plt.legend(merged.columns)
    plt.title("Total US Crude Stocks vs SPR")
    plt.xlabel('Date')
    plt.ylabel('MBBL')
    plt.savefig('charts/total_stocks.png', dpi=300)


def stocks_by_year(weekly_stocks=weekly_stocks):

    weekly_stocks = weekly_stocks.to_frame()
    weekly_stocks['year'] = weekly_stocks.index.year
    weekly_stocks['month'] = weekly_stocks.index.month
    weekly_stocks = weekly_stocks[weekly_stocks['year'] > 2015]
    monthly_average = weekly_stocks.groupby('month').mean()
    grouped = weekly_stocks.groupby('year')
    fig, ax = plt.subplots()

    for name, group in grouped:
        ax.plot(group['month'], group['stocks'], label=name)
    ax.plot(monthly_average.index, monthly_average['stocks'], label='average')
    plt.ticklabel_format(axis='y', style='plain')
    plt.legend(loc='upper center', bbox_to_anchor=(0.5, -0.05), fancybox=True, shadow=True, ncol=5)
    # add horizontal lines
    ticks = [x for x in range(800000, 1300000, 100000)]
    for t in ticks:
        plt.axhline(t, color='black', alpha=0.1)

    plt.title('Crude Stocks by Year')
    plt.xlabel('Month')
    plt.ylabel('MBBL')
    plt.savefig('charts/stocks_by_year.png', dpi=300)