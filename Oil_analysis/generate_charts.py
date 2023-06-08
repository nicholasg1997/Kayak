import numpy as np
import pandas as pd
import USA_oil_data as oil
import prices
from FRED_oil_data import get_fred_data
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns

# sns.set()
sns.set_style("whitegrid")


class Charting:
    def __init__(self):
        self.today = pd.to_datetime('today').date()
        self.forecast_start_date = '2015-01-01'
        self.forecast_end_date = '2025-01-01'
        self.start_date = '2022-01-01'
        self.only_crude = True
        self.save_path = 'charts/'
        self.get_data()

    def get_data(self):
        self.weekly_stocks = oil.weekly_stocks()
        self.spr = oil.spr_reserves()
        self.imports, self.exports = oil.imports_exports(only_crude=self.only_crude)
        self.net_imports = self.imports - self.exports
        self.net_imports = self.net_imports.rename('net_imports')
        self.forecast = oil.crude_production_forecast(end=self.forecast_end_date, start=self.forecast_start_date)
        self.prod, self.cons = oil.world_prod_cons(end=self.forecast_end_date, start=self.forecast_start_date)
        self.product_supplied = oil.weekly_product_supplied()
        self.refinery_inputs = oil.weekly_refinery_inputs()
        self.production = oil.weekly_field_production().sort_index()

    def total_stocks(self):
        weekly_stocks = self.weekly_stocks.copy()
        spr = self.spr.copy()

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
        plt.savefig(f'{self.save_path}/total_stocks_{self.today}.png', dpi=300)
        del weekly_stocks
        del spr

    def stocks_by_year(self):
        weekly_stocks = self.weekly_stocks.copy()

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
        plt.savefig(f'{self.save_path}/stocks_by_year_{self.today}.png', dpi=300)
        del weekly_stocks

    def weekly_stock_change(self):
        weekly_stocks = self.weekly_stocks.copy()

        weekly_stocks = weekly_stocks.to_frame()

        weekly_stocks['year'] = weekly_stocks.index.year
        weekly_stocks['month'] = weekly_stocks.index.month

        weekly_stocks['diff'] = weekly_stocks['stocks'].diff(-1)
        weekly_stocks = weekly_stocks[weekly_stocks['year'] >= 2018]

        week_info = weekly_stocks.index.isocalendar()
        week_number = week_info['week']

        merged_df = pd.merge(week_number, weekly_stocks, left_index=True, right_index=True)
        grouped = merged_df.groupby('year')

        weeks = grouped['week'].unique()
        years = len(grouped['year'].unique())
        width = 0.8 / years

        pos = 0
        width = 0.15
        fig, ax = plt.subplots(figsize=(15, 8))
        for name, group in grouped:
            position = group['week']
            position = position[position < 53]

            data = group['diff'] / 7
            data = data[:52]
            ax.bar(position + pos * width, data, width=width, label=name)
            pos += 1
        plt.legend()
        plt.xlabel('Week')
        plt.ylabel('MBBL/D')
        plt.title('Change in Stocks (YoY) by week (Including SPR)')
        plt.savefig(f'{self.save_path}/weekly_stock_change_{self.today}.png', dpi=300)
        del weekly_stocks

    def imp_exp(self):
        imports = self.imports.copy()
        exports = self.exports.copy()

        imports_exports = pd.merge(imports, exports, on='period')

        imports_exports['year'] = imports_exports.index.year
        imports_exports = imports_exports[imports_exports['year'] > 2005]
        imports_exports['net'] = imports_exports['imports'] - imports_exports['exports']

        plt.plot(imports_exports[['imports', 'exports', 'net']].rolling(12).mean())
        plt.legend(imports_exports[['imports', 'exports', 'net']])
        plt.axhline(0, color='black', alpha=0.5)
        plt.xlabel('Year')
        plt.ylabel('MBBL/D')
        plt.title('Imports, Exports and Net Imports')
        plt.savefig(f'{self.save_path}/net_imports_{self.today}.png', dpi=300)
        del imports
        del exports

    def crude_production_forecast(self):
        fig, ax = plt.subplots(figsize=(10, 5))
        self.forecast.plot(ax=ax)

        plt.axvline(pd.to_datetime('today'), color='black', alpha=0.5)
        plt.text(pd.to_datetime('today'), 9, 'today', ha='right', va='top', color='black')

        plt.title('Crude Production Forecast')
        plt.xlabel('Date')
        plt.ylabel('Production (millions of MBBL)')
        plt.savefig(f'{self.save_path}/crude_production_forecast_{self.today}.png', dpi=300)

    def world_prod_cons(self):
        fig, ax = plt.subplots(figsize=(10, 5))
        self.prod.plot(ax=ax, label='Production')
        self.cons.plot(ax=ax, label='Consumption')

        plt.axvline(pd.to_datetime('today'), color='black', alpha=0.5)
        plt.text(pd.to_datetime('today'), 85, 'today', ha='right', va='top', color='black')

        plt.title('World liquid fuels production and consumption')
        plt.xlabel('Date')
        plt.ylabel('millions of barrels per day')

        plt.legend()

        plt.savefig(f'{self.save_path}/world_prod_cons_{self.today}.png', dpi=300)

    def supply_injections(self):
        stocks = self.weekly_stocks.copy().to_frame() / 7
        stocks['change'] = stocks['stocks'].diff(-1)
        stocks['Stock_change'] = stocks['change'].diff(-52)
        stocks = stocks.sort_index()

        product_supplied = self.product_supplied.copy() * -1
        net_imports = self.net_imports.copy()
        production = self.production.copy()

        master_df = pd.merge(product_supplied, net_imports, on='period')
        master_df = pd.merge(master_df, production, on='period')

        changes = master_df.diff(52)

        sd_data = stocks['Stock_change'].rename('Storage Injections')

        changes = changes[self.today - pd.DateOffset(years=1):self.today + pd.DateOffset(days=1)]
        sd_data = sd_data[self.today - pd.DateOffset(years=1):self.today + pd.DateOffset(days=1)]

        changes.rename(columns={'weekly_product_supplied': 'Consumption', 'net_imports': 'Net Imports',
                                'weekly_field_production': 'Production'}, inplace=True)

        changes.rename(columns={'weekly_product_supplied': 'Consumption', 'net_imports': 'Net Imports',
                                'weekly_field_production': 'Production'}, inplace=True)
        # %%
        sd_data.index = pd.to_datetime(sd_data.index)

        ax = sd_data.plot.bar(ylabel="MBBL", xlabel="Date", figsize=(15, 8),
                              color='black', position=0, width=0.3)

        changes.plot.bar(stacked=True, sharex=True, ax=ax, position=1, width=0.3)

        ax.xaxis.set_major_formatter(plt.FixedFormatter(sd_data.index.strftime('%Y-%m-%d')))

        plt.title('change in Storage Injections by Components (YOY)')
        plt.legend(loc='upper center', bbox_to_anchor=(0.5, 0.05), ncol=5)
        plt.ylabel('YoY Change (MBBL/D)')
        plt.xlabel('date')
        plt.savefig(f'{self.save_path}/supply_injections_{self.today}.png', dpi=300)
        del stocks
        del product_supplied
        del net_imports
        del production

    def generate_all_charts(self):
        self.crude_production_forecast()
        self.world_prod_cons()
        self.supply_injections()
        self.weekly_stock_change()
        self.imp_exp()

if __name__ == '__main__':
    charts = Charting()
    charts.generate_all_charts()






