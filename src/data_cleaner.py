import os

import pandas as pd

from canola_price_prediction.src.hardcodes import top_producing_countries

# import data from files
prod_df = pd.read_csv('../data/input_data/production.csv')
pest_df = pd.read_csv('../data/input_data/pesticide.csv')
fert_df = pd.read_csv('../data/input_data/fertilizer.csv')
weather_df = pd.read_csv('../data/input_data/weather_data.csv')
location_df  = pd.read_csv('../data/input_data/weather_locations.csv')
price_df = pd.read_csv('../data/input_data/canola_prices_daily.csv')


# merge crop_data
merge_col = ['Country', 'Year']
ag_df = pd.merge(prod_df, pest_df,  how='left', left_on=merge_col, right_on = merge_col)
ag_df = pd.merge(ag_df, fert_df,  how='left', left_on=merge_col, right_on = merge_col)
ag_df = ag_df.rename(columns={'Year': 'year', 'Country': 'country'})
temp_df = ag_df.loc[(ag_df['year']>2018) & (ag_df['year']<2024)]
top_producers = ag_df.groupby(['country'])['production_tonnes'].agg('sum')
top_producers.sort_values(ascending=False).head(30).index.to_list()
ag_df = ag_df.loc[ag_df['country'].isin(top_producing_countries)]

#ag_df.to_csv('../data/model_data/top_producers.csv', index=False)

# import and process weather data
weather_df[['year', 'month']] = weather_df['month'].str.split('-', expand=True)
weather_df = weather_df.groupby(['year', 'location_ref']).agg(list)
weather_df = weather_df.reset_index()
weather_df = weather_df[weather_df['year'] != '2025']
monthly_cols = list(weather_df.columns)
for col in ['year', 'location_ref', 'month']:
    monthly_cols.remove(col)
for col in monthly_cols:
    # Expand the list into new columns
    new_cols = weather_df[col].apply(pd.Series)

    # Rename the new columns (optional)
    new_cols.columns = [col + '_01', col + '_02', col + '_03', col + '_04',
                        col + '_05', col + '_06', col + '_07', col + '_08',
                        col + '_09', col + '_10', col + '_11', col + '_12']

    weather_df = pd.concat([weather_df, new_cols], axis=1)

monthly_cols.append('month')
weather_df.drop(monthly_cols, axis=1, inplace=True)
weather_df['year'] = weather_df['year'].astype(int)


ag_df = ag_df.merge(location_df, how='left', on='country')
ag_df = ag_df[ag_df['country'] != 'World']
ag_df.drop(['latitude','longitude','lat_num','lon_num'], axis=1, inplace=True)
ag_df = ag_df.merge(weather_df, how='left', on=['year', 'location_ref'])
ag_df.to_csv('../data/model_data/prod_data.csv')


price_df['Date'] = pd.to_datetime(price_df['Date'])
price_df = price_df.replace({',': ''}, regex=True)
price_df['month'] = price_df['Date'].dt.strftime('%Y-%m')
price_df.drop(columns=['Date'], inplace=True)
price_df.rename(columns={'Close': 'close', 'Open': 'open',
                      'High': 'high', 'Low': 'low',
                      'Volume': 'volume'}, inplace=True)
float_cols = ['close', 'open', 'high', 'low', 'volume']
for col in float_cols:
    price_df[col] = price_df[col].astype(float)
price_df = price_df.groupby('month').agg({'close':'mean',
                                          'open':'mean',
                                          'high':'max',
                                          'low':'min',
                                          'volume':'sum'})
price_df.to_csv('../data/model_data/canola_prices_monthly.csv')