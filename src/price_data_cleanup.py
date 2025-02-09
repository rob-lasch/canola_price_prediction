import pandas as pd

price_df = pd.read_csv('../data/ag_data/canola_prices_daily.csv')
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

price_df.to_csv('../data/ag_data/canola_prices_monthly.csv')
