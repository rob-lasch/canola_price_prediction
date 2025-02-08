import os

import pandas as pd

prod_df = pd.read_csv('../data/ag_data/production.csv')

pest_df = pd.read_csv('../data/ag_data/pesticide.csv')

fert_df = pd.read_csv('../data/ag_data/fertilizer.csv')

merge_col = ['Country', 'Year']

ag_df = pd.merge(prod_df, pest_df,  how='left', left_on=merge_col, right_on = merge_col)

ag_df = pd.merge(ag_df, fert_df,  how='left', left_on=merge_col, right_on = merge_col)

ag_df = ag_df.rename(columns={'Year': 'year', 'Country': 'country'})

temp_df = ag_df.loc[(ag_df['year']>2018) & (ag_df['year']<2024)]

top_producers = ag_df.groupby(['country'])['production_tonnes'].agg('sum')

top_producers.sort_values(ascending=False).head(30).index.to_list()

'''
I ran this then pulled out the the regions to manually make a list of the top 7 canola producers from 2019-2023
I also kept the world production for reference
'''

top_producing_countries = ['World',
                           'Canada',
                           'China',
                           'India',
                           'Germany',
                           'France',
                           'Australia',
                           'Poland']

ag_df = ag_df.loc[ag_df['country'].isin(top_producing_countries)]

ag_df.to_csv('../data/ag_data/top_producers.csv', index=False)