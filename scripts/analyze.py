import pandas as pd
import numpy as np

torrent_df = pd.read_csv('../data/torrent_data.csv', encoding='latin-1')
torrent_df = torrent_df[['Title', 'Released', 'Genre', 'Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']]
# convert to datetime
torrent_df['Released'] = pd.to_datetime(torrent_df['Released'])

financial_df = pd.read_csv('../data/movie_dollars.csv', encoding='latin-1')
financial_df = financial_df.rename(columns={'title':'Title', 'release_date':'Released', 'prod_budget':'Prod_Budget', 'dom_gross':'Dom_Gross', 'world_gross':'World_Gross'})
# convert to datetime
financial_df['Released'] = pd.to_datetime(financial_df['Released'])

# inner join to get uniques
data_df = pd.merge(financial_df, torrent_df, how='outer', on=['Title', 'Released'])
print(len(data_df))

# drop empty cells
data_df = data_df.replace('', np.nan, regex=True)
data_df =  data_df.dropna(how='any', subset=['Prod_Budget', 'Genre', 'Torrentz_Count'])

# drop failed or non-numeric torrent count cells
data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']] = data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']].convert_objects(convert_numeric=True)
data_df =  data_df.dropna(how='any', subset=['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count'])
print(len(data_df))
print(data_df.head(20))
# drop out that squeaky clean
data_df.to_csv('../data/data.csv', sep=',', index=False)