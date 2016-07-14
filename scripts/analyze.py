import pandas as pd
import numpy as np
from sklearn.cross_validation import train_test_split

def clean():
       # read in torrent_data
       torrent_df = pd.read_csv('../data/torrent_data.csv', encoding='latin-1')
       torrent_df = torrent_df[['Title', 'Rated', 'Released', 'Runtime', 'Genre', 'Director', 'Actors', 'Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']]

       # convert released date to datetime
       torrent_df['Released'] = pd.to_datetime(torrent_df['Released'])
       torrent_df['Year'] = pd.DatetimeIndex(torrent_df['Released']).year
       torrent_df['Month'] = pd.DatetimeIndex(torrent_df['Released']).month

       # convert runtime to number
       torrent_df['Runtime'] = torrent_df['Runtime'].str.rstrip(' min')
       print('Data points of torrent_df = ' + str(len(torrent_df)))

       # read in financial data
       financial_df = pd.read_csv('../data/movie_dollars.csv', encoding='latin-1')
       financial_df = financial_df.rename(columns={'title':'Title', 'release_date':'Released', 'prod_budget':'Prod_Budget'})
       financial_df = financial_df[['Title', 'Released', 'Prod_Budget']]

       # convert released date to datetime
       financial_df['Released'] = pd.to_datetime(financial_df['Released'])
       print('Data points of financial_df = ' + str(len(financial_df)))

       # merge data frames
       data_df = pd.merge(financial_df, torrent_df, how='inner', on=['Title', 'Released'])
       print('Data points AFTER merge, BEFORE clean = ' + str(len(data_df)))

       # drop empty cells
       data_df = data_df.replace('', np.nan, regex=True)
       data_df =  data_df.dropna(how='any', subset=['Prod_Budget', 'Genre', 'Torrentz_Count'])

       # drop failed or non-numeric torrent count cells
       data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']] = data_df[['Pirate_Count', 'Torrentz_Count',
                                                                                  'Zoogle_Ver_Count']].convert_objects(convert_numeric=True)
       data_df =  data_df.dropna(how='any', subset=['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count'])
       print('Data points AFTER merge AND clean = ' + str(len(data_df)))

       # sum torrent counts
       data_df['Total_Torrents'] = data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']].sum(axis=1)

       # trim to pertinent columns
       data_df = data_df[['Title', 'Released', 'Year', 'Month', 'Rated', 'Runtime', 'Genre', 'Director', 'Actors', 'Total_Torrents']]

       train_df, test_df = train_test_split(data_df, train_size=0.80, random_state=1)

       # drop out that squeaky clean
       data_df.to_csv('../data/data.csv', sep=',', index=False)
       train_df.to_csv('../data/train_data.csv', sep=',', index=False)
       test_df.to_csv('../data/test_data.csv', sep=',', index=False)

clean()

if __name__ is '__main__':
       clean()