import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')

def clean():
       torrent_df = pd.read_csv('../data/torrent_data.csv', encoding='latin-1')
       torrent_df = torrent_df[['Title', 'Released', 'Rated', 'Genre', 'Director', 'Actors', 'Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']]
       # convert to datetime
       torrent_df['Released'] = pd.to_datetime(torrent_df['Released'])
       print('Data points of torrent_df = ' + str(len(torrent_df)))

       financial_df = pd.read_csv('../data/movie_dollars.csv', encoding='latin-1')
       financial_df = financial_df.rename(columns={'title':'Title', 'release_date':'Released', 'prod_budget':'Prod_Budget'})
       financial_df = financial_df[['Title', 'Released', 'Prod_Budget']]
       # convert to datetime
       financial_df['Released'] = pd.to_datetime(financial_df['Released'])
       print('Data points of financial_df = ' + str(len(financial_df)))

       # merge data frames
       data_df = pd.merge(financial_df, torrent_df, how='inner', on=['Title', 'Released'])
       print('Data points AFTER merge, BEFORE clean = ' + str(len(data_df)))

       # drop empty cells
       data_df = data_df.replace('', np.nan, regex=True)
       data_df =  data_df.dropna(how='any', subset=['Prod_Budget', 'Genre', 'Torrentz_Count'])

       # drop failed or non-numeric torrent count cells
       data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']] = data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']].convert_objects(convert_numeric=True)
       data_df =  data_df.dropna(how='any', subset=['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count'])
       print('Data points AFTER merge AND clean = ' + str(len(data_df)))

       # sum torrent counts
       data_df['Total_Torrents'] = data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']].sum(axis=1)
       # drop out that squeaky clean
       data_df.to_csv('../data/data.csv', sep=',', index=False)

clean()

if __name__ is '__main__':
       clean()