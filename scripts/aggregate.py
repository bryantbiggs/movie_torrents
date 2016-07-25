import pandas as pd
import numpy as np

def aggregate():
       # files
       tor_file = '../data/torrents.csv'
       budget_file = '../data/budget.csv'
       merge_drop_file = '../data/merge_drop.csv'
       write_file = '../data/data.csv'

       # read in torrent_data
       torrent_df = pd.read_csv(tor_file, encoding='latin-1')
       torrent_df = torrent_df[['Title', 'Rated', 'Released', 'Runtime', 'Genre', 'Director',
                                'Actors', 'Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']]

       # convert released date to datetime
       torrent_df['Released'] = pd.to_datetime(torrent_df['Released'])
       torrent_df['Year'] = pd.DatetimeIndex(torrent_df['Released']).year
       torrent_df['Month'] = pd.DatetimeIndex(torrent_df['Released']).month

       # convert runtime to number
       torrent_df['Runtime'] = torrent_df['Runtime'].str.rstrip(' min')

       # read in budget data
       financial_df = pd.read_csv(budget_file, encoding='latin-1')
       financial_df = financial_df[['Title', 'Released', 'Prod_Budget', 'Dom_Gross', 'World_Gross']]

       # convert released date to datetime
       financial_df['Released'] = pd.to_datetime(financial_df['Released'])

       # standardize titles
       title_standardize(torrent_df, 'Title')
       title_standardize(financial_df, 'Title')

       # merge data frames
       data_df = pd.merge(financial_df, torrent_df, how='inner', on=['Title', 'Released'])

       # output rows NOT matched for further analysis
       diff = financial_df[(~financial_df.Title.isin(data_df.Title)) & (~financial_df.Released.isin(data_df.Released))]
       diff.to_csv(merge_drop_file, sep=',', index=False)

       # drop empty torrent count cells
       data_df = data_df.replace('', np.nan, regex=True)
       data_df =  data_df.dropna(how='any', subset=['Prod_Budget', 'Genre', 'Torrentz_Count'])

       # drop failed or non-numeric torrent count cells
       data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']] = \
              data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']].convert_objects(convert_numeric=True)
       data_df =  data_df.dropna(how='any', subset=['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count'])

       # sum torrent counts
       data_df['Total_Torrents'] = data_df[['Pirate_Count', 'Torrentz_Count', 'Zoogle_Ver_Count']].sum(axis=1)

       # drop out final data set
       data_df.to_csv(write_file, sep=',', index=False)

def title_standardize(movie_lst, column):
       # strip off series prefix
       movie_lst[column] = movie_lst[column].str.replace(r'^(.+(:))', '')

       # remove punctuation
       movie_lst[column] = movie_lst[column].str.replace('[?:.\',!]*', '')

       # replace sequel numbers
       movie_lst[column] = movie_lst[column].str.replace(r'(?<!\d)3(?!\d)', 'III')
       movie_lst[column] = movie_lst[column].str.replace(r'(?<!\w)and(?!\w)', '&')

       # fix foreign shtuff
       movie_lst[column] = movie_lst[column].str.replace('é', 'e')
       movie_lst[column] = movie_lst[column].str.replace('è', 'e')
       movie_lst[column] = movie_lst[column].str.replace('í', 'i')
       movie_lst[column] = movie_lst[column].str.replace('à', 'a')

       return movie_lst
aggregate()
if __name__ is '__main__':
       aggregate()