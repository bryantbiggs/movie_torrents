'''Combine data sources on fuzzy-matched titles to create final data set'''
from multiprocessing import Pool, cpu_count
from functools import partial
import pandas as pd
import numpy as np
from fuzzywuzzy import process

from my_aws import S3

KEY_OMDB_TOR = 'OMDB_Torrents.csv'
KEY_NUM = 'TheNumbers_budgets.csv'
KEY_FINAL = 'Final_Data.csv'
BUCKET = 'movie-torrents'


def fuzzy_match(search_title, match_titles):
    '''
    Perform fuzzy match on provided title (searc_title)

    Args:
        search_title (str): Movie title to match up with
    Returns:
        dict: Dictionary of title searched, title it most closely
              matches and the percent that they match up
    '''
    match_title, match_percent = process.extractOne(
        search_title, match_titles)
    return {'RowTitle': search_title,
            'FuzzTitle': match_title,
            'FuzzPercent': match_percent}


class Aggregate():
    '''
    Combine data sources on fuzzy-matched titles and
    create final data set
    '''

    def __init__(self):
        '''
        Get a S3 connection object and pull down both data sources
        upon instantiation
        '''
        self.s3_obj = S3()
        self.tor_data = self.s3_obj.get_data(KEY_OMDB_TOR, BUCKET)
        self.num_data = self.s3_obj.get_data(KEY_NUM, BUCKET)
        self.final_data = None

    def make_fuzzy_match(self):
        '''
        Fuzzy match `The Numbers` data up to Torrent data up to
        a threshold of (95%) to prep Torrent data to be merged
        with `The Numbers` data

        Args:
            none
        Returns:
            pd.dataframe: Dataframe of torrent data with fuzzy
                          matched title to join on
        '''
        torrent_titles = self.tor_data['Title'].tolist()
        numbers_titles = self.num_data['title'].tolist()

        # Map fuzz search over available cpus
        num_cpus = cpu_count()
        worker_pool = Pool(num_cpus)
        fuzz_results = worker_pool.map(
            partial(fuzzy_match, match_titles=numbers_titles), torrent_titles)

        # fuzz_results = worker_pool.map(fuzzy_match, torrent_titles)
        worker_pool.close()
        worker_pool.join()

        # Put results into a dataframe and rename columns
        fuzz_data = pd.DataFrame(fuzz_results)
        fuzz_data = fuzz_data[['RowTitle', 'FuzzTitle', 'FuzzPercent']]
        fuzz_data.columns = ['Title', 'FuzzTitle', 'FuzzPercent']

        # Append to torrent dataframe and drop duplicate titles
        merge_df = pd.merge(self.tor_data, fuzz_data, how='inner', on='Title')
        merge_df.drop_duplicates(subset='Title', inplace=True)

        # Drop rows where match was below 95%
        merge_df = merge_df[merge_df['FuzzPercent'] >= 95]

        self.tor_data = merge_df
        return merge_df

    def clean_fuzz_data(self):
        '''
        Clean up date prior to merging with `The Numbers` data

        Args:
            none
        Returns:
            pd.dataframe: Dataframe of Torrent data ready to be merged
                          with `The Numbers` data
        '''
        tor = self.tor_data
        # remove where no torrent counts were received from any source
        tor['CheckTup'] = list(zip(tor['Kat_Count'].tolist(),
                                   tor['Pirate_Count'].tolist(),
                                   tor['Extra_Count'].tolist(),
                                   tor['Torrentz_Count'].tolist(),
                                   tor['Torrentz_Ver_Count'].tolist(),
                                   tor['Zoogle_Ver_Count'].tolist()))
        tor = tor[tor['CheckTup'] != ('Fail', 'Fail', 'Fail', 'Fail',
                                      'Fail', 'Fail')].reset_index(drop=True)
        # Drop temp column for checking
        del tor['CheckTup']

        # replace Fail, None, N and NaN with 0 - remove >, and <
        int_cols = ['Metascore', 'Runtime', 'imdbRating', 'imdbVotes',
                    'Kat_Count', 'Pirate_Count', 'Extra_Count',
                    'Torrentz_Count', 'Torrentz_Ver_Count', 'Zoogle_Ver_Count']

        for col in int_cols:
            tor[col] = tor[col].replace(['Fail', 'None', 'N', 'NaN'], '0')
            tor[col] = tor[col].apply(lambda x: str(x).replace(
                '>', '').replace('<', '').replace(',', ''))
            tor[col] = tor[col].replace(np.nan, 0)
            tor[col] = tor[col].fillna(value=0)
            tor[col] = pd.to_numeric(tor[col],
                                     errors='coerce',
                                     downcast='integer')
            tor[col] = tor[col].fillna(value=0)

        # fill in remaining NaN's with blanks
        tor.fillna(value='', inplace=True)
        self.tor_data = tor
        return tor

    def merge_data_sources(self):
        '''
        Merge Torrent data set with `The Numbers` data set

        Args:
            none
        Returns:
            pd.dataframe: Dataframe of resultant data set
        '''

        self.num_data.columns = ['FuzzTitle', 'ReleaseDate',
                                 'ProductionBudget', 'DomesticBudget',
                                 'WorldGross']

        # merge data frames
        data_df = pd.merge(self.tor_data, self.num_data,
                           how='left', on='FuzzTitle')
        data_df = data_df.drop_duplicates(subset='imdbID')

        self.final_data = data_df
        return data_df

    def prepare_final_data(self):
        '''
        Final preparations to final data set prior to
        upload to S3

        Args:
            none
        Returns:
            pd.dataframe: Dataframe of final data set
        '''
        data_df = self.final_data

        # Clean up dates
        data_df['Released'] = pd.to_datetime(data_df['Released'])
        data_df['ReleaseDate'] = pd.to_datetime(data_df['ReleaseDate'])
        data_df.loc[data_df['Released'].isnull(
        ), 'Released'] = data_df['ReleaseDate']

        # Drop columns no longer needed
        del data_df['ReleaseDate']
        del data_df['FuzzTitle']
        del data_df['FuzzPercent']

        # sum torrent counts
        data_df['Total_Torrents'] = data_df[['Kat_Count',
                                             'Pirate_Count',
                                             'Extra_Count',
                                             'Torrentz_Count',
                                             'Torrentz_Ver_Count',
                                             'Zoogle_Ver_Count']].sum(axis=1)

        self.final_data = data_df
        return data_df

    def put_data_s3(self):
        '''
        Upload clean data to S3 storage

        Args:
            none
        Returns:
            none
        '''
        self.s3_obj.put_data(self.final_data, KEY_FINAL, BUCKET)

    def export_data(self, write_file):
        '''
        Export data to csv file with the provided name/location (write_file)

        Args:
            write_file (str): Full file path of where to save csv file
        Returns:
            none
        '''
        self.final_data.to_csv(write_file, sep=',', index=False)

    def aggregate_data(self):
        '''
        Aggregate both data sources, clean, and then uplpoad to S3

        Args:
            none
        Returns:
            none
        '''
        self.make_fuzzy_match()
        self.clean_fuzz_data()
        self.merge_data_sources()
        self.prepare_final_data()
        self.put_data_s3()


if __name__ == '__main__':
    AGG = Aggregate()
    AGG.aggregate_data()
