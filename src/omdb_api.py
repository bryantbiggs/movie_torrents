'''
Pull movie metadata from `The Open Movie Database` API
for films that were scraped from `The Numbers` website
'''
import time
import json
import requests
import pandas as pd

from src.my_aws import S3

KEY_OMDB = 'OMDB_API.csv'
KEY_NUM = 'TheNumbers_budgets.csv'
BUCKET = 'movie-torrents'


class OMDBApi():
    '''
    Pull movie metadata from `The Open Movie Database` API
    for films that were scraped from `The Numbers` website
    '''

    def __init__(self):
        '''
        Get an S3 connection object, set website base address,
        and get the numbers data to create the query parameters
        '''
        self.s3_connect = S3()
        self.base_address = 'http://www.omdbapi.com'
        self.the_numbers_data = self.get_thenumbers_data()
        self.query_params = self.get_query_params()
        self.api_data = None

    def get_thenumbers_data(self):
        '''
        Pull down movie data previously scraped from `The Numbers`
        website from S3 storage

        Args:
            none
        Returns:
            pd.dataframe: Dataframe of data from `The Numbers` website
        '''
        the_numbers_data = self.s3_connect.get_data(KEY_NUM, BUCKET)

        return the_numbers_data

    def get_query_params(self):
        '''
        Create a list of query tuple parameters to be used for
        querying the OMDB API

        Args:
            none
        Returns:
            List(tuples): List of query value tuples for ODMB API
        '''
        numbers_title = self.the_numbers_data['title']
        numbers_year = [year[:4]
                        for year in self.the_numbers_data['release_date']]
        query_params = [(title, year)
                        for title, year in zip(numbers_title, numbers_year)]

        return query_params

    def get_omdb_metadata(self, omdb_api_key):
        '''
        Poll OMDB API to get metadata for the movie title and year provided
        from the scraped `The Numbers` data

        Args:
            omdb_api_key (str): API key required to use OMDB API
        Returns:
            pd.dataframe: Pandas dataframe of data with OMDB metadata appended
        '''

        polled_records = []

        for title, year in self.query_params:
            time.sleep(0.5)

            payload = {'t': title, 'y': year, 'apikey': omdb_api_key}
            html = requests.get(self.base_address, params=payload)

            resp = json.loads(html.text)
            if html.status_code != 200 or 'Error' in resp.keys():
                continue

            html_text = html.text
            html_json = json.loads(html_text)
            polled_records.append(html_json)

        api_data = pd.DataFrame.from_dict(polled_records, orient='columns')

        self.api_data = api_data
        return api_data

    def clean_data(self):
        '''
        Clean data pulled from OMDB API

        Args:
            none
        Returns:
            pd.dataframe: Pandas dataframe of data with clean OMDB metadata
        '''
        api_data = self.api_data
        api_data = api_data[['Actors', 'Awards', 'BoxOffice', 'Country',
                             'DVD', 'Director', 'Genre', 'Language',
                             'Metascore', 'Production', 'Rated', 'Released',
                             'Runtime', 'Title', 'Type', 'Writer', 'imdbID',
                             'imdbRating', 'imdbVotes']]

        for col in ['BoxOffice', 'imdbVotes']:
            api_data[col].replace(to_replace='N/A', value='0', inplace=True)
            api_data[col] = api_data[col].replace(
                r'[\$,]', '', regex=True)
            api_data[col] = pd.to_numeric(
                api_data[col], errors='coerice', downcast='integer')

        api_data['Runtime'].replace(to_replace='N/A', value='0', inplace=True)
        api_data['Runtime'] = api_data['Runtime'].replace(
            r'[ min]', '', regex=True)
        api_data['Runtime'] = pd.to_numeric(
            api_data['Runtime'], errors='coerice', downcast='integer')

        for col in ['DVD', 'Released']:
            api_data[col] = pd.to_datetime(
                api_data[col], errors='coerce', format='%d %b %Y')

        for col in ['Metascore', 'imdbRating']:
            api_data[col].replace(to_replace='N/A', value='0', inplace=True)
            api_data[col] = pd.to_numeric(
                api_data[col], errors='coerice', downcast='float')

        api_data = api_data[api_data['Type'] == 'movie']

        self.api_data = api_data
        return api_data

    def upload_data_s3(self):
        '''
        Upload clean data (pd.dataframe) to S3 storage

        Args:
            none
        Returns:
            none
        '''
        self.s3_connect.put_data(self.api_data, KEY_OMDB, BUCKET)


if __name__ == '__main__':
    API_POLL = OMDBApi()
