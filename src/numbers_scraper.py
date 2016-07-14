'''
Class to scrape `The Numbers` website for movie/film budget data
'''
import requests
import pandas as pd
from bs4 import BeautifulSoup

from src.my_aws import S3

KEY_NUM = 'TheNumbers_budgets.csv'
BUCKET = 'movie-torrents'


class NumbersScraper():
    '''
    Scraper to scrape data from `The Numbers`
    website for movie/film budget data
    '''

    def __init__(self):
        '''
        Set the websites base address and get a S3 connection object
        upon instantiation
        '''
        self.base_address = 'http://www.the-numbers.com/movie/budgets/all/'
        self.s3_obj = S3()
        self.table_data = None

    def get_table_data(self):
        '''
        Iterate over entire collection of films provided by `The Numbers`
        website in tabular form - 100 records per page

        Args:
            none
        Returns:
            pd.dataframe: Dataframe containing information pulled from
                          `The Numbers` website tables
        '''
        movies_dict = {}
        record_count = 1

        while True:
            address = '{0}{1}'.format(self.base_address, record_count)
            web_req = requests.get(address)
            soup = BeautifulSoup(web_req.text, 'lxml')
            html_td_tags = [td_tag.text for td_tag in soup.select('td ')]

            if not html_td_tags:
                break

            movie_records = [html_td_tags[i:i + 6]
                             for i in range(0, len(html_td_tags), 6)]
            # Page counts end(start) in --1
            record_mod = record_count - 1

            for movie in movie_records:
                movies_dict[int(movie[0]) + record_mod] = movie[1:]

            # 100 records per page
            record_count += 100

        movie_dataframe = pd.DataFrame.from_dict(movies_dict, orient='index')
        self.table_data = movie_dataframe
        return movie_dataframe

    def clean_data(self):
        '''
        Clean scraped data and convert to appropriate data formats
        Args:
            none
        Returns:
            pd.dataframe: Dataframe of cleaned data pulled from `The Numbers`
                          website tables
        '''
        table = self.table_data
        table.columns = ['release_date',
                         'title',
                         'production_budget',
                         'domestic_gross',
                         'world_gross']

        for col in ['production_budget', 'domestic_gross', 'world_gross']:
            table[col] = table[col].replace(
                r'[\$,]', '', regex=True).astype(int)

        table['release_date'] = pd.to_datetime(
            table['release_date'], format='%m/%d/%Y')

        table['title'] = table['title'].apply(lambda x: x.replace('âs', 's'))
        table['title'] = table['title'].apply(lambda x: x.replace('â', ' '))

        self.table_data = table
        return table

    def put_data_s3(self):
        '''
        Upload clean data to S3 storage

        Args:
            none
        Returns:
            none
        '''
        self.s3_obj.put_data(self.table_data, KEY_NUM, BUCKET)

    def export_data(self, write_file):
        '''
        Export data to csv file with the provided name/location (write_file)

        Args:
            write_file (str): Full file path of where to save csv file
        Returns:
            none
        '''
        self.table_data.to_csv(write_file, sep=',', index=False)

    def run_scraper(self):
        '''
        Run scraper to collect data, clean it, and uplpoad it to S3

        Args:
            none
        Returns:
            none
        '''
        self.get_table_data()
        self.clean_data()
        self.put_data_s3()


if __name__ == '__main__':
    SCRAPER = NumbersScraper()
    SCRAPER.run_scraper()
