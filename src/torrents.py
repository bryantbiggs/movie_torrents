'''
Poll torrent sites checking for number of torrent copies for a select film
'''
import time
import re
import requests
import pandas as pd
from bs4 import BeautifulSoup

from my_aws import S3

KEY_OMDB = 'OMDB_API.csv'
KEY_OMDB_TOR = 'OMDB_Torrents.csv'
BUCKET = 'movie-torrents'


class TorrentCount():
    '''
    Poll popular torrent sites Kickass Torrents, Pirate Bay, Torrentz,
    and Torrentz Verified collecting the number of torrent copies
    available for a given film
    '''

    def __init__(self):
        '''
        Connect to AWS S3 bucket, pull down pandas dataframe of data,
        and create list of movie tuples upon instantiation
        '''
        self.s3_connect = S3()
        self.omdb_data = self.get_data_s3()
        self.movies_tup = self.make_movie_tuple()

    def get_data_s3(self):
        '''
        Get pandas dataframe from S3

        Args:
            none

        Returns:
            pd.dataframe: Dataframe containing moive data sans torrent counts
        '''
        omdb_data = self.s3_connect.get_data(KEY_OMDB, BUCKET)

        omdb_data['Year'] = pd.DatetimeIndex(omdb_data['Released']).year
        omdb_data = omdb_data.dropna(subset=['Year'])
        omdb_data['Year'] = omdb_data['Year'].apply(
            lambda year: str(int(year)))

        return omdb_data

    def make_movie_tuple(self):
        '''
        Create a list of tuples containing IMDB id, title, and release year
        to use when polling through torrent sites

        Args:
            none

        Returns:
            list[tuples]: Movie tuples in the form (imdbID, year, title)
        '''
        movies_tup = [(imdb_id, title, year) for imdb_id, title, year in
                      zip(self.omdb_data['imdbID'],
                          self.omdb_data['Title'],
                          self.omdb_data['Year'])]

        return movies_tup

    @classmethod
    def kat_crawl(cls, imdb_id):
        '''
        Class method to crawl Kickass Torrents website to get
        number of torrent copies available for given movie IMDB id

        Args:
            imdb_id (str): Unique id for film from IMDB website

        Returns:
            str: Number of torrent copies found on Kickass Torrents
                 website for given IMDB id
        '''
        address = 'https://kat.cr/usearch/category:movies%20imdb:{0}/'.format(
            imdb_id[2:])
        web_req = requests.get(address)
        if web_req.status_code != 200:
            return 'Fail'

        soup = BeautifulSoup(web_req.text, 'lxml')
        html_title = soup.div.h2.span

        if not html_title:
            return 'Fail'

        title_strip = re.sub(
            r'(<span>  results )([0-9*]\D[0-9*]*)( from )', '',
            str(html_title))
        torrent_count = re.sub(r'(</span>)', '', title_strip)

        return torrent_count

    @classmethod
    def pirate_crawl(cls, imdb_id):
        '''
        Class method to crawl Pirate Bay website to get
        number of torrent copies available for given movie IMDB id

        Args:
            imdb_id (str): Unique id for film from IMDB website

        Returns:
            str: Number of torrent copies found on Pirate Bay site
                 for given IMDB id
        '''
        address = 'https://thepiratebay.org/search/{0}/'.format(imdb_id)
        web_req = requests.get(address)
        if web_req.status_code != 200:
            return 'Fail'

        soup = BeautifulSoup(web_req.text, 'lxml')
        html_title = soup.body.h2

        if not html_title:
            return 'Fail'

        title_strip = re.search(r'(?<=approx )([^ found>]+)', str(html_title))
        torrent_count = title_strip.group(0)

        return torrent_count

    @classmethod
    def torrentz_crawl(cls, title, year):
        '''
        Class method to crawl Torrentz website to get number of
        torrent copies available for given movie title and release year

        Args:
            title (str): Title of movie to search on Torrentz site
            year (str): Year of movie to search on Torrentz site

        Returns:
            str: Number of torrent copies found on Torrentz verified
                 site for given title and year
        '''
        address = 'http://www.torrentz.eu/search?f={0}+{1}'.format(title, year)
        web_req = requests.get(address)
        if web_req.status_code != 200:
            return 'Fail'

        soup = BeautifulSoup(web_req.text, 'lxml')
        html_title = soup.h2

        if not html_title:
            return 'Fail'

        title_strip = re.search(
            r'(?<=none">)([^ torrents>]+)', str(html_title))
        torrent_count = title_strip.group(0)

        return torrent_count

    @classmethod
    def torrentz_ver_crawl(cls, title, year):
        '''
        Class method to crawl Torrentz Verified website to get number of
        torrent copies available for given movie title and release year

        Args:
            title (str): Title of movie to search on Torrentz verified site
            year (str): Year of movie to search on Torrentz site

        Returns:
            str: Number of torrent copies found on Torrentz verified
                 site for given title and year
        '''
        address = 'http://www.torrentz.eu/verified?f={0}+{1}'.format(
            title, year)
        web_req = requests.get(address)
        if web_req.status_code != 200:
            return 'Fail'

        soup = BeautifulSoup(web_req.text, 'lxml')
        html_title = soup.h2

        if not html_title:
            return 'Fail'

        title_strip = re.search(
            r'(?<=none">)([^ torrents>]+)', str(html_title))
        torrent_count = title_strip.group(0)

        return torrent_count

    def poll_torrent_counts(self):
        '''
        Loop through movies tuple and poll torrent sites for number of
        torrent copies found before uploading final results to AWS S3

        Args:
            none

        Returns:
            none
        '''
        for imdb_id, title, year in self.movies_tup:
            time.sleep(1)

            omdb_data = self.omdb_data
            omdb_data.loc[omdb_data['imdbID'] == imdb_id,
                          'Kat_Count'] = self.kat_crawl(imdb_id)
            omdb_data.loc[omdb_data['imdbID'] == imdb_id,
                          'Pirate_Count'] = self.pirate_crawl(imdb_id)
            omdb_data.loc[omdb_data['imdbID'] == imdb_id,
                          'Torrentz_Count'] = self.torrentz_crawl(title, year)
            omdb_data.loc[omdb_data['imdbID'] == imdb_id,
                          'Torrentz_Ver_Count'] = \
                self.torrentz_ver_crawl(title, year)

        self.s3_connect.put_data(omdb_data, KEY_OMDB_TOR, BUCKET)


if __name__ == '__main__':
    TOR_COUNT = TorrentCount()
