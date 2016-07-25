from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import os
import time

def download_crawl():
    # remove any previous files
    write_file = '../data/torrents.csv'
    read_file = '../data/omdb.csv'
    err_file = '../data/tor_error.csv'

    if os.path.isfile(write_file): os.remove(write_file)
    if os.path.isfile(read_file): os.remove(read_file)
    if os.path.isfile(err_file): os.remove(err_file)

    # get omdb data frame
    omdb_data = pd.read_csv(read_file, encoding='latin-1', error_bad_lines=False)

    # creat tuple for torrent search
    movie_tup = [(str(imdb_num), str(title), str(year)) for imdb_num, title, year in
                 zip(omdb_data['imdbID'], omdb_data['Title'], omdb_data['Year'])]

    # loop through movies and grab torrent counts
    for imdb_num, title, year in (movie_tup):
        # meter number of requests sent out
        time.sleep(2)

        # kickass torrents cralwer
        omdb_data.loc[omdb_data.imdbID == imdb_num, 'Kat_Count'] = kat_crawl(imdb_num)

        # pirate bay crawler
        omdb_data.loc[omdb_data.imdbID == imdb_num, 'Pirate_Count'] = pirate_crawl(imdb_num)

        # torrentz (all) crawler
        omdb_data.loc[omdb_data.imdbID == imdb_num, 'Torrentz_Count'] = torrentz_crawl(title, year)

        # torrentz (verified) crawler
        omdb_data.loc[omdb_data.imdbID == imdb_num, 'Torrentz_Ver_Count'] = torrentz_ver_crawl(title, year)

        # write results to file
        omdb_data.to_csv(write_file, mode='w', index=False)

def kat_crawl(imdb_num, title, year):

    try:
        # try to get media count
        url = 'https://kat.cr/usearch/category:movies%20imdb:{0}/'.format(imdb_num[2:])
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = re.sub(r'(<span>  results )([0-9*]\D[0-9*]*)( from )', '', str(soup.div.h2.span))
        result = re.sub(r'(</span>)', '', result)

    except:
        # otherwise, return Fail and write to error file
        err('Kickass', imdb_num, title, year)
        result = 'Fail'

    return result

def pirate_crawl(imdb_num, title, year):

    try:
        def pirate():
            # try to get media count
            url = 'https://thepiratebay.org/search/{0}/'.format(imdb_num)
            soup = BeautifulSoup(requests.get(url).text, 'lxml')

            result = re.search(r'(?<=approx )([^ found>]+)', str(soup.body.h2))
            result = result.group(0)
            return result

        result = pirate()

    except:
        try:
            # try again, their site hangs a lot
            result = pirate()
        except:
            # otherwise, return Fail and write to error file
            err('Piratebay', imdb_num, title, year)
            result = 'Fail'

    return result

def torrentz_crawl(imdb_num, title, year):

    try:
        # try to get media count
        url = 'http://www.torrentz.eu/search?f={0}+{1}'.format(title, year)
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = re.search(r'(?<=none">)([^ torrents>]+)', str(soup.h2))
        result = result.group(0)

    except:
        # otherwise, return Fail and write to error file
        err('Torrentz', imdb_num, title, year)
        result = 'Fail'

    return result

def torrentz_ver_crawl(imdb_num, title, year):

    try:
        # try to get media count
        url = 'http://www.torrentz.eu/verified?f={0}+{1}'.format(title, year)
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = re.search(r'(?<=none">)([^ torrents>]+)', str(soup.h2))
        result = result.group(0)

    except:
        # otherwise, return Fail and write to error file
        err('Torrentz verified', imdb_num, title, year)
        result = 'Fail'

    return result

def err(site, imdb, title, year):
    # log movies that failed to return data from omdb api
    err_file = '../data/tor_error.csv'

    with open(err_file, 'a') as f:
        f.write('{0},{1},{2},{3}\n'.format(site, imdb, title, year))

if __name__ is '__main__':
    download_crawl()