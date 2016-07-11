from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import os
import time

def download_crawl():
    # remove not found file for each run
    path = '../data/not_found.txt'
    if os.path.isfile(path): os.remove(path)

    # get omdb data frame
    omdb_data = pd.read_csv('../data/omdb_data.csv', encoding='latin-1')

    imdb_num = omdb_data['imdbID']
    title = omdb_data['Title']
    year = omdb_data['Year']
    movie_tup = [(str(imdb_num), str(title), str(year)) for imdb_num, title, year in zip(imdb_num, title, year)]

    for imdb_num, title, year in (movie_tup):
        # meter number of requests sent out
        time.sleep(5)

        print(title + ' - ' + year + ' - ' + imdb_num)

        # kickass torrents cralwer
        #omdb_data.loc[omdb_data.imdbID == imdb_num, 'Kat_Count'] = kat_crawl(imdb_num)

        # pirate bay crawler
        #omdb_data.loc[omdb_data.imdbID == imdb_num, 'Pirate_Count'] = pirate_crawl(imdb_num)

        # extratorrent crawler
        #omdb_data.loc[omdb_data.imdbID == imdb_num, 'Extra_Count'] = extratorrent_crawl(title, year)

        # torrentz (all) crawler
        #omdb_data.loc[omdb_data.imdbID == imdb_num, 'Torrentz_Count'] = torrentz_crawl(title, year)

        # torrentz (verified) crawler
        #omdb_data.loc[omdb_data.imdbID == imdb_num, 'Torrentz_Ver_Count'] = torrentz_ver_crawl(title, year)

        # zoogle crawler
        zoogle_crawl(title, year)
        print('======================')

        # write results to file
        omdb_data.to_csv('../data/torrent_data.csv', mode='w', index=False)

def kat_crawl(imdb_num):

    try:
        # try to get media count
        url = 'https://kat.cr/usearch/category:movies%20imdb:{0}/'.format(imdb_num[2:])
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = soup.div.h2.span
        result = re.sub(r'(<span>  results )([0-9*]\D[0-9*]*)( from )', '', str(result))
        result = re.sub(r'(</span>)', '', result)

    except:
        # otherwise, return 0 and write to not found file
        path = '../data/not_found.txt'
        output = 'Kat == ' + str(imdb_num)
        bad_return(path, output)
        result = 'Fail'

    print('Kat == ', result)
    return result

def pirate_crawl(imdb_num):

    try:
        def pirate():
            # try to get media count
            url = 'https://thepiratebay.org/search/{0}/'.format(imdb_num)
            soup = BeautifulSoup(requests.get(url).text, 'lxml')

            result = soup.body.h2
            result = re.search(r'(?<=approx )([^ found>]+)', str(result))
            result = result.group(0)
            return result

        result = pirate()

    except:
        try:
            # try again, their site hangs a lot
            result = pirate()
        except:
            # otherwise, return 0 and write to not found file
            path = '../data/not_found.txt'
            output = 'Pirate == ' + str(imdb_num)
            bad_return(path, output)
            result = 'Fail'

    print('PirateBay == ', result)
    return result

def extratorrent_crawl(title, year):

    try:
        # try to get media count
        url = 'http://extratorrent.cc/advanced_search/?with={0}+{1}&s_cat=4size_type=b#results/'.format(title, year)
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = str(soup.find_all('b')[12])
        result = result[3:-4]

    except:
        # otherwise, return 0 and write to not found file
        path = '../data/not_found.txt'
        output = 'Extratorrent == ' + str(title) + ' - ' + str(year)
        bad_return(path, output)
        result = 'Fail'

    print('Extratorrent == ', result)
    return result

def torrentz_crawl(title, year):

    try:
        # try to get media count
        url = 'http://www.torrentz.eu/search?f={0}+{1}'.format(title, year)
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = re.search(r'(?<=none">)([^ torrents>]+)', str(soup.h2))
        result = result.group(0)

    except:
        # otherwise, return 0 and write to not found file
        path = '../data/not_found.txt'
        output = 'Torrentz (all) == ' + str(title) + ' - ' + str(year)
        bad_return(path, output)
        result = 'Fail'

    print('Torrentz (all) == ', result)
    return result

def torrentz_ver_crawl(title, year):

    try:
        # try to get media count
        url = 'http://www.torrentz.eu/verified?f={0}+{1}'.format(title, year)
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = re.search(r'(?<=none">)([^ torrents>]+)', str(soup.h2))
        result = result.group(0)

    except:
        # otherwise, return 0 and write to not found file
        path = '../data/not_found.txt'
        output = 'Torrentz (verified) == ' + str(title) + ' - ' + str(year)
        bad_return(path, output)
        result = 'Fail'

    print('Torrentz (verified) == ', result)
    return result

def zoogle_crawl(title, year):

    try:
        # try to get media count
        url = 'https://zooqle.com/search?q={0}+{1}+category%3AMovies'.format(title, year)
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = soup.find_all('span')[5]
        #result = str(result.split(' '))
        #print(str(result))
        #result = re.search(r'(?<=of >)([^)</span> >]+)', str(result))
        #result = result.group(0)
        #print(result)

    except:
        # otherwise, return 0 and write to not found file
        path = '../data/not_found.txt'
        output = 'Zoogle == ' + str(title) + ' - ' + str(year)
        bad_return(path, output)
        result = 'Fail'

    #print('Zoogle == ', result)
    return result

def bad_return(file, search_val):
    '''
    :param file: full path to file to write value not found to
    :param search_val: criteria that was searched and failed/not found
    :return: none - search_val is written to file
    '''
    if os.path.isfile(file):
        with open(file, 'a') as f:
            f.write(search_val + '\n')
    else:
        with open(file, 'w') as f:
            f.write(search_val + '\n')


download_crawl()

if __name__ is '__main__':
    download_crawl()