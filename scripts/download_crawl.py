from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import os

def kat_crawl():

    # remove not found file
    os.remove('../data/kat_notfound.txt')

    # get imdb id
    omdb_data = pd.read_csv('../data/omdb_data.csv', encoding='latin-1')

    for imdb_num in omdb_data['imdbID']:

        try:
            # try to get media count
            url = 'https://kat.cr/usearch/category:movies%20imdb:{0}/'.format(imdb_num[2:])
            soup = BeautifulSoup(requests.get(url).text, 'lxml')

            result = soup.div.h2.span

            result = re.sub(r'(<span>  results )([0-9*]\D[0-9*]*)( from )', '', str(result))
            result = re.sub(r'(</span>)', '', result)

        except:
            # otherwise, return 0 and write to not found file
            with open('../data/kat_notfound.txt', 'a') as f:
                f.write(imdb_num + '\n')
            result = 0

    return result

kat_crawl()

if __name__ is '__main__':
    kat_crawl()