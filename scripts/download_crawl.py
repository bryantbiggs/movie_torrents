from bs4 import BeautifulSoup
import requests
import re
import pandas as pd
import os

def download_crawl():
    # get imdb id
    omdb_data = pd.read_csv('../data/omdb_data.csv', encoding='latin-1')

    for imdb_num in omdb_data['imdbID']:
        kat_crawl(imdb_num)
        omdb_data.loc[omdb_data.imdbID == imdb_num, 'Kat_Count'] = kat_crawl(imdb_num)

        omdb_data.to_csv('../data/text_data.csv', mode='w', index=False)

def kat_crawl(imdb_num):

    # remove not found file for each run
    kat_path = '../data/kat_notfound.txt'
    if os.path.isfile(kat_path): os.remove(kat_path)

    try:
        # try to get media count
        url = 'https://kat.cr/usearch/category:movies%20imdb:{0}/'.format(imdb_num[2:])
        soup = BeautifulSoup(requests.get(url).text, 'lxml')

        result = soup.div.h2.span
        result = re.sub(r'(<span>  results )([0-9*]\D[0-9*]*)( from )', '', str(result))
        result = re.sub(r'(</span>)', '', result)

    except:

        # otherwise, return 0 and write to not found file
        if os.path.isfile(kat_path):
            with open(kat_path, 'a') as f:
                f.write(imdb_num + '\n')
            result = 0
        else:
            with open(kat_path, 'w') as f:
                f.write(imdb_num + '\n')
            result = 0

    return result

download_crawl()

if __name__ is '__main__':
    download_crawl()