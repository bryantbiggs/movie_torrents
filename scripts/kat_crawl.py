from bs4 import BeautifulSoup
import codecs
import requests
import re
import pandas as pd
import json

#movie_lst = pd.read_csv('../data/movie_dollars.csv')

#def kat_crawl():

    #url = 'https://kat.cr/usearch/category:movies%20imdb:2488496/'
    #driver = webdriver.Chrome(chromedriver)
    #driver.get(url)

    # enter search into search box
    #search_box = driver.find_element_by_id('contentSearch')
    #search_box.send_keys('Avatar')

    #search_box.send_keys(Keys.RETURN)


    # use encoding='latin-1' to steamroll encoding issues
    #with codecs.open(url, "r", encoding="latin-1") as fdata:
        #soup = BeautifulSoup(fdata.read(), 'lxml')

        # get all data in table in ordered list
        #movie_table = [link.text for link in soup.select('td ')]

        # chunk data to groups of 6 (6 colums in original table)
        #movie_lst = [movie_table[i:i + 6] for i in range(0, len(movie_table), 6)]

        # move data into dict to send to pandas
        #movie_dict = {}
        #for movie in movie_lst:
        #    movie_dict[movie[0]] = movie[1:]

        # birth a panda
        #movie_df = pd.DataFrame.from_dict(movie_dict, orient='index')

        # add column names and re-order
        #movie_df.columns = ['release_date', 'title', 'prod_budget', 'dom_gross', 'world_gross']
        #movie_df = movie_df[['title', 'release_date', 'prod_budget', 'dom_gross', 'world_gross']]

        # convert currencies to floating point values
        #movie_df[['prod_budget']] = movie_df[['prod_budget']].replace('[\$,]', '', regex=True).astype(float)
        #movie_df[['dom_gross']] = movie_df[['dom_gross']].replace('[\$,]', '', regex=True).astype(float)
        #movie_df[['world_gross']] = movie_df[['world_gross']].replace('[\$,]', '', regex=True).astype(float)

        #assert movie_df['prod_budget'].dtype == 'float64'
        #assert movie_df['dom_gross'].dtype == 'float64'
        #assert movie_df['world_gross'].dtype == 'float64'

        # convert release date to datetime
        #movie_df['release_date'] = pd.to_datetime(movie_df['release_date'], format='%m/%d/%Y')

        #assert movie_df['release_date'].dtype == 'datetime64[ns]'

        # order largest budget to least
        #movie_df = movie_df.sort_values(by='prod_budget', ascending=False).reset_index()
        #del movie_df['index']

        # drop out that squeaky clean
        #movie_df.to_csv('movie_dollars.csv', sep=',', index=False)

def kat_crawl():
    front = 'https://kat.cr/usearch/category:movies%20imdb:'
    end = '/'

    movie_lst = pd.read_csv('../data/movie_dollars.csv')
    d = {}
    bad_lst = []

    #for imdb_num in movie_lst['imdb']:
    imdb_num = '2488496'


        # try api call with title as is (spaces, etc.)
    url = front + imdb_num + end
    soup = BeautifulSoup(requests.get(url).text, 'lxml')

    result = soup.div.h2.span

    result = re.sub(r'(<span>  results )([0-9*]\D[0-9*]*)( from )', '', str(result))
    result = re.sub(r'(</span>)', '', result)
    print(result)

        #d[imdb_num] = html

        # with each call, write to file
        #with open('kat.json', 'w') as f:
        #    json.dump(d, f)

            # remove all special characters and replace whitespace with '+'
            #movie_api = re.sub(r'[^a-zA-Z0-9 ]*', '', movie)
            #movie_api = re.sub(' ', '+', movie_api)

            #try:
            #    # try api call with modified title
            #    url = front + movie_api + end
            #    html = requests.get(url).text
            #    d[imdb_num] = html

                # with each call, write to file
            #    with open('movie.json', 'w') as f:
            #        json.dump(d, f)
            #except:
            #    # show me any movies that didn't make the api call
            #    print(movie)
            #    bad_lst.append(movie)
kat_crawl()
if __name__ is '__main__':
    kat_crawl()