# http://www.omdbapi.com/?t=Avatar&y=2009&plot=short&r=json

import requests
import re
import pandas as pd
import json
from pandas.io.json import json_normalize
from bs4 import BeautifulSoup
import codecs

def omdb():
    '''
    :return:
    '''
    front = 'http://www.omdbapi.com/?t='
    end = '&r=json'

    movie_lst = pd.read_csv('movie_dollars.csv')

    api_title_lst = []

    for movie in movie_lst['title']:
        try:
            # clean up movie titles
            jn = re.sub(r'[^a-zA-Z0-9 ]*', '', movie)
            jn = re.sub(' ', '+', jn)
            api_title_lst.append(jn)
        except:
            print(movie)

    # add api ready titles to dataframe
    movie_lst['api_title'] = api_title_lst

    #print(movie_lst.head(10))

    d = {}
    bad_lst = []

    for movie in movie_lst['title']:

        try:
            url = front + movie + end
            html = requests.get(url).text
            d[movie] = html

            with open('movie.json', 'w') as f:
                json.dump(d, f)
        except:
            movie_api = re.sub(r'[^a-zA-Z0-9 ]*', '', movie)
            movie_api = re.sub(' ', '+', movie_api)

            try:
                url = front + movie_api + end
                html = requests.get(url).text
                d[movie] = html

                with open('movie.json', 'w') as f:
                    json.dump(d, f)
            except:
                print(movie)
                bad_lst.append(movie)


        #soup = BeautifulSoup(html, 'lxml')

        #response = urlopen(request)
        #elevations = html.read()
        #data = json.loads(elevations)
        #d = json_normalize(data['results'])


    # function to filter href tags by regex expression
    #def genre_title(href):
    #    return href and re.compile("\.\/chart\/\?id\=").search(href)

    # pull href tags based on filter
    #genre_hrefs = soup.find_all(href=genre_title)

    # list of genres
    #genre_lst = [re.sub('<[^>]+>', '', str(i)) for i in genre_hrefs]

    # list of links to navigate to genres
    #genre_link_lst = [re.sub(r'(<[^>]+="./)', '', str(i)) for i in genre_hrefs]
    #genre_link_lst = [re.sub(r'(>[^>]+>)', '', str(i)) for i in genre_link_lst]

    # pack up genres with links to there page in a tuple
    #genre_tup = [(genre, link) for genre, link in zip(genre_lst, genre_link_lst)]
    #return

omdb()
if __name__ is '__main__':
    omdb()