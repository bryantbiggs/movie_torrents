import requests
import re
import pandas as pd
import json

def omdb():
    '''
    :return:
    '''
    front = 'http://www.omdbapi.com/?t='
    end = '&r=json'

    movie_lst = pd.read_csv('movie_dollars.csv')
    d = {}
    bad_lst = []

    for movie in movie_lst['title']:

        try:
            # try api call with title as is (spaces, etc.)
            url = front + movie + end
            html = requests.get(url).text
            d[movie] = html

            # with each call, write to file
            with open('movie.json', 'w') as f:
                json.dump(d, f)
        except:
            # remove all special characters and replace whitespace with '+'
            movie_api = re.sub(r'[^a-zA-Z0-9 ]*', '', movie)
            movie_api = re.sub(' ', '+', movie_api)

            try:
                # try api call with modified title
                url = front + movie_api + end
                html = requests.get(url).text
                d[movie] = html

                # with each call, write to file
                with open('movie.json', 'w') as f:
                    json.dump(d, f)
            except:
                # show me any movies that didn't make the api call
                print(movie)
                bad_lst.append(movie)

if __name__ is '__main__':
    omdb()