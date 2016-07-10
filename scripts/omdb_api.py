import requests
import re
import pandas as pd
import json
from pprint import pprint

def omdb_api_query():
    '''
    :return: using csv list of movies from The Numbers, gather movie data from omdb api
    '''
    front = 'http://www.omdbapi.com/?t='
    end = '&r=json'

    movie_lst = pd.read_csv('../data/movie_dollars.csv')
    omdb_dict = {}
    bad_lst = []

    for movie in movie_lst['title']:

        try:
            # try api call with title as is (spaces, etc.)
            url = front + movie + end
            html = requests.get(url).text
            omdb_dict[movie] = html

            # with each call, write to file
            with open('../data/movie.json', 'w') as f:
                json.dump(omdb_dict, f)
        except:
            # remove all special characters and replace whitespace with '+'
            movie_api = re.sub(r'[^a-zA-Z0-9 ]*', '', movie)
            movie_api = re.sub(' ', '+', movie_api)

            try:
                # try api call with modified title
                url = front + movie_api + end
                html = requests.get(url).text
                omdb_dict[movie] = html

                # with each call, write to file
                with open('../data/movie.json', 'w') as f:
                    json.dump(omdb_dict, f)
            except:
                # show me any movies that didn't make the api call
                bad_lst.append(movie)
                with open('../data/bad_omdb.txt', 'w') as f:
                    f.write(bad_lst)

def strip_html():
    with open('../data/movie.json') as json_data:
        movie_json = json.load(json_data)
        #pprint(movie_json)
        for movie, x in movie_json:
            #if re.findall('<!DOCTYPE html>', movie) != []:
            print(x)

    #pprint(movie_json)

def omdb_json_clean():

    movie_lst = pd.read_csv('../data/movie_dollars.csv')

    new_dict = {}
    bad_lst = []
    d = {}
    with open('../data/movie.json') as json_data:

        movie_json = json.load(json_data)

        for mov in movie_lst['title']:

            try:
                movie = movie_json[mov]
                if re.findall('<!DOCTYPE html>', movie) == []:
                    movie = re.sub(r'{', '', movie)
                    movie = re.sub(r'}', '', movie)
                    movie_meta = movie.split('","')

                    for i in range(len(movie_meta)):
                        movie_meta[i] = re.sub('\"', '', movie_meta[i])

                    for meta in movie_meta:
                        temp = meta.split(':')
                        new_dict[temp[0]] = temp[1]

                    d[mov] = new_dict
                else:
                    bad_lst.append(mov)
            except:
                bad_lst.append(mov)

    #print(len(bad_lst))
    pprint(d)
    #dict = {}
    #for i in new_dict:

    #df = pd.DataFrame.from_dict(d, orient='index')
    #print(df.head(20))

    #with open('../data/movie.json', 'r') as re_file:
    #    line = str(re_file.readlines())
    #    line = line.replace('\\\\', '')
    #    line = ''.join(line)

        #with open('../data/clean.txt', 'w') as wr_file:
        #    wr_file.write(line)

#strip_html()
omdb_json_clean()

if __name__ is '__main__':
    omdb_api_query()