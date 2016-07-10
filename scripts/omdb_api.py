import requests
import re
import pandas as pd
import json
from pprint import pprint
import time

def omdb_api_query():
    '''
    :return: using csv list of movies from The Numbers, gather movie data from omdb api
    '''
    movie_lst = pd.read_csv('../data/movie_dollars.csv')

    # tupple of movie, title
    title = movie_lst['title']
    year = [year[:4] for year in movie_lst['release_date']]
    movie_tup = [(title, year) for title, year in zip(title, year)]

    movie_dict = {}
    count = 0

    for title, year in movie_tup:
        # meter number of requests to omdb api
        time.sleep(5)

        url = 'http://www.omdbapi.com/?t={0}&y={1}&plot=short&r=json'.format(title, year)
        html = requests.get(url).text

        count += 1
        print(str(len(movie_tup)-count), html)

        if 'Error' in html:
            with open('../data/not_found.txt', 'a') as f:
                f.write(title + '\n')
        else:
            # remove curly braces and split to list
            movie_meta = re.sub(r'{', '', html)
            movie_meta = re.sub(r'}', '', movie_meta)
            movie_meta = movie_meta.split('","')

            # remove all backslashes
            for i in range(len(movie_meta)):
                movie_meta[i] = re.sub('\"', '', movie_meta[i])

            labels, info = [], []

            # extract labels and data
            for meta in movie_meta:
                temp = meta.split(':')
                labels.append(temp[0])
                info.append(temp[1])

            # add to dictionary
            movie_dict[title] = info

            if count == 1:
                # first time through, add labels
                df = pd.DataFrame.from_dict(movie_dict, orient='index')
                df.columns = labels
            else:
                df = pd.DataFrame.from_dict(movie_dict, orient='index')

            # write to csv
            df.to_csv('../data/omdb_data.csv', mode='w', header=labels, index=False)

        #except:
            # remove all special characters and replace whitespace with '+'
            #movie_api = re.sub(r'[^a-zA-Z0-9 ]*', '', movie)
            #movie_api = re.sub(' ', '+', movie_api)

            #try:
                # try api call with modified title
            #    url = front + movie_api + end
            #    html = requests.get(url).text
            #    omdb_dict[movie] = html

                # with each call, write to file
                #with open('../data/movie.json', 'w') as f:
                #    json.dump(omdb_dict, f)
            #except:

            # show me any movies that didn't make the api call
            #bad_lst.append(movie)

            #with open('../data/bad_omdb.txt', 'a') as f:
            #    f.write(str(movie))

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

omdb_api_query()

if __name__ is '__main__':
    omdb_api_query()