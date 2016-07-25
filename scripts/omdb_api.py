import requests
import re
import pandas as pd
import time
import os

def omdb_api_query():
    # files
    write_file = '../data/omdb.csv'
    read_file = '../data/budget.csv'
    err_file = '../data/omdb_error.csv'

    # remove any previously generated files
    if os.path.isfile(err_file): os.remove(err_file)
    if os.path.isfile(write_file): os.remove(write_file)

    # use encoding='latin-1' to steamroll encoding issues
    movie_lst = pd.read_csv(read_file, encoding='latin-1')

    # tupple of movie, title
    title = movie_lst['Title']
    year = [year[:4] for year in movie_lst['Released']]
    movie_tup = [(title, year) for title, year in zip(title, year)]

    count = 0

    for title, year in movie_tup:
        # meter number of requests to omdb api
        time.sleep(2)

        try:
            # omdb api address
            url = 'http://www.omdbapi.com/?t={0}&y={1}&plot=short&r=json'.format(title, year)
            html = requests.get(url).text

            # progress output to console
            count += 1
            #print(str(len(movie_tup)-count), html)

            # if api call returned an error, write movie title and year to file
            if 'Error' in html:
                err(title, year)

            # otherwise, api call returned movie data - process it
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
                movie_dict = {title: info}

                if count == 1:
                    # first time through, add labels
                    df = pd.DataFrame.from_dict(movie_dict, orient='index')
                    df.columns = labels
                    df.to_csv(write_file, mode='w', header=labels, index=False)
                else:
                    df = pd.DataFrame.from_dict(movie_dict, orient='index')
                    df.to_csv(write_file, mode='a', header=False, index=False)

        # if api call could not find title, year then write movie title and year to file
        except:
            err(title, year)


def err(title, year):
    # log movies that failed to return data from omdb api
    with open('../data/omdb_error.csv', 'a') as f:
        f.write('{0}, {1}\n'.format(title, year))

if __name__ is '__main__':
    omdb_api_query()