import requests
import re
import pandas as pd
import time
import string

def omdb_api_query():
    '''
    :return: using csv list of movies from The Numbers, gather movie data from omdb api
    '''
    movie_lst = pd.read_csv('../data/movie_dollars.csv', encoding='latin-1')

    # tupple of movie, title
    title = movie_lst['title']
    year = [year[:4] for year in movie_lst['release_date']]
    movie_tup = [(title, year) for title, year in zip(title, year)]

    count = 0

    for title, year in movie_tup:
        # meter number of requests to omdb api
        time.sleep(5)

        try:
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
                movie_dict = {}
                movie_dict[title] = info

                if count == 1:
                    # first time through, add labels
                    df = pd.DataFrame.from_dict(movie_dict, orient='index')
                    df.columns = labels

                    # write to csv
                    df.to_csv('../data/omdb_data.csv', mode='w', header=labels, index=False)
                else:
                    df = pd.DataFrame.from_dict(movie_dict, orient='index')
                    # write to csv
                    df.to_csv('../data/omdb_data.csv', mode='a', header=False, index=False)

        except:
            with open('../data/not_found.txt', 'a') as f:
                f.write(title + '\n')

def modified_query():

    # load financial omdb data into dataframe
    movie_lst = pd.read_csv('../data/movie_dollars.csv', encoding='latin-1')
    movie_lst['year'] = pd.to_datetime(movie_lst['release_date'])

    # remove punctuation
    movie_lst['strip_title'] = movie_lst['title'].apply(lambda x:''.join([i for i in x if i not in string.punctuation]))

    with open('../data/not_found.txt', encoding='latin-1') as f:
        #titles = [line.strip() for line.apply(lambda x:''.join([i for i in x if i not in string.punctuation])) in f]
        titles = [''.join(x) for x in title if x not in string.punctuation for title in f]

    #print(movie_lst.loc[movie_lst['title'] == titles])
    print(titles)

modified_query()

if __name__ is '__main__':
    omdb_api_query()