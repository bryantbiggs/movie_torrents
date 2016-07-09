import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
import codecs

def movie_dollars():
    '''
    Takes webpage manually saved as html and outputs a clean csv of the movie financial data
    '''

    url = "The Numbers - Movie Budgets.html"

    # use encoding='latin-1' to steamroll encoding issues
    with codecs.open(url, "r", encoding="latin-1") as fdata:
        soup = BeautifulSoup(fdata.read(), 'lxml')

        # get all data in table in ordered list
        movie_table = [link.text for link in soup.select('td ')]

        # chunk data to groups of 6 (6 colums in original table)
        movie_lst = [movie_table[i:i + 6] for i in range(0, len(movie_table), 6)]

        # move data into dict to send to pandas
        movie_dict = {}
        for movie in movie_lst:
            movie_dict[movie[0]] = movie[1:]

        # birth a panda
        movie_df = pd.DataFrame.from_dict(movie_dict, orient='index')

        # add column names and re-order
        movie_df.columns = ['release_date', 'title', 'prod_budget', 'dom_gross', 'world_gross']
        movie_df = movie_df[['title', 'release_date', 'prod_budget', 'dom_gross', 'world_gross']]

        # convert currencies to floating point values
        movie_df[['prod_budget']] = movie_df[['prod_budget']].replace('[\$,]', '', regex=True).astype(float)
        movie_df[['dom_gross']] = movie_df[['dom_gross']].replace('[\$,]', '', regex=True).astype(float)
        movie_df[['world_gross']] = movie_df[['world_gross']].replace('[\$,]', '', regex=True).astype(float)

        assert movie_df['prod_budget'].dtype == 'float64'
        assert movie_df['dom_gross'].dtype == 'float64'
        assert movie_df['world_gross'].dtype == 'float64'

        # convert release date to datetime
        movie_df['release_date'] = pd.to_datetime(movie_df['release_date'], format='%m/%d/%Y')

        assert movie_df['release_date'].dtype == 'datetime64[ns]'

        # order largest budget to least
        movie_df = movie_df.sort_values(by='prod_budget', ascending=False).reset_index()
        del movie_df['index']

        # drop out that squeaky clean
        movie_df.to_csv('movie_dollars.csv', sep=',', index=False)

if __name__ is '__main__':
    movie_dollars()