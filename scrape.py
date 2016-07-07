import requests
import re
import pandas as pd
from bs4 import BeautifulSoup
import codecs
# url = 'http://www.boxofficemojo.com/'
# html = requests.get(url).text
# soup = BeautifulSoup(html, 'lxml')


def movie_dollars():
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
        movie_df.columns = ['Release_Date', 'Title', 'Prod_Budget', 'Dom_Gross', 'World_Gross']
        movie_df = movie_df[['Title', 'Release_Date', 'Prod_Budget', 'Dom_Gross', 'World_Gross']]

        # convert currencies to floating point values
        movie_df[['Prod_Budget']] = movie_df[['Prod_Budget']].replace('[\$,]', '', regex=True).astype(float)
        movie_df[['Dom_Gross']] = movie_df[['Dom_Gross']].replace('[\$,]', '', regex=True).astype(float)
        movie_df[['World_Gross']] = movie_df[['World_Gross']].replace('[\$,]', '', regex=True).astype(float)

        assert movie_df['Prod_Budget'].dtype == 'float64'
        assert movie_df['Dom_Gross'].dtype == 'float64'
        assert movie_df['World_Gross'].dtype == 'float64'

        # convert release date to datetime
        movie_df['Release_Date'] = pd.to_datetime(movie_df['Release_Date'], format='%m/%d/%Y')

        assert movie_df['Release_Date'].dtype == 'datetime64[ns]'

        # order largest budget to least
        movie_df = movie_df.sort_values(by='Prod_Budget', ascending=False).reset_index()
        del movie_df['index']

        # drop out that squeaky clean
        movie_df.to_csv('movie_dollars.csv', sep=',', index=False)

movie_dollars()
