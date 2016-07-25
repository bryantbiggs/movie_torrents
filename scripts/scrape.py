import pandas as pd
from bs4 import BeautifulSoup
import codecs

def movie_dollars():
    # files
    # webpage was manually saved to directory
    read_file = "../data/The Numbers - Movie Budgets.html"
    write_file = '../data/budget.csv'

    # use encoding='latin-1' to steamroll encoding issues
    with codecs.open(read_file, "r", encoding="latin-1") as fdata:

        soup = BeautifulSoup(fdata.read(), 'lxml')

        # put all table data into an ordered list
        movie_tbl = [link.text for link in soup.select('td ')]

        # chunk data to groups of 6 (6 colums in original table)
        movie_lst = [movie_tbl[i:i + 6] for i in range(0, len(movie_tbl), 6)]

        # move data into dictionary to send to pandas dataframe
        movie_dict = {movie[0]: movie[1:] for movie in movie_lst}

        # give birth to a panda
        movie_df = pd.DataFrame.from_dict(movie_dict, orient='index')

        # add column names and re-order
        movie_df.columns = ['Released', 'Title', 'Prod_Budget', 'Dom_Gross', 'World_Gross']
        movie_df = movie_df[['Title', 'Released', 'Prod_Budget', 'Dom_Gross', 'World_Gross']]

        # convert currencies to floating point values
        movie_df[['Prod_Budget']] = movie_df[['Prod_Budget']].replace('[\$,]', '', regex=True).astype(float)
        movie_df[['Dom_Gross']] = movie_df[['Dom_Gross']].replace('[\$,]', '', regex=True).astype(float)
        movie_df[['World_Gross']] = movie_df[['World_Gross']].replace('[\$,]', '', regex=True).astype(float)

        assert movie_df['Prod_Budget'].dtype == 'float64'
        assert movie_df['Dom_Gross'].dtype == 'float64'
        assert movie_df['World_Gross'].dtype == 'float64'

        # convert release date to datetime
        movie_df['Released'] = pd.to_datetime(movie_df['Released'], format='%m/%d/%Y')

        assert movie_df['Released'].dtype == 'datetime64[ns]'

        # re-order dataframe on production budget column largest to smallest, delete old index
        movie_df = movie_df.sort_values(by='Prod_Budget', ascending=False).reset_index()
        del movie_df['index']

        # drop out clean csv
        movie_df.to_csv(write_file, sep=',', index=False)

if __name__ is '__main__':
    movie_dollars()