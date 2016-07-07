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

def genres():
    # http://www.boxofficemojo.com/genres/

    url = 'http://www.boxofficemojo.com/genres/'
    html = requests.get(url).text
    soup = BeautifulSoup(html, 'lxml')

    #soup.fin
    #login_form = driver.find_element_by_xpath("/html/body/form[1]")
    #tbl = [link.text for link in soup.select('b')]
    #print(tbl)

    #for tag in soup.find_all(re.compile(r'^./chart/?id=')):
    #    print(tag.name)

    # function to filter href tags by regex expression
    def genre_title(href):
        return href and re.compile("\.\/chart\/\?id\=").search(href)

    # pull href tags based on filter
    genre_hrefs = soup.find_all(href=genre_title)

    # return list of genres
    genre_lst = [re.sub('<[^>]+>', '', str(i)) for i in genre_hrefs]

    # return list of links to navigate
    genre_link_lst = [re.sub(r'(<[^>]+="./)', '', str(i)) for i in genre_hrefs]
    genre_link_lst = [re.sub(r'(>[^>]+>)', '', str(i)) for i in genre_link_lst]

    genre_tup = [(genre, link) for genre, link in zip(genre_lst, genre_link_lst)]
    return genre_tup

genres()

#if __name__ is '__main__':
#    movie_dollars()
#    genres()