import requests
from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import sqlite3
from sqlite3 import Error

DB_FILE_PATH = 'sampleSQLite.db'

def fetch_web_content():
    headers = {"Accept-Language": "en-US, en;q=0.5"}
    url = "https://en.wikipedia.org/wiki/September_10"
    results = requests.get(url, headers=headers)
    web_content = BeautifulSoup(results.text, "html.parser")
    return web_content

def web_scrape_and_insert_to_table(item,web_content):
    h2 = web_content.find(lambda elm: elm.name == "h2" and item in elm.text)
    uls = []
    for nextSibling in h2.findNextSiblings():
        if nextSibling.name == 'h2':
            break
        if nextSibling.name == 'ul':
            uls.append(nextSibling)

    lis = []
    for ul in uls:
        for li in ul.findAll('li'):
            if li.find('ul'):
                break
            lis.append(li)

    year = []
    details = []

    for li in lis:
        txt = li.text
        year.append(txt.split('–')[0])
        details.append(txt.split('–')[1])
 
    data = pd.DataFrame({
        'year': year,
        item+'_info': details,
         })
    insert_values_to_table(item,data)

def connect_to_db(db_file):
    sqlite3_conn = None

    try:
        sqlite3_conn = sqlite3.connect(db_file)
        return sqlite3_conn
    except Error as err:
        print(err)

        if sqlite3_conn is not None:
            sqlite3_conn.close()


def insert_values_to_table(table_name, dataframe):

    conn = connect_to_db(DB_FILE_PATH)

    if conn is not None:
        c = conn.cursor()

        c.execute('CREATE TABLE IF NOT EXISTS ' + table_name +
                  '(year        VARCHAR,'
                  'details     VARCHAR)'
                  )

        dataframe.columns = get_column_names_from_db_table(c, table_name)
        dataframe.to_sql(name=table_name, con=conn, if_exists='append', index=False)

        conn.close()
        print('SQL insert process finished')
    else:
        print('Connection to database failed')


def get_column_names_from_db_table(sql_cursor, table_name):

    table_column_names = 'PRAGMA table_info(' + table_name + ');'
    sql_cursor.execute(table_column_names)
    table_column_names = sql_cursor.fetchall()

    column_names = list()

    for name in table_column_names:
        column_names.append(name[1])

    return column_names


if __name__ == '__main__':
    items = ['Events','Births','Deaths']
    web_content = fetch_web_content()
    for item in items :
        web_scrape_and_insert_to_table(item,web_content)
