import sqlite3
from sqlite3 import Error
import pandas as pd
import os
from lxml import html
import requests
from collections import OrderedDict
import time as t
import datetime
import csv

#get the current working directory
print(os.getcwd())

#read in the file on a list of stock to query
with open("./stocklist.csv", mode='r') as infile:
    reader = csv.reader(infile)
    mydict = {rows[0]:rows[1] for rows in reader}
    
#++++++++++++++++++++++++++++++++++++++++++++ functions +++++++++++++++++++++++++++++++++++++++++++++
#function for table creation
def create_connection(db_file):
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        return conn
    except Error as e:
        print(e)

    return conn
    
def connection_to_db(dir):
    try:

        connection = sqlite3.connect(dir)
        cur = connection.cursor()

        return connection, cur

    except Exception as error:
        print(error)

def create_table(conn, create_table_sql):
    try:
        c = conn.cursor()
        c.execute(create_table_sql)
    except Error as e:
        print(e) 

def add_data(conn,data):
    sql = ''' INSERT INTO stock(timestamp, name, previousclose, open, bid, ask, day_range, week52_range, volume, avg_volume, market_cap, beta_5Y_monthly, pe_ratio, eps, earning_date, forward_dividend_yield, ex_dividend_dt, target_1Y_est, currentprice)
              VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) '''
    cur = conn.cursor()
    cur.execute(sql, data)
    conn.commit()
    return cur.lastrowid

def query(connection, target, source):

    con = connection
    query = "SELECT " + target + " from " + source
    df = pd.read_sql_query(query,con)
    
    return df

def delete_data(conn,name):
    sql =  'DELETE FROM stock WHERE name = ?'
    
    cur = conn.cursor()
    cur.execute(sql,(name))
    conn.commit()

def parse(ticker):
    summary_data = OrderedDict()
    url = "http://finance.yahoo.com/quote/%s?p=%s" % (ticker, ticker)
    response = requests.get(url, timeout=30)
    print("Parsing %s" % (url))
    parser = html.fromstring(response.text)
    summary_table = parser.xpath('//div[contains(@data-test,"summary-table")]//tr')
    currentPrice = parser.xpath('//span[@data-reactid="32"]//text()')
    try:
        for table_data in summary_table:
            raw_table_key = table_data.xpath(
                './/td[1]//text()')
            raw_table_value = table_data.xpath(
                './/td[2]//text()')
            table_key = ''.join(raw_table_key).strip()
            table_value = ''.join(raw_table_value).strip()
            summary_data.update({table_key: table_value})

        summary_data.update({'Current Price': currentPrice[0]})
        
        return summary_data
    except:
        return {"error": "Unhandled Error"}

#++++++++++++++++++++++++++++++++++++++++++++ functions +++++++++++++++++++++++++++++++++++++++++++++
#indicate a db file
database = r"demo.db"
sql_create_table = """ CREATE TABLE IF NOT EXISTS stock(
                                    timestamp text,
                                    name text, 
                                    previousclose text, 
                                    open text, 
                                    bid text, 
                                    ask text, 
                                    day_range text, 
                                    week52_range text, 
                                    volume text, 
                                    avg_volume text, 
                                    market_cap text, 
                                    beta_5Y_monthly text, 
                                    pe_ratio text, 
                                    eps text, 
                                    earning_date text, 
                                    forward_dividend_yield text, 
                                    ex_dividend_dt text, 
                                    target_1Y_est text, 
                                    currentprice text
                                ); """

# create a database connection to the db file
conn = create_connection(database)

# create tables
if conn is not None:
    # create projects table
    create_table(conn, sql_create_table)
else:
    print("Error! cannot create the database connection.")

for i in mydict.items():
    scraped_data = parse(i[1])
    addon_list = [datetime.datetime.now(),i[0]]
    data = addon_list + list(scraped_data.values())
    entry = add_data(conn, data)

x = query(conn,'open,volume','stock')

#close db connection
#conn.close()