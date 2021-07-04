
import sys
sys.path.insert(0,'/workplace/gellstorm/sqlite')
sys.path.insert(0,'/workplace/gellstorm/alpaca')

import ftplib, io, sys, argparse
import pandas as pd
import sqliteUtility, data_load, get_tickers

sqll_util = sqliteUtility.sqliteUtility()
con = sqll_util.create_connection('/workplace/data/Gellstorm.db')

my_parser = argparse.ArgumentParser()
my_parser.add_argument('-e',dest='exchange', help="Exchange to pull tickers from.")

args = my_parser.parse_args()

exchange = args.exchange

if exchange == 'sp500':
    query_exchange = 'SP500'
elif exchange == 'nsdq':
    query_exchange = 'NASDAQ'
elif exchange == 'nyse':
    query_exchange = 'NYSE'

df = get_tickers.GetTickerClient().pullList(exchange)


df.drop_duplicates(subset = ['Exchange', 'Symbol'], keep = False, inplace = True)

sqll_util.execute_statement(con, "delete from def_tickers where exchange = '{}';".format(query_exchange))



data_load.DataLoadClient().loadInsertData(df, "def_tickers", con )
    #close(filename)
print(sqll_util.execute_query(con,"select * from def_tickers"))
