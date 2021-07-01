
import sys
sys.path.insert(0,'/workplace/gellstorm/sqlite')
sys.path.insert(0,'/workplace/gellstorm/alpaca')

import ftplib, io, sys
import pandas as pd
import sqliteUtility, data_load, get_tickers

sqll_util = sqliteUtility.sqliteUtility()
con = sqll_util.create_connection('/workplace/data/Gellstorm.db')

df = get_tickers.GetTickerClient().pullList('nsdq')

df.drop_duplicates(subset = ['Exchange', 'Symbol'], keep = False, inplace = True)

sqll_util.execute_statement(con, "delete from def_tickers where exchange = 'NASDAQ';")


data_load.DataLoadClient().loadInsertData(df, "def_tickers", con )
    #close(filename)
print(sqll_util.execute_query(con,"select * from def_tickers where exchange = 'NASDAQ';"))
