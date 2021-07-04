import sys
sys.path.insert(0,'/workplace/gellstorm/sqlite')
sys.path.insert(0,'/workplace/gellstorm/alpaca')

import config, requests, time, json, argparse, pytz
from datetime import datetime, timezone, timedelta
import pandas as pd
pd.set_option('display.max_rows',100)

import data_extract, data_load, sqliteUtility

my_parser = argparse.ArgumentParser()

my_parser.add_argument('-s', dest='start', help = "Start date for data extract..")

my_parser.add_argument('-e', dest='end', help = "End date for data extract.")

my_parser.add_argument('-x', dest='exchange', help = "Stock Exchange to extract data for.")

my_parser.add_argument('-g', dest='grain', help = "Grain of data extract.")
#start_time = datetime.strptime("2021-01-01", "%Y-%m-%d")

args =my_parser.parse_args()

start = args.start
end = args.end
exchange = args.exchange
grain = args.grain

start_time = datetime.strptime(start,"%Y-%m-%d")
#print(start_time)
#end_time = datetime.strptime("2021-06-23", "%Y-%m-%d")
end_time = datetime.strptime(end,"%Y-%m-%d")
#print(end_time)
sqll_util = sqliteUtility.sqliteUtility()
con = sqll_util.create_connection('/workplace/data/Gellstorm.db')

if exchange == 'sp500':
    query_exchange = 'SP500'
elif exchange == 'nsdq':
    query_exchange = 'NASDAQ'
elif exchange == 'nyse':
    query_exchange = 'NYSE'

tickers = sqll_util.execute_query(con,"select distinct ticker from def_tickers where exchange = '{}' and ticker not like '% %';".format(query_exchange))
#print(tickers)
#print(type(tickers))
tickers = list(tickers.ticker)
ticker_count = len(tickers)
DEClient = data_extract.DataExtractClient()
DLClient = data_load.DataLoadClient()
df = pd.DataFrame()


delta = timedelta(days=(end_time-start_time).days)
#print(type(start_time))
#data_dict = {}
progress = 0
for ticker in tickers:
    progress = progress + 1
    temp_start_time = start_time
    data_dict = {}
    data_dict[ticker] = {}
    counter = 0
    skip_flag = 0
    print('{}: {}, {}/{}'.format(datetime.now().strftime('%H:%M:%S'),ticker, progress, ticker_count))
    while temp_start_time < end_time:
        temp_end_time = temp_start_time + delta
        #print(temp_start_time)
        #print(temp_end_time)
        #print(start_time)
        #print(temp_end_time)
        query_start = temp_start_time.date()
        query_end = temp_end_time.date()
        #print(ticker)
        #print(start_time.isoformat())
        #print(temp_end_time.isoformat())

        #try:
        #print('Extracting Data.')
        data_json = DEClient.getBars(ticker, temp_start_time.isoformat(), temp_end_time.isoformat(), grain)
        #print(data_json)
        #print('Completed.')
        if len(data_json[ticker]['bars']) == 0:
            temp_start_time = temp_start_time + delta
            skip_flag = 1
            continue

        if counter == 0:
            data_dict[ticker]['bars'] = data_json[ticker]['bars']
        else:
            data_dict[ticker]['bars'].extend(data_json[ticker]['bars'])

        counter = counter + 1

        temp_start_time = temp_start_time + delta
        #data['t'] = datetime.fromisoformat(data['t'])


    #print(data_dict)
    if skip_flag == 1:
        skip_flag = 0
        continue
    data = DLClient.jsonClean(data_dict)
    #print(data)
    #exit(0)
    data.t = data.t.apply(lambda x: x.replace('Z', ''))
    data.t = data.t.apply(lambda x: x.replace('T', ' '))
    #print(data.t)
    #exit(0)
    data.t = pd.to_datetime(data.t, format = '%Y-%m-%d %H:%M:%S')
    data.t = data.t.dt.tz_localize(pytz.utc)
    data.t = data.t.dt.tz_convert(pytz.timezone('US/Eastern'))
    data.t = data.t.dt.tz_localize(None)
    #data.t = data.t.dt.replace(tzinfo=pytz.timezone('US/Eastern'))
    #print(data.t)
    #exit(0)
    #print(temp_start_time)
    #print(temp_end_time)
    delete_sql = "delete from dat_bars_atomic where ticker = '{}' and t between '{}' and datetime('{}','-1 minute');".format(ticker, start_time.date(), end_time.date())
        #print(delete_sql)
        #print(data.head(100))
    sqll_util.execute_statement(con, delete_sql)

    data = data[(data['t'] >= start_time) & (data['t'] <= end_time)]

    DLClient.loadInsertData(data,"dat_bars_atomic", con)
        #temp_start_time = temp_start_time + delta


    # except:
    #     print('Failed: {}'.format(ticker))
    #     continue
# with open("nsdq_2021.json", 'w') as filehandler:
#     json.dump(data, filehandler, indent=4)
#     # filehandler.writelines("%s" % data)