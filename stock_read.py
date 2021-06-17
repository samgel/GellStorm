from bs4 import BeautifulSoup
import datetime, time
import requests
import pandas as pd
import os.path
from os import path
import string
from io import StringIO
import time

#url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
url_template = "https://en.wikipedia.org/wiki/Companies_listed_on_the_New_York_Stock_Exchange_"#(A)"

url_historicals_template_1 = "https://query1.finance.yahoo.com/v7/finance/download/"

url_historicals_template_2 = "?period1=433728000&period2=1622505600&interval=1d&events=history&includeAdjustedClose=true"

url_summary_template = "https://finance.yahoo.com/quote/"

alphabet_list = list(string.ascii_uppercase)
tickers = []


for letter in alphabet_list:
    url = url_template+"("+letter+")"
    #Get content from letter in NYSE
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    tables = soup.find_all("table")

    #Pulling second table (assuming structure is same on each page)

    rows = tables[1].find_all("tr")
    #Ignore first row (header)
    for row in rows[1:]:
        #print(row)
        cols = row.find_all("td")
        #2nd column should hold Ticker Symbol
        tickers.append(cols[1].text.strip())


# for table in tables:
#     print(count)
#     print(table)
#     count = count + 1
#print(table)

# tickers = []
# for row in table.find_all('tr'):
#     cols = row.find_all('td')
#     if cols:
#         tickers.append(cols[0].text.strip())
#
# for ticker in tickers:
#     print(ticker)


#stock_data = pd.DataFrame(columns=["Date","Open","High","Low","Close","Adj Close", "Volume"])

stock_data = []

counter = 0

for ticker in tickers:
    print(ticker)

    #historical data
    code = 0
    retries = 0
    retry_limit = 5
    while code not in [200, 404] and retries <= retry_limit:
        url = url_historicals_template_1 + ticker + url_historicals_template_2
        print(url)
        response = requests.get(url)
        print(response.status_code)
        code = response.status_code
        retries = retries + 1
        #if code in [200, 404]:
        #    break
        time.sleep(5)

    if code != 200:
        print("Unable to pull historicals, skipping")
        continue

    #data = response.content.decode('utf-8')
    data = str(response.content)
    data = data.replace("b'","")
    data = data.replace("'","")
    data = data.replace("\\n","\n"+ticker+",")

    if counter == 0:
        data = 'Ticker,'+data
    else:
        data = data.replace("Date,Open,High,Low,Close,Adj Close,Volume","")
    #print(data.encode("utf-8"))
    #data = bytes(data).decode("utf-8")
    #data = data.encode(encoding = "utf8")
    #print (data)
    #break

    #df = pd.read_csv(StringIO(data.content.decode('utf-8')))
    # stock_data.append(other = df, ignore_index=True)
    stock_data.append(data)
    print("historicals pulled successfully")

    # if counter == 2:
    #     break
    #Skip divs for testing purposes
    counter = counter + 1
    continue

    url = url_summary_template + ticker
    print(url)
    page = requests.get(url)
    soup = BeautifulSoup(page.content, 'html.parser')

    try:
        tables = soup.find_all("table")

        yield_stop_list = ["(", ")", "%"]

        rows = tables[1].find_all("tr")
        cols = rows[5].find_all("td")
        dividend_rate_yield = str(cols[1].text.strip()).split()
        #print(dividend_rate_yield)
        if dividend_rate_yield[0] == 'N/A':
            print("No dividend, using 0")
            df["dividend"] = 0.00
            continue
        else:
            dividend_rate = float(dividend_rate_yield[0])
            dividend_yield = dividend_rate_yield[1]

            for stop in yield_stop_list:
                dividend_yield = dividend_yield.replace(stop, "")


            dividend_yield = float(dividend_yield)

            df["dividend"] = dividend_rate
            print("Dividend pulled successfully")
    except:
        print("Unable to pull dividend, using Null")
        df["dividend"] = None
    stock_data = stock_data.append(other = df, ignore_index=True)
    print("Ticker " + ticker + " data appended successfully")
    # except:
    #     print("Failed for ticker: " + ticker)
    #     break

with open('stock_data.txt', 'w') as filehandle:
    filehandle.writelines("%s" % db for db in stock_data)
    #filehandle.write(data)
#print(stock_data.head())
#stock_data.to_csv('stock_data.csv', index = False)