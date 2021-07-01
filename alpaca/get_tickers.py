import pandas as pd
import ftplib, io, string, requests
from bs4 import BeautifulSoup


#_nsdq_url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
#_sp_url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"
_sp500_url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
_nyse_url_template = "https://en.wikipedia.org/wiki/Companies_listed_on_the_New_York_Stock_Exchange_"#(A)"

class GetTickerClient():
    def __init__(self):
        return None

    def pullList(self, exchange):
        self.exchange = exchange

        if self.exchange == 'sp500':
            # self.sp_df = pd.read_csv(_sp_url)
            # self.tickers = list(self.sp_df["Symbol"])
            self.tickers = []
            self.names = []
            self.url = _sp500_url
            # Get content from letter in NYSE
            self.page = requests.get(self.url)
            self.soup = BeautifulSoup(self.page.content, 'html.parser')

            self.tables = self.soup.find_all("table")

            # Pulling second table (assuming structure is same on each page)

            self.rows = self.tables[0].find_all("tr")
            # Ignore first row (header)
            for row in self.rows[1:]:
                # print(row)
                self.cols = row.find_all("td")
                # 2nd column should hold Ticker Symbol
                self.tickers.append(self.cols[0].text.strip())
                self.names.append(self.cols[1].text.strip())

            self.tickers = pd.DataFrame(self.tickers,columns=['Symbol'])
            self.tickers['Name'] = pd.Series(self.names,index=self.tickers.index)
            self.tickers['Exchange'] = pd.Series(['SP500' for x in range(len(self.tickers.index))], index=self.tickers.index)
            self.tickers = self.tickers[['Exchange','Symbol','Name']]
            return self.tickers

        if self.exchange == 'nyse':
            self.tickers = []
            self.names = []
            self.alphabet_list = list(string.ascii_uppercase)

            for letter in self.alphabet_list:
                self.url = _nyse_url_template + "(" + letter + ")"
                # Get content from letter in NYSE
                self.page = requests.get(self.url)
                self.soup = BeautifulSoup(self.page.content, 'html.parser')

                self.tables = self.soup.find_all("table")

                # Pulling second table (assuming structure is same on each page)

                self.rows = self.tables[1].find_all("tr")
                # Ignore first row (header)
                for row in self.rows[1:]:
                    # print(row)
                    self.cols = row.find_all("td")
                    # 2nd column should hold Ticker Symbol
                    self.tickers.append(self.cols[1].text.strip())
                    self.names.append(self.cols[0].text.strip())

            self.tickers = pd.DataFrame(self.tickers,columns=['Symbol'])
            self.tickers['Name'] = pd.Series(self.names, index=self.tickers.index)
            self.tickers['Exchange'] = pd.Series(['NYSE' for x in range(len(self.tickers.index))], index=self.tickers.index)
            self.tickers = self.tickers[['Exchange','Symbol','Name']]
            return self.tickers

        elif self.exchange == 'nsdq':
            # self.nsdq_df = pd.read_csv(_nsdq_url)
            # self.tickers = list(self.nsdq_df["Symbol"])
            self.filename = 'nasdaqlisted.txt'

            self.ftp = ftplib.FTP("ftp.nasdaqtrader.com")
            self.ftp.login("Anonymous", "guest")
            #ftp.cwd("Symboldirectory")
            # ftp.dir()
            # ftp.retrbinary("RETR /symboldirectory/" + filename, open(filename,'wb').write)
            # file = ftp.retrbinary("RETR /symboldirectory/" + filename, writeFunc)
            self.file = io.BytesIO()

            self.ftp_retr_string = "RETR /Symboldirectory/" + self.filename

            #print(ftp_retr_string)
            self.ftp.retrbinary(self.ftp_retr_string, self.file.write)
            self.file.seek(0)

            self.file = io.StringIO(self.file.read().decode().replace('|', '__'))

            self.file.seek(0)

            self.df = pd.read_csv(self.file, sep='__', encoding='utf-8')

            self.df['Exchange'] = pd.Series(['NASDAQ' for x in range(len(self.df.index))], index=self.df.index)
            self.df.drop(self.df.tail(1).index, inplace=True)
            self.tickers = self.df[['Exchange', 'Symbol','Security Name']]
            self.tickers.columns = ['Exchange', 'Symbol', 'Name']

            return self.tickers


if __name__ == '__main__':
    print(GetTickerClient().pullList('nsdq'))
    #print(GetTickerClient().pullList('nyse'))
    #print(GetTickerClient().pullList('sp500'))
