import pandas as pd


_nsdq_url = "https://datahub.io/core/nasdaq-listings/r/nasdaq-listed-symbols.csv"
_sp_url = "https://datahub.io/core/s-and-p-500-companies/r/constituents.csv"

class GetTickerClient():
    def __init__(self):
        return None

    def pullList(self, exchange):
        self.exchange = exchange
        if self.exchange == 'sp500':
            self.sp_df = pd.read_csv(_sp_url)
            self.tickers = list(self.sp_df["Symbol"])
            return self.tickers

        elif self.exchange == 'nsdq':
            self.nsdq_df = pd.read_csv(_nsdq_url)
            self.tickers = list(self.nsdq_df["Symbol"])
            return self.tickers


if __name__ == '__main__':
    print(GetTickerClient().pullList('nsdq'))
    print(GetTickerClient().pullList('sp500'))
