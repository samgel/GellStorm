import config, requests, time, json
from datetime import datetime, timezone
from get_tickers import GetTickerClient

#
# start_time = datetime.strptime("2021-06-01","%Y-%m-%d")
# start_time = start_time.isoformat()
#
# end_time = datetime.strptime("2021-06-02","%Y-%m-%d")
# end_time = end_time.isoformat()
#

class DataExtractClient():
    def __init__(self):
        pass

    def getBars(self, ticker, start_time, end_time, time_frame):
        self.ticker = ticker
        self.start_time = start_time
        self.end_time = end_time
        self.time_frame = time_frame
        self.ticker = ticker
        self.extract = {}


        #print("Pulling data for: %s" % self.ticker)
        bars_url = "{}/v2/stocks/{}/bars?start={}Z&end={}Z&timeframe={}&limit=10000".format(config.HISTORICAL_DATA_ENDPOINT, self.ticker, self.start_time, self.end_time, self.time_frame)
        r = requests.get(bars_url, headers = config.HEADERS)

        if r.status_code == 200:
            json_string = r.content.decode('utf-8')
            #print(json_string)
            self.extract[ticker] = json.loads(json_string)
            #print("Data pulled successfully for: %s" % ticker)

        #print("Data pulled unsuccessfully for: %s" % ticker)

        return self.extract


if __name__ == '__main__':
    start_time = datetime.strptime("2021-01-01","%Y-%m-%d")
    start_time = start_time.isoformat()

    end_time = datetime.strptime("2021-06-23","%Y-%m-%d")
    end_time = end_time.isoformat()

    tickers = GetTickerClient().pullList('nsdq')

    DEClient = DataExtractClient()
    data = DEClient.getBars(tickers, start_time, end_time, '1Day')
    with open("nsdq_2021.json", 'w') as filehandler:
        json.dump(data, filehandler, indent=4)
        #filehandler.writelines("%s" % data)