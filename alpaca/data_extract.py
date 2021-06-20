import config, requests
from datetime import datetime, timezone
import time

start_time = datetime.strptime("2021-06-01","%Y-%m-%d")
start_time = start_time.isoformat()

end_time = datetime.strptime("2021-06-02","%Y-%m-%d")
end_time = end_time.isoformat()




BARS_URL = "{}/v2/stocks/MSFT/bars?start={}Z&end={}Z&timeframe=1Day".format(config.HISTORICAL_DATA_ENDPOINT,start_time, end_time)

print(BARS_URL)
r = requests.get(BARS_URL, headers = config.HEADERS)

print(r.content)