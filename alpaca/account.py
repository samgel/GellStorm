import config, requests


ACCOUNT_URL = "{}/v2/account".format(config.BASE_ENDPOINT)
r = requests.get(ACCOUNT_URL, headers = config.HEADERS)

print(r.content)