import urllib.parse
import os

import requests
import json

import pandas as pd

headers = {
    # Request headers
    'Ocp-Apim-Subscription-Key': os.environ.get('ns_api', '<NS_API_KEY>')
}

params = urllib.parse.urlencode({
})

try:
    timeout = 50

    session = requests.Session()
    adapter = requests.adapters.HTTPAdapter(pool_connections=32, pool_maxsize=32)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    url = f"https://gateway.apiportal.ns.nl/reisinformatie-api/api/v2/stations?{params}"
    response = session.get(url=url, headers=headers)
    content = json.loads(response.text)

    df = pd.DataFrame(content['payload'])
    df['stnname'] = df['namen'].apply(pd.Series)['lang']

    # Reduce to needed data
    df = df[(df['stationType'] == 'MEGA_STATION') |
            (df['stnname'] == 'Vlissingen') |
            (df['stnname'] == 'Delft')]
    df = df[df['land'] == 'NL']

    # Make up bike capacity
    df['bike_capacity'] = 20
    df = df[['UICCode', 'code', 'lat', 'lng', 'bike_capacity', 'stnname']]
    df.to_csv('stations.csv', index=False)
except Exception as e:
    print("[Errno {0}] {1}".format(e.errno, e.strerror))
