import pandas as pd
import requests as r
from datetime import datetime as dt
import json
import math


def main():
    get_data()
    
    spout_stats()
    return

def get_data():
    data = pd.read_csv("airports.csv")
    icao_codes = data["icaoId"].dropna().to_list()
    iters = math.ceil(len(icao_codes)/400)
    print(iters)
    limit = 400
    current = 0
    loadobj = []
    for i in range(iters):
        url = "https://aviationweather.gov/api/data/metar?ids="
        counter = 0
        #icao_codes = ["KJFK", "KBOS", "KMCI"]
        while counter < limit and current < len(icao_codes):
            if counter == 0:
                url = url + icao_codes[current]
            else:
                url = url + "%2C" + icao_codes[current]
            counter += 1
            current += 1
        time = dt.now().strftime("%Y%m%d_%H%M")
        url = url + "&format=json&taf=false&hours=1&date="+time
        #print(url)
        query = r.get(url)
        print(query.status_code)
        jquery = json.loads(query.text)
        loadobj.extend(jquery)
    json.dump(loadobj, open("text.json", "w"))
    newdata = pd.json_normalize(loadobj)
    newdata["receiptTime"] = pd.to_datetime(newdata["receiptTime"])
    finalsheet = newdata.loc[newdata.groupby('icaoId')['receiptTime'].idxmax()]
    finalsheet = pd.merge(finalsheet, data, how = "left", on = "icaoId")
    finalsheet.to_csv("output.csv")
    return


def spout_stats():
    sheet = pd.read_csv("output.csv")
    print(sheet.nlargest(5, "wspd")[["receiptTime","icaoId", "Airport name", "wspd"]])
    print(sheet.nlargest(5, "wgst")[["receiptTime","icaoId", "Airport name", "wgst"]])
    print(sheet.nsmallest(5, "temp")[["receiptTime","icaoId", "Airport name", "temp"]])
    print(sheet.nlargest(5, "temp")[["receiptTime", "icaoId", "Airport name", "temp"]])
    return

if __name__ == "__main__":
    main()