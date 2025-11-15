import pandas as pd
import requests as r
from datetime import datetime as dt, timezone as tz
import json
import math


def main():
    get_data()
    spout_stats()
    return

def get_data():
    
    # Will need to check this when I import onto Pi
    with open("lastpull.txt", "r") as f:
        datestr = f.read()
        date = dt.strptime(datestr, "%Y%m%d_%H%M")
    if (dt.now() - date).total_seconds() < 3600:
        print("Data pulled less than an hour ago, skipping pull.")
        return
    print("Pulling new data...")
    data = pd.read_csv("airports.csv")
    icao_codes = data["icaoId"].dropna().to_list()
    iters = math.ceil(len(icao_codes)/400)
    print("Need to download "+ str(iters) + " rounds of data")
    limit = 400
    current = 0
    loadobj = []
    for i in range(iters):
        url = "https://aviationweather.gov/api/data/metar?ids="
        counter = 0
        while counter < limit and current < len(icao_codes):
            if counter == 0:
                url = url + icao_codes[current]
            else:
                url = url + "%2C" + icao_codes[current]
            counter += 1
            current += 1
        time = dt.now(tz.utc).strftime("%Y%m%d_%H%M")
        url = url + "&format=json&taf=false&hours=1&date="+time
        #print(url)
        query = r.get(url)
        print("Batch No " + str(i + 1) + " pulled, status code " + str(query.status_code))
        jquery = json.loads(query.text)
        loadobj.extend(jquery)
    json.dump(loadobj, open("text.json", "w"))
    newdata = pd.json_normalize(loadobj)
    newdata["receiptTime"] = pd.to_datetime(newdata["receiptTime"])
    finalsheet = newdata.loc[newdata.groupby('icaoId')['receiptTime'].idxmax()]
    finalsheet = pd.merge(finalsheet, data, how = "left", on = "icaoId")
    finalsheet.to_csv("output.csv")
    with open("lastpull.txt", "w") as f:
        f.write(dt.now().strftime("%Y%m%d_%H%M"))
    return


def spout_stats():
    sheet = pd.read_csv("output.csv")
    print(sheet.nlargest(5, "wspd")[["receiptTime","icaoId", "Airport name", "wspd"]])
    print(sheet.nlargest(5, "wgst")[["receiptTime","icaoId", "Airport name", "wgst"]])
    print(sheet.nsmallest(5, "temp")[["receiptTime","icaoId", "Airport name", "temp"]])
    print(sheet.nlargest(5, "temp")[["receiptTime", "icaoId", "Airport name", "temp"]])
    print(sheet.nlargest(5, "precip")[["receiptTime", "icaoId", "Airport name", "precip"]])
    return

if __name__ == "__main__":
    main()