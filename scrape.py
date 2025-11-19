import pandas as pd
import requests as r
from datetime import datetime as dt, timezone as tz
import json
import math
from flask import redirect

debug = True


def debug_print(input):
    global debug
    if debug:
        print(input)
    return

# TODO: Add error handling for network issues, invalid responses, etc.
def get_data():   
    # Will need to check this when I import onto Pi
    with open("data/lastpull.txt", "r") as f:
        datestr = f.read()
        date = dt.strptime(datestr, "%Y%m%d_%H%M")
    debug_print("Pulling new data...")

    data = pd.read_csv("data/airports.csv")
    icao_codes = data["icaoId"].dropna().to_list()
    iters = math.ceil(len(icao_codes)/400)
    debug_print("Need to download "+ str(iters) + " rounds of data")
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
        debug_print("Batch No " + str(i + 1) + " pulled, status code " + str(query.status_code))
        jquery = json.loads(query.text)
        loadobj.extend(jquery)
    json.dump(loadobj, open("data/backup.json", "w"))
    newdata = pd.json_normalize(loadobj)
    newdata["receiptTime"] = pd.to_datetime(newdata["receiptTime"])
    finalsheet = newdata.loc[newdata.groupby('icaoId')['receiptTime'].idxmax()]
    finalsheet = pd.merge(finalsheet, data, how = "left", on = "icaoId")
    finalsheet.to_csv("data/output.csv")
    with open("data/lastpull.txt", "w") as f:
        f.write(dt.now(tz.utc).strftime("%Y%m%d_%H%M"))
    return

def spout_stats():
    sheet = pd.read_csv("data/output.csv")
    sheet["reportTime"] = pd.to_datetime(sheet["reportTime"]).dt.strftime("%Y-%m-%d %H:%M UTC")
    wspd = sheet.nlargest(5, "wspd")[["reportTime","icaoId", "name", "wspd"]].to_dict(orient="records")
    wgst = sheet.nlargest(5, "wgst")[["reportTime","icaoId", "name", "wgst"]].to_dict(orient="records")
    cold = sheet.nsmallest(5, "temp")[["reportTime","icaoId", "name", "temp"]].to_dict(orient="records")
    hot =  sheet.nlargest(5, "temp")[["reportTime", "icaoId", "name", "temp"]].to_dict(orient="records")
    #precip = sheet.nlargest(5, "precip")[["reportTime", "icaoId", "Airport name", "precip"]].todict(orient='records')
    return wspd, wgst, cold, hot

