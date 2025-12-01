import pandas as pd
import requests as r
from datetime import datetime as dt, timezone as tz
import json
import math
from flask import redirect
from time import sleep
import logging as l


#formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')

#l = logging.getLogger(__name__)
#file_handler = logging.FileHandler('app.log')
#file_handler.setFormatter(formatter)
#l.addHandler(file_handler)

# TODO: Logging system
# TODO: Add error handling for network issues, invalid responses, etc.
def get_data():   
    # Will need to check this when I import onto Pi
    exit_code = 0
    l.info("Pulling new data...")

    data = pd.read_csv("data/airports.csv")
    icao_codes = data["icaoId"].dropna().to_list()
    iters = math.ceil(len(icao_codes)/400)
    l.debug("Need to download "+ str(iters) + " rounds of data")
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
        try:
            query = r.get(url)
            l.info("Batch No " + str(i + 1) + " pulled, status code " + str(query.status_code))
            jquery = json.loads(query.text)
            loadobj.extend(jquery)
        except Exception as e:
            l.error("Error during request: " + str(i + 1) + str(e))
            exit_code = 1
            continue
    json.dump(loadobj, open("data/backup.json", "w"))
    newdata = pd.json_normalize(loadobj)
    newdata["receiptTime"] = pd.to_datetime(newdata["receiptTime"])
    newdata["orig_name"] = newdata["name"]
    newdata["letterCode2"] = newdata["name"].str.split(",").str[2].str.strip()
    iso_codes = pd.read_csv("data/iso_codes.csv")[["country", "letterCode2"]]
    newdata = pd.merge(newdata, iso_codes, how="left", on="letterCode2")
    newdata["name"] = newdata["name"].str.split(",").str[0].str.strip()
    finalsheet = newdata.loc[newdata.groupby('icaoId')['receiptTime'].idxmax()]
    # finalsheet = pd.merge(finalsheet, data, how = "left", on = "icaoId") Not necessary, and does not work
    finalsheet.to_csv("data/output.csv")
    with open("data/lastpull.txt", "w") as f:
        f.write(dt.now(tz.utc).strftime("%Y%m%d_%H%M"))
    return exit_code

#TODO Low priority, but could we optimize by appending on the second run?
def run_pull():
    exit_code = get_data()
    if exit_code != 0:
        sleep(60)
        get_data()
    return

def spout_stats():
    sheet = pd.read_csv("data/output.csv")
    sheet[["name", "country"]] = sheet[["name", "country"]].fillna('None found')
    l.DEBUG(sheet)
    sheet["reportTime"] = pd.to_datetime(sheet["reportTime"]).dt.strftime("%Y-%m-%d %H:%M UTC")
    wspd = sheet.nlargest(5, "wspd")[["reportTime","icaoId", "name", "country", "wspd"]].to_dict(orient="records")
    wgst = sheet.nlargest(5, "wgst")[["reportTime","icaoId", "name", "country", "wgst"]].to_dict(orient="records")
    cold = sheet.nsmallest(5, "temp")[["reportTime","icaoId", "name", "country", "temp"]].to_dict(orient="records")
    hot =  sheet.nlargest(5, "temp")[["reportTime", "icaoId", "name", "country", "temp"]].to_dict(orient="records")
    #precip = sheet.nlargest(5, "precip")[["reportTime", "icaoId", "Airport name", "precip"]].todict(orient='records')
    return wspd, wgst, cold, hot

