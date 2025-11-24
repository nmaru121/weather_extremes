from flask import Flask, jsonify, render_template
from scrape import *
from apscheduler.schedulers.background import BackgroundScheduler
import logging


app = Flask(__name__ , template_folder='./templates')

initialize = False

@app.before_request
def before():
	global initialize
	if initialize == True:
		return
	scheduler = BackgroundScheduler()
	scheduler.add_job(func=run_pull, trigger="date", run_date=dt.now())  # Initial data fetch
	scheduler.add_job(func=run_pull, trigger="cron", minute="3, 18, 33, 48")  # Fetch data every 15 minutes
	scheduler.start()
	initialize = True
	return

@app.route('/')
def index():
    l.info("Rendering main page")
    return render_template('index.html')

@app.route('/api/stats', methods=['GET'])
def api_stats():
    wspd, wgst, cold, hot = spout_stats()
    with open("data/lastpull.txt", "r") as f:
        last_pull = f.read()
    last_pull_formatted = dt.strptime(last_pull, "%Y%m%d_%H%M").strftime("%Y-%m-%d %H:%M UTC")
    response = {
        "top_wind_speeds": wspd,
        "top_wind_gusts": wgst,
        "top_cold": cold,
        "top_hot": hot,
        "last_pull": last_pull_formatted
    }
    return jsonify(response)

# TODO: Search for your closest airport
