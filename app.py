from flask import Flask, jsonify, render_template 
from markupsafe import escape
from dynamic import *
from apscheduler.schedulers.background import BackgroundScheduler
import logging as l

l.basicConfig(filename="app.log", level=l.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')

app = Flask(__name__ , template_folder='./templates')
app.config['SERVER_NAME'] = 'localhost:8080'

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
    
@app.route('/api/<icaoId>', methods=['GET'])
def api_airport(icaoId):
    icaoId = escape(icaoId)
    data = pd.read_csv("data/output.csv")
    data = data.loc[data["icaoId"] == icaoId]
    if data.empty:
        return jsonify({"response_status": 404})
    response = data.to_dict(orient="records")
    response= {"response_status": 200, "content":response}
    return jsonify(response)


@app.route('/updates', methods=['GET'])
def updates_page():
    # Implement the logic for the /updates endpoint
    return render_template('updates.html', updates=json.load(open("data/updates.json", "r")))