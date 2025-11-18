from flask import Flask, render_template
from scrape import *
from apscheduler.schedulers.background import BackgroundScheduler

app = Flask(__name__ , template_folder='./templates')
scheduler = BackgroundScheduler(timezone = "UTC")
scheduler.add_job(get_data, 'cron', minute='0, 15, 30, 45')  # Schedule to run every 15 minutes
scheduler.start()

@app.route('/')
def index():
    print("Rendering main page")
    wspd, wgst, cold, hot = spout_stats()
    with open("data/lastpull.txt", "r") as f:
        last_pull = f.read()
    last_pull_formatted = dt.strptime(last_pull, "%Y%m%d_%H%M").strftime("%Y-%m-%d %H:%M UTC")
    return render_template('index.html', top_wind_speeds=wspd, top_wind_gusts=wgst, top_cold=cold, top_hot=hot, last_pull=last_pull_formatted)