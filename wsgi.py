from werkzeug.middleware.proxy_fix import ProxyFix
from app import app
from scrape import *
from apscheduler.schedulers.background import BackgroundScheduler

if __name__ == "__main__":
    get_data()  # Initial data fetch
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=get_data, trigger="cron", minute="3, 18, 33, 48")  # Fetch data every 15 minutes
    scheduler.start()
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    app.run()