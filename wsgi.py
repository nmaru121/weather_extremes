from werkzeug.middleware.proxy_fix import ProxyFix
from app import app as a
from apscheduler.schedulers.background import BackgroundScheduler

def setup_app(app):
    scheduler = BackgroundScheduler()
    scheduler.add_job(func=get_data, trigger='date')
    scheduler.add_job(func=get_data, trigger="cron", minute="3, 18, 33, 48")  # Fetch data every 15 minutes
    scheduler.start()
    return app

if __name__ == "__main__":
    app = a
    app = setup_app(app)
    app.wsgi_app = ProxyFix(app.wsgi_app, x_for=1, x_proto=1, x_host=1, x_prefix=1)
    app.run()