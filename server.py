from flask import Flask, json
from flask_cors import CORS
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.interval import IntervalTrigger
import atexit
import database
import etl_controller

api = Flask(__name__)
CORS(api)


def startServer():
    startScheduledETL()
    api.run()


@api.route('/charge_points', methods=['GET'])
def getChargePoints():
    return json.dumps(database.getChargePoints())


@api.route('/date_interval', methods=['GET'])
def getDateInterval():
    return json.dumps(database.getMaxMinDate())


def startScheduledETL():
    # create schedule for ETL
    scheduler = BackgroundScheduler()
    scheduler.start()
    scheduler.add_job(
        func=etl_controller.initETL,
        trigger=IntervalTrigger(weeks=4),
        id='etl_job',
        name='Check new data every month',
        replace_existing=True)
    # Shut down the scheduler when exiting the app
    atexit.register(lambda: scheduler.shutdown())
