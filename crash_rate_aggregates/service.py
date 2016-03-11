from flask import Flask, jsonify, request
import config

from gevent.monkey import patch_all
from psycogreen.gevent import patch_psycopg
from psycopg2.pool import SimpleConnectionPool

app = Flask(__name__)
app.config.from_object("config")

patch_all()
patch_psycopg()

pool = SimpleConnectionPool(app.config["MIN_DB_CONNECTIONS"], app.config["MAX_DB_CONNECTIONS"], dsn=app.config["DB_CONNECTION_STRING"])

def query(query, params=()):
    global pool
    conn = pool.getconn()
    try:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return cursor.fetchall()
    finally:
        pool.putconn(conn)

@app.route("/crash_rates_by")
def crash_rates():
    # it's very important to ensure that these values are escaped properly!
    metric_mappings = {
        "build_id": "build_id",
        "channel": "channel",
        "os_name": "os_name",
        "os_version": "os_version",
        "locale": "locale",
        "experiment_id": "experiment_id",
        "experiment_branch": "experiment_branch",
        "e10s_enabled": "e10s_enabled",
    }
    #for name in request.args:
    filters = " and ".join("{} = {}".format(metric, values))
    if filters: filters = " where " + filters

    result = query("""
    select main_crash_rate, content_crash_rate, plugin_crash_rate
    from aggregates{}
    """.format(filters))
    return jsonify(
        main_process_crashes
    )