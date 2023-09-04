import os
import logging
from datetime import datetime, timezone

import flask
from google.cloud import datastore

from auth import authenticated

app = flask.Flask("internal")


def store_time(dt):
    entity = datastore.Entity(key=datastore.Client().key("visit"))
    entity.update({"timestamp": dt})
    datastore.Client().put(entity)


def fetch_times(limit):
    query = datastore.Client().query(kind="visit")
    query.order = ["-timestamp"]
    times = query.fetch(limit=limit)
    return times


@app.route('/openapi', methods=['GET', 'POST'])
def openapi():
    with authenticated() as claim:
        return {'user-data': claim.user_data}


@app.route('/', methods=['GET', 'POST'])
def index():
    with authenticated(fail_if_not=False) as claim:
        store_time(datetime.now(tz=timezone.utc))
        if claim.user_data:
            times = fetch_times(10)

        else:
            times = None

        project_id = os.getenv("GOOGLE_PROJECT_ID")
        messaging_sender_id = os.getenv("GOOGLE_MESSAGING_SENDER_ID")
        api_key = os.getenv("GOOGLE_API_KEY")
        return flask.render_template(
            "index.html",
            user_data=claim.user_data,
            error_message=claim.error_message,
            times=times,
            api_key=api_key,
            auth_domain=f"{project_id}.firebaseapp.com",
            project_id=project_id,
            messaging_sender_id=messaging_sender_id,
            storage_bucket=f"{project_id}.appspot.com"
        )
