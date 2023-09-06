import os
import logging
from datetime import datetime, timezone
from operator import itemgetter

import flask
from google.cloud import datastore
from google.cloud import tasks_v2

import ibdataloader
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


def post_job_exchange(exchange_name: str, exchange_url: str):
    logging.info(f'posting url {exchange_url} for exchange {exchange_name}')
    client = tasks_v2.CloudTasksClient()
    task = tasks_v2.Task()
    #task.target = client.get_function(function_name)
    #task.queue = queue_name
    #task.project = project_id
    #return client.create_task(task)


@app.route('/launch/download/<product_type>', methods=['GET', 'POST'])
def launch_task_download(product_type: str):
    exchanges = ibdataloader.load_exchanges_for_product_type(ibdataloader.ProductType(product_type.lower()))
    for exchange_name, exchange_url in sorted(exchanges, key=itemgetter(0)):
        # posting job for every exchange
        post_job_exchange(exchange_name, exchange_url)

    return {'message': f'tasks successfully launched for product type {product_type}, loading from {len(exchanges)} exchanges'}
