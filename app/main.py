import os
import logging
from datetime import datetime, timezone
from operator import itemgetter
import flask

import ibdataloader
from auth import authenticated

import google.cloud.logging
from google.cloud import tasks_v2
from google.cloud import datastore


app = flask.Flask(__name__)


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
    logging.info(f"Using Firebase API Key: {os.getenv('GOOGLE_API_KEY')}")
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
    # Initialize request argument(s)
    request = tasks_v2.CreateTaskRequest(
        parent="parent_value",
    )

    # Make the request
    response = client.create_task(request=request)
    function_name = ''
    target = client.get_target(
        name=f"projects/{function_name.split('.')[0]}/locations/global/functions/{function_name.split('.')[1]}")

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


if __name__ == "__main__":
    log_client = google.cloud.logging.Client(project=os.getenv('GOOGLE_PROJECT_ID'))
    log_client.setup_logging(log_level=logging.INFO)
    logger = logging.getLogger()
    if os.getenv("LOCAL_LOGGING", "False").upper() in ("TRUE", "ON", "ENABLED"):
        # output logs to console - otherwise logs are only visible when running in GCP
        console_output = logging.StreamHandler()
        console_output.setFormatter(logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s'))
        logging.getLogger().addHandler(console_output)

    is_debug = os.getenv("FLASK_DEBUG", "True")
    if is_debug:
        logging.warning('flask: DEBUG mode is on')
    app.run(debug=is_debug.upper() in ("TRUE", "ON", "ENABLED"), host="0.0.0.0", port=int(os.environ.get("PORT", "8080")))
