import flask

import functions_framework

import ibdataloader
from services import app

import os

import google.cloud.logging
import logging


log_client = google.cloud.logging.Client(project=os.getenv('GOOGLE_PROJECT_ID'))
log_client.setup_logging(log_level=logging.INFO)
logger = logging.getLogger()
if os.getenv("LOCAL_LOGGING", "False") == "True":
    # output logs to console - otherwise logs are only visible when running in GCP
    console_output = logging.StreamHandler()
    console_output.setFormatter(logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s'))
    logging.getLogger().addHandler(console_output)


@functions_framework.http
def main(request: flask.Request):
    logging.info(f"Using Firebase API Key: {os.getenv('GOOGLE_API_KEY')}")
    internal_context = app.test_request_context(path=request.full_path, method=request.method)
    internal_context.request.data = request.data
    internal_context.request.headers = request.headers
    internal_context.push()
    return_value = app.full_dispatch_request()
    internal_context.pop()
    return return_value


@functions_framework.http
def job_start_exchange(request: flask.Request):
    # extract exchange_name, exchange_url and product_type
    exchange_name = ''
    exchange_url = ''
    product_type = ibdataloader.ProductType.ETF
    exchange_instruments = ibdataloader.load_for_exchange(exchange_name, exchange_url)
    for instrument in exchange_instruments:
        instrument.product_type = product_type
