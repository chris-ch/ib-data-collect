import flask

import functions_framework

from services import app

import os
from google.cloud import logging

logger = logging.Client()
logger.setup_logging()


@functions_framework.http
def main(request: flask.Request):
    print(f"Using Firebase API Key: {os.getenv('GOOGLE_API_KEY')}")
    internal_context = app.test_request_context(path=request.full_path, method=request.method)
    internal_context.request.data = request.data
    internal_context.request.headers = request.headers
    internal_context.push()
    return_value = app.full_dispatch_request()
    internal_context.pop()
    return return_value
