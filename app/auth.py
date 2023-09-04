from contextlib import contextmanager
from typing import NamedTuple, Dict, Any, Optional

import flask
import google.oauth2.id_token
from google.auth.transport import requests


class Claim(NamedTuple):
    user_data: Optional[Dict[str, Any]]
    error_message: Optional[str]


@contextmanager
def authenticated(fail_if_not: bool = True):
    # Code to acquire resource, e.g.:
    id_token = flask.request.cookies.get("token")
    if id_token:
        try:
            # Verify the token against the Firebase Auth API. This example
            # verifies the token on each page load. For improved performance,
            # some applications may wish to cache results in an encrypted
            # session store (see for instance
            # http://flask.pocoo.org/docs/1.0/quickstart/#sessions).
            user_data = google.oauth2.id_token.verify_firebase_token(id_token, requests.Request())
            yield Claim(user_data=user_data, error_message=None)

        except ValueError as err:
            if fail_if_not:
                flask.abort(flask.Response('invalid token', 401))
            else:
                yield Claim(user_data=None, error_message=str(err))

    elif fail_if_not:
        flask.abort(flask.Response('unauthorized', 401))

    else:
        yield Claim(user_data=None, error_message=None)
