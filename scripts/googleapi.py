import argparse
import json
import os
from pprint import pprint
import logging

import httplib2

from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools

_IBROKERS_DB_FOLDER_ID = '0B4QNJgt5Fd0fQ1M0ckVBWVpJdkk'
_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))


def files(service, query):
    page_token = None
    while True:
        response = service.files().list(q=query,
                                        corpus='domain',
                                        spaces='drive',
                                        fields='nextPageToken, files(id, modifiedTime, createdTime, webViewLink, parents)',
                                        pageSize=100,
                                        pageToken=page_token
                                        ).execute()
        for result in response.get('files', list()):
            yield result

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break


def children_files_by_id(service, folder_id):
    query = """
    mimeType != 'application/vnd.google-apps.folder'
    and trashed = False
    and '{}' in parents
    """.format(folder_id)
    return files(service, query)


def file_by_id(service, file_id):
    response = service.files().get(fileId=file_id)
    return response.execute()


def main():
    parser = argparse.ArgumentParser(description='Loading instruments data from IBrokers',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     parents=[tools.argparser]
                                     )
    parser.add_argument('--config',
                        metavar='JSON_FILENAME',
                        type=str,
                        help='location of config file, using "{}" by default'.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )

    args = parser.parse_args()
    full_config_path = os.path.abspath(args.config)
    logging.info('reading from default config "%s"', full_config_path)
    if not os.path.isfile(full_config_path):
        raise RuntimeError('unable to load config file: {}'.format(full_config_path))

    config_json = json.load(open(args.config, 'rt'))

    store = file.Storage(os.sep.join(('output', 'token.json')))
    credentials = store.get()
    if not credentials or credentials.invalid:
        logging.info('renewing credentials')
        scopes = ('https://www.googleapis.com/auth/drive',)
        client_secret_filename = config_json['google_api_client_secret']
        flow = client.flow_from_clientsecrets(client_secret_filename, scope=scopes)
        credentials = tools.run_flow(flow, store, args)

    api_key = config_json['api_key']
    authorized_http = credentials.authorize(httplib2.Http())
    service = discovery.build('drive', 'v3', http=authorized_http, developerKey=api_key)
    print(file_by_id(service, _IBROKERS_DB_FOLDER_ID))
    print(list(children_files_by_id(service, _IBROKERS_DB_FOLDER_ID)))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('ib-instruments.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)

    main()
