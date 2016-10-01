import argparse
import json
import os
import logging

import httplib2

from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools

_IBROKERS_DB_FOLDER_ID = '0B4QNJgt5Fd0fQ1M0ckVBWVpJdkk'
_IBROKERS_DB_FILENAME = 'IBrokers Instruments DB'
_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))


def files(service, query):
    page_token = None
    file_fields = 'id', 'name', 'mimeType', 'modifiedTime', 'createdTime', 'webViewLink', 'parents'
    while True:
        response = service.files().list(q=query,
                                        corpus='domain',
                                        spaces='drive',
                                        fields='nextPageToken, files({})'.format(', '.join(file_fields)),
                                        pageSize=100,
                                        pageToken=page_token
                                        ).execute()
        for result in response.get('files', list()):
            yield result

        page_token = response.get('nextPageToken', None)
        if page_token is None:
            break


def children_by_id(service, folder_id):
    query = """
    mimeType {comparator} 'application/vnd.google-apps.folder'
    and trashed = False
    and '{folder_id}' in parents
    """
    all_folders = files(service, query.format(folder_id=folder_id, comparator='='))
    all_files = files(service, query.format(folder_id=folder_id, comparator='!='))
    return all_folders, all_files


def file_by_id(service, file_id):
    response = service.files().get(fileId=file_id)
    return response.execute()


def prepare_sheet(drive, sheets, folder_id, folder_name):
    folders, files = children_by_id(drive, folder_id)
    ibrokers_file_candidates = [item for item in files if item['name'] == folder_name]

    if len(ibrokers_file_candidates) == 0:
        logging.info('creating spreadsheet')
        create_body = {
            'properties': {
                'title': folder_name,
            },
            'sheets': [
                {
                    'properties': {
                        'title': 'Instruments'
                    }
                }
            ],
        }
        spreadsheet_id = sheets.spreadsheets().create(body=create_body).execute()['spreadsheetId']
        drive.files().update(fileId=spreadsheet_id,
                             addParents=folder_id,
                             removeParents='root',
                             fields='id, parents').execute()
        logging.info('created spreadsheet %s', spreadsheet_id)

    else:
        spreadsheet_id = ibrokers_file_candidates[0]['id']
        sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        logging.info('using existing spreadsheet %s', spreadsheet_id)

    return spreadsheet_id


def setup_services(client_secret_filename, api_key, args):
    store = file.Storage(os.sep.join(('output', 'token.json')))
    credentials = store.get()
    if not credentials or credentials.invalid:
        logging.info('renewing credentials')
        scopes = ('https://www.googleapis.com/auth/drive',)
        flow = client.flow_from_clientsecrets(client_secret_filename, scope=scopes)
        credentials = tools.run_flow(flow, store, args)

    authorized_http = credentials.authorize(httplib2.Http())

    drive = discovery.build('drive', 'v3', http=authorized_http, developerKey=api_key)
    sheets = discovery.build('sheets', 'v4', http=authorized_http, developerKey=api_key)
    return drive, sheets


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
    client_secret_filename = config_json['google_api_client_secret']
    api_key = config_json['api_key']
    drive, sheets = setup_services(client_secret_filename, api_key, args)
    print(prepare_sheet(drive, sheets, _IBROKERS_DB_FOLDER_ID, _IBROKERS_DB_FILENAME))

if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    file_handler = logging.FileHandler('ib-instruments.log', mode='w')
    formatter = logging.Formatter('%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    file_handler.setFormatter(formatter)
    logging.getLogger().addHandler(file_handler)
    main()
