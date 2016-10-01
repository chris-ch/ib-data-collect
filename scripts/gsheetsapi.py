"""
Demo showing how to use some of the functions from the Drive v3 and Sheets v4 Google API.
Updates (or creates if not existing) a spreadsheet under a folder specified by its id
and updates a cell with the current date.

Requirements: a client secret json file and a json config file:
{
  "api_key": "<YOUR_API_KEY>",
}

Generating those files is not so much of a pain but takes nevertheless too long to explain here.
Please Google for it and try to avoid the misleading links (sadly, that amounts to 95% of them).

"""
import argparse
import json
import os
import logging
from datetime import datetime

import httplib2

from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools

_SHEET_FILENAME = 'My Demo Sheet'
_DEFAULT_CONFIG_FILE = os.sep.join(('.', 'config.json'))
_DEFAULT_CLIENT_SECRET_FILE = os.sep.join(('.', 'client-secret.json'))


def files(service, query):
    """
    Lists files and folders using the specified query.
    :param service:
    :param query:
    :return:
    """
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
    """
    Lists children for the specified id.
    :param service:
    :param folder_id:
    :return:
    """
    query = """
    mimeType {comparator} 'application/vnd.google-apps.folder'
    and trashed = False
    and '{folder_id}' in parents
    """
    all_folders = files(service, query.format(folder_id=folder_id, comparator='='))
    all_files = files(service, query.format(folder_id=folder_id, comparator='!='))
    return all_folders, all_files


def main():
    parser = argparse.ArgumentParser(description='Google API demo',
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     parents=[tools.argparser]
                                     )
    parser.add_argument('target_folder_id',
                        type=str,
                        help='folder id to be found on Google Drive GUI directly'
                        )
    parser.add_argument('--config',
                        metavar='JSON_FILENAME',
                        type=str,
                        help='location of config file, using "{}" by default'.format(_DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CONFIG_FILE
                        )
    parser.add_argument('--google-client-secret',
                        metavar='CLIENT_SECRET_JSON',
                        type=str,
                        help='location of google client secret file, using "{}" by default'.format(
                            _DEFAULT_CONFIG_FILE),
                        default=_DEFAULT_CLIENT_SECRET_FILE
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
        client_secret_filename = args.google_client_secret
        flow = client.flow_from_clientsecrets(client_secret_filename, scope=scopes)
        credentials = tools.run_flow(flow, store, args)

    api_key = config_json['api_key']
    authorized_http = credentials.authorize(httplib2.Http())

    drive = discovery.build('drive', 'v3', http=authorized_http, developerKey=api_key)
    sheets = discovery.build('sheets', 'v4', http=authorized_http, developerKey=api_key)
    target_folder_id = args.target_folder_id
    folders, files = children_by_id(drive, target_folder_id)

    # file names are not unique so we need to be careful when we access by name
    # Here only the first occurrence is considered... If no occurrence is found a new file is created
    target_file_candidates = [item for item in files if item['name'] == _SHEET_FILENAME]
    if len(target_file_candidates) == 0:
        logging.info('creating spreadsheet')
        create_body = {
            'name': _SHEET_FILENAME,
            'parents': [target_folder_id],
            'mimeType': 'application/vnd.google-apps.spreadsheet',
        }
        spreadsheet = drive.files().create(body=create_body).execute()
        spreadsheet_id = spreadsheet['id']
        logging.info('updating spreadsheet: %s', spreadsheet_id)
        update_body = {'requests': [
            {
                'updateSheetProperties': {
                    'properties': {'title': 'NEW_SHEET_NAME'},
                    'fields': 'title',
                },
            },
            {'addSheet': {
                'properties': {'title': 'ADDED_SHEET_NAME', 'index': 0}
            },
            },
        ],
        }
        sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=update_body).execute()

    else:
        spreadsheet_id = target_file_candidates[0]['id']
        sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
        logging.info('using existing spreadsheet %s', spreadsheet_id)

    # ready to edit spreadsheet <spreadsheet_id>
    cell_update_body = {
        'range': 'A1:B2',
        'majorDimension': 'ROWS',
        'values': [[1, 2], [3, datetime.now().isoformat()]],
    }
    sheets.spreadsheets().values().update(spreadsheetId=spreadsheet_id,
                                          range='A1:B2',
                                          body=cell_update_body,
                                          valueInputOption='RAW'
                                          ).execute()
    logging.info('updated spreadsheet %s', spreadsheet_id)


if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO, format='%(asctime)s:%(name)s:%(levelname)s:%(message)s')
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('googleapiclient.discovery').setLevel(logging.WARNING)
    main()
