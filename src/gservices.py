import logging

import httplib2

from apiclient import discovery
from oauth2client import file
from oauth2client import client
from oauth2client import tools


def files(drive, query):
    page_token = None
    file_fields = 'id', 'name', 'mimeType', 'modifiedTime', 'createdTime', 'webViewLink', 'parents'
    while True:
        response = drive.files().list(q=query,
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


def children_by_id(drive, folder_id):
    query = """
    mimeType {comparator} 'application/vnd.google-apps.folder'
    and trashed = False
    and '{folder_id}' in parents
    """
    all_folders = files(drive, query.format(folder_id=folder_id, comparator='='))
    all_files = files(drive, query.format(folder_id=folder_id, comparator='!='))
    return all_folders, all_files


def file_by_id(drive, file_id):
    response = drive.files().get(fileId=file_id)
    return response.execute()


def prepare_sheet(drive, sheets, folder_id, folder_name):
    """
    Finds the sheet id for the specified name and creates the sheet if needed.

    :param drive: Google Drive service
    :param sheets: Google sheets service
    :param folder_id:
    :param folder_name:
    :return:
    """
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
                        'title': 'IB Instruments'
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


def setup_services(client_secret_filename, api_key, token_filename, args):
    store = file.Storage(token_filename)
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


def update_sheet(sheets, spreadsheet_id, header, rows):
    """
    Updates the first available sheet from spreadsheet_id with the specified rows and header.

    :param sheets: Google Sheets service
    :param spreadsheet_id:
    :param header:
    :param rows:
    :return:
    """
    spreadsheet_data = sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    first_sheet_id = spreadsheet_data['sheets'][0]['properties']['sheetId']
    first_sheet_title = spreadsheet_data['sheets'][0]['properties']['title']
    clear_sheet_body = {
        'updateCells': {
            'range': {
                'sheetId': first_sheet_id
            },
            'fields': '*',
        }
    }
    set_sheet_properties_body = {
        'updateSheetProperties': {
            'properties': {
                'sheetId': first_sheet_id,
                'title': first_sheet_title,
                'index': 0,
                'gridProperties': {
                    'rowCount': len(rows) + 1,
                    'columnCount': 10,
                    'frozenRowCount': 1,
                    'hideGridlines': False,
                },
            },
            'fields': '*',
        }
    }
    cell_update_body = {
        'updateCells': {
            'range': {'sheetId': first_sheet_id,
                      'startRowIndex': 0, 'endRowIndex': len(rows) + 1,
                      'startColumnIndex': 0, 'endColumnIndex': 4},
            'fields': '*',
            'rows': [{'values': [
                {'userEnteredValue': {'stringValue': header_field}} for header_field in header]}] + [
                        {'values': [{'userEnteredValue': {'stringValue': row[field]}} for field in header]}
                        for row in rows]
        }
    }
    batch_update_body = {
        'requests': [clear_sheet_body, set_sheet_properties_body, cell_update_body]
    }
    sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_body).execute()