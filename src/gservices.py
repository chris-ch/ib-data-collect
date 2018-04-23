import gspread
import httplib2

from apiclient import discovery
from oauth2client.service_account import ServiceAccountCredentials


_GOOGLE_DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'
_GOOGLE_DRIVE_FILE_SCOPE = 'https://www.googleapis.com/auth/drive.file'

flag_authorized = False


def authorize_gspread(google_creds):
    """
    Authorization is called only once and then re-used.

    :param google_creds:
    :return:
    """
    global flag_authorized
    svc_sheet = None
    if not flag_authorized:
        authorized_http, credentials = authorize_services(google_creds)
        svc_sheet = gspread.authorize(credentials)
        flag_authorized = True

    return svc_sheet


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


def authorize_services(credentials_file):
    """

    :param credentials_file:
    :return:
    """
    scopes = [_GOOGLE_DRIVE_SCOPE]
    credentials = ServiceAccountCredentials.from_json_keyfile_name(credentials_file, scopes=scopes)
    if not credentials or credentials.invalid:
        raise Exception('Invalid credentials')

    authorized_http = credentials.authorize(httplib2.Http())
    return authorized_http, credentials


def setup_services(credentials_file):
    """
    :param credentials_file: Google JSON Service Account credentials
    :return: tuple (Drive service, Sheets service)
    """
    authorized_http, credentials = authorize_services(credentials_file)
    svc_drive = discovery.build('drive', 'v3', http=authorized_http, cache_discovery=False)
    svc_sheets = discovery.build('sheets', 'v4', http=authorized_http, cache_discovery=False)
    return svc_drive, svc_sheets


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