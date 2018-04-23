import logging
import gspread
import httplib2

from apiclient import discovery
from gspread.utils import rowcol_to_a1
from oauth2client.service_account import ServiceAccountCredentials

_GOOGLE_DRIVE_SCOPE = 'https://www.googleapis.com/auth/drive'
_GOOGLE_DRIVE_FILE_SCOPE = 'https://www.googleapis.com/auth/drive.file'

svc_sheet = None


def authorize_gspread(google_creds):
    """
    Authorization is called only once and then re-used.

    :param google_creds:
    :return:
    """
    global svc_sheet
    if not svc_sheet:
        authorized_http, credentials = authorize_services(google_creds)
        svc_sheet = gspread.authorize(credentials)

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


def update_sheet(srv_sheets, spreadsheet_id, header, rows):
    """
    Updates the first available sheet from spreadsheet_id with the specified rows and header.

    :param srv_sheets: Google Sheets service
    :param spreadsheet_id:
    :param header:
    :param rows:
    :return:
    """
    spreadsheet_data = srv_sheets.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
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
    srv_sheets.spreadsheets().batchUpdate(spreadsheetId=spreadsheet_id, body=batch_update_body).execute()


def get_spreadsheet(srv_sheet, spreadsheet_id):
    return srv_sheet.open_by_key(spreadsheet_id)


def walk_through_range(header, rows):
    for row in rows:
        for field in header:
            yield row[field]


def import_dictrows(spreadsheet, worksheet_name, rows):
    if len(rows) == 0:
        logging.warning("no row to be added for worksheet %s", worksheet_name)
        return

    count_rows = len(rows) + 1
    header = rows[0].keys()
    count_cols = len(header)
    worksheet = spreadsheet.add_worksheet(title=worksheet_name, rows=count_rows, cols=count_cols)
    header_range = worksheet.range('%s:%s' % (rowcol_to_a1(1, 1), rowcol_to_a1(1, count_cols)))
    for field, cell in zip(header, header_range):
        cell.value = field

    worksheet.update_cells(header_range)

    data_range = worksheet.range('%s:%s' % (rowcol_to_a1(2, 1), rowcol_to_a1(count_rows, count_cols)))
    for cell, value in zip(data_range, walk_through_range(header, rows)):
        cell.value = value

    worksheet.update_cells(data_range)
    return worksheet


def worksheets_by_title(spreadsheet):
    worksheets = spreadsheet.worksheets()
    return {worksheet.title: worksheet for worksheet in worksheets}


def update_spreadsheet(svc_sheet, spreadsheet_id, worksheet_name, rows):
    spreadsheet = get_spreadsheet(svc_sheet, spreadsheet_id)
    ws_by_title = worksheets_by_title(spreadsheet)
    matching_worksheets = [ws for ws in spreadsheet.worksheets() if ws.title == worksheet_name]
    if len(matching_worksheets) > 0:
        old_worksheet = matching_worksheets[0]
        rename_title = 'old_' + worksheet_name
        if rename_title in ws_by_title:
            spreadsheet.del_worksheet(ws_by_title[rename_title])

        old_worksheet.update_title(rename_title)
        new_worksheet = import_dictrows(spreadsheet, worksheet_name, rows)
        spreadsheet.del_worksheet(old_worksheet)

    else:
        new_worksheet = import_dictrows(spreadsheet, worksheet_name, rows)

    return new_worksheet


def clean_spreadsheet(svc_sheet, spreadsheet_id, worksheet_names):
    spreadsheet = get_spreadsheet(svc_sheet, spreadsheet_id)
    for worksheet in spreadsheet.worksheets():
        if len(spreadsheet.worksheets()) <= 1:
            break

        if worksheet.title not in worksheet_names:
            logging.info('removing worksheet "%s"', worksheet.title)
            spreadsheet.del_worksheet(worksheet)


"""
{
  "requests": [
    {
      "updateDimensionProperties": 
    },
    """


def resize_column(worksheet, column_index, column_width):
    """

    :param worksheet:
    :param column_index: counting from 1
    :param column_width:
    :return:
    """
    parent = worksheet.spreadsheet
    body = {
        'requests': [{
            'updateDimensionProperties': {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "COLUMNS",
                    "startIndex": column_index - 1,
                    "endIndex": column_index
                },
                "properties": {
                    "pixelSize": column_width
                },
                "fields": "pixelSize"
            }
        }]
    }

    return parent.batch_update(body)


def auto_resize_columns(worksheet, column_index_start, column_index_end):
    """

    :param worksheet:
    :param column_index_start: included, counting from 1
    :param column_index_end: excluded, counting from 1
    :return:
    """
    parent = worksheet.spreadsheet
    body = {
        'requests': [{
          "autoResizeDimensions": {
            "dimensions": {
              "sheetId": worksheet.id,
              "dimension": "COLUMNS",
              "startIndex": column_index_start - 1,
              "endIndex": column_index_end - 1
            }
          }
        }]
    }
    return parent.batch_update(body)


def auto_resize_column(worksheet, column_index):
    """

    :param worksheet:
    :param column_index:
    :return:
    """
    return  auto_resize_columns(worksheet, column_index, column_index + 1)
