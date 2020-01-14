import sys
import os

import pandas as pd
from oauth2client.service_account import ServiceAccountCredentials

try:
    # deal with https://github.com/pyenv/pyenv/issues/415
    PATH = sys.path
    if os.environ.get('PYENV_VIRTUAL_ENV'):
        sys.path.append(
            os.path.join(
                os.environ['PYENV_VIRTUAL_ENV'],
                f'lib/python{sys.version_info.major}.{sys.version_info.minor}/site-packages'))
    import gspread
    from gspread.exceptions import WorksheetNotFound
finally:
    sys.path = PATH

DEFAULT_SCOPE = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
    'https://www.googleapis.com/auth/drive.appdata',
    'https://www.googleapis.com/auth/drive.file',
    'https://www.googleapis.com/auth/drive.metadata',
    'https://www.googleapis.com/auth/drive.metadata.readonly',
    'https://www.googleapis.com/auth/drive.readonly'
]


def authorize_creds(creds_file, scope=DEFAULT_SCOPE):
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    return gspread.authorize(creds)


class Sheet:
    def __init__(self, gc, spreadsheet_key):
        self.spreadsheet_key = spreadsheet_key
        self.sheet = gc.open_by_key(spreadsheet_key)

    def get_worksheet_df(self, name, header_row=0, constants=None):
        try:
            cells = self.sheet.worksheet(name).get_all_values()
        except WorksheetNotFound:
            sheet_names = [
                worksheet['properties']['title']
                for worksheet in self.sheet.fetch_sheet_metadata()['sheets']
            ]
            raise UserWarning(f"worksheet {name} not found. Options: {sheet_names}")

        headers = cells[header_row]
        rows = cells[header_row + 1:]
        extra_cells = []
        if constants:
            headers += list(constants.keys())
            extra_cells = list(constants.values())
        rows = [row + extra_cells for row in rows if any(row)]
        return pd.DataFrame(rows, columns=headers)

    @property
    def modtime_str(self):
        url = f"https://www.googleapis.com/drive/v3/files/{self.spreadsheet_key}"
        params = {'fields': 'modifiedTime'}
        file_ = self.sheet.client.request('get', url, params=params).json()
        return file_['modifiedTime']
