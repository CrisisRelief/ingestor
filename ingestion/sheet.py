import gspread
import pandas as pd
from gspread.exceptions import WorksheetNotFound
from oauth2client.service_account import ServiceAccountCredentials

DEFAULT_SCOPE = ['https://spreadsheets.google.com/feeds', 'https://www.googleapis.com/auth/drive']


def authorize_creds(creds_file, scope=DEFAULT_SCOPE):
    creds = ServiceAccountCredentials.from_json_keyfile_name(creds_file, scope)
    return gspread.authorize(creds)


class Sheet:
    def __init__(self, gc, spreadsheet_key):
        self.sheet = gc.open_by_key(spreadsheet_key)

    def get_worksheet_df(self, name, header_row=0, constants=None):
        try:
            cells = self.sheet.worksheet(name).get_all_values()
        except WorksheetNotFound:
            sheet_names = [
                worksheet['properties']['title'] for worksheet in
                self.sheet.fetch_sheet_metadata()['sheets']
            ]
            raise UserWarning(f"worksheet {name} not found. Options: {sheet_names}")

        headers = cells[header_row]
        rows = cells[header_row + 1:]
        extra_cells = []
        if constants:
            headers += list(constants.keys())
            extra_cells = list(constants.values())
        rows = [
            row + extra_cells for row in rows if any(row)
        ]
        return pd.DataFrame(rows, columns=headers)
