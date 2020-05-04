import os
from unittest.mock import MagicMock

import gspread


class ChDir(object):
    """
    Step into a directory temporarily.
    """
    def __init__(self, path):
        self.old_dir = os.getcwd()
        self.new_dir = path

    def __enter__(self):
        os.chdir(self.new_dir)

    def __exit__(self, *args):
        os.chdir(self.old_dir)


def mock_worksheet_helper():
    mock_worksheet = MagicMock()
    mock_worksheet.get_all_values.return_value = [
        ['crap', 'above', 'header'],
        ['actual', 'header', 'row'],
        ['1', '2', '3'],
        ['4', '5', '6'],
    ]
    mock_sheet = MagicMock(spec=gspread.Spreadsheet)
    mock_sheet.worksheet.return_value = mock_worksheet
    mock_creds = MagicMock()
    mock_creds.open_by_key.return_value = mock_sheet
    return mock_worksheet, mock_sheet, mock_creds
