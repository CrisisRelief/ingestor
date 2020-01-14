import sys
from unittest.mock import MagicMock
import gspread

import pandas as pd

from . import REPO_ROOT

try:
    PATH = sys.path
    sys.path.append(REPO_ROOT)

    from ingestion.sheet import Sheet
finally:
    sys.path = PATH


class TestSheets(object):
    def setup_method(self):
        self.mock_worksheet = MagicMock()
        self.mock_worksheet.get_all_values.return_value = [
            ['crap', 'above', 'header'],
            ['actual', 'header', 'row'],
            ['1', '2', '3'],
            ['4', '5', '6'],
        ]
        self.mock_sheet = MagicMock(spec=gspread.Spreadsheet)
        self.mock_sheet.worksheet.return_value = self.mock_worksheet
        self.mock_creds = MagicMock()
        self.mock_creds.open_by_key.return_value = self.mock_sheet

    def test_get_worksheet_df_skips_preheader(self):
        expected = pd.DataFrame({'actual': ['1', '4'], 'header': ['2', '5'], 'row': ['3', '6']})
        sheet = Sheet(self.mock_creds, 'foo')

        result = sheet.get_worksheet_df('bar', 1)

        assert expected.equals(result)

    def test_get_worksheet_df_adds_constants(self):
        expected = pd.DataFrame({
            'actual': ['1', '4'],
            'header': ['2', '5'],
            'row': ['3', '6'],
            'extra': ['const', 'const']
        })
        sheet = Sheet(self.mock_creds, 'foo')

        result = sheet.get_worksheet_df('bar', 1, {'extra': 'const'})

        assert expected.equals(result)

    def test_get_worksheet_df_ignores_blanks(self):
        mock_worksheet = MagicMock()
        mock_worksheet.get_all_values.return_value = [
            ['crap', 'above', 'header'],
            ['actual', 'header', 'row'],
            ['1', '2', '3'],
            ['4', '5', '6'],
            ['', '', ''],
        ]
        mock_sheet = MagicMock(spec=gspread.Spreadsheet)
        mock_sheet.worksheet.return_value = mock_worksheet
        mock_creds = MagicMock()
        mock_creds.open_by_key.return_value = mock_sheet
        expected = pd.DataFrame({'actual': ['1', '4'], 'header': ['2', '5'], 'row': ['3', '6']})

        sheet = Sheet(mock_creds, 'foo')
        result = sheet.get_worksheet_df('bar', 1)

        assert expected.equals(result)
