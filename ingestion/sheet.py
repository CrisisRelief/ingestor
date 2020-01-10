import pandas as pd


class Sheet:
    def __init__(self, gc, spreadsheet_key):
        self.sheet = gc.open_by_key(spreadsheet_key)

    def get_worksheet_df(self, name, header_row=0):
        cells = self.sheet.worksheet(name).get_all_values()
        headers = cells[header_row]
        return pd.DataFrame(cells[header_row + 1:], columns=headers)
