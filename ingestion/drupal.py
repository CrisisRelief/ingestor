import requests
import pandas as pd


class Drupal:
    def __init__(self, base_url, username, password, login_form_id='user_login_form'):
        self.base_url = base_url
        self.login_form_id = login_form_id
        self.session = requests.Session()
        self.login(username, password)

    def login(self, username, password):
        resp = self.session.post(
            f'{self.base_url}user/login',
            data={
                'name': username,
                'pass': password,
                'form_id': self.login_form_id,
                'op': 'Log+in'
            }
        )
        if len(self.session.cookies) == 0:
            raise UserWarning(
                f"error logging in. response: {resp}\n{resp.headers}\n{resp.text}")

    def get_form_entries_df(self, form_name):
        # TODO: test and implement get_form_entries_df
        # The output needs to be a dataframe, e.g. :

        rows = [
            {
                "contact_email": "foo@bar.com",
                "contact_name": "test contact name",
                "contact_phone": "test phone",
                "link": "test link",
                "public_contact": "0",
                "resource_category": [
                    "187"
                ],
                "resource_description": "test description",
                "resource_title": "testing title"
            }
        ]
        return pd.DataFrame(rows)
