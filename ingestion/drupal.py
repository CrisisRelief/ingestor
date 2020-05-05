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

    def get_form_entries_df(self, form_id):
        # TODO: test and implement get_form_entries_df

        # Step 1: Get a list of webforms from https://test.crisis.app/jsonapi/webform/webform

        # Step 2: Validate that form_id exists

        # Step 3: get all of the `attributes.drupal_internal__sid` and `attributes.changed` values for the form from https://test.crisis.app/jsonapi/webform_submission/{form_id}

        # Step 4 (optional): check if there are any forms with mod_time greater than the last time this importer ran

        # Step 5: Download the `data` from each sid using https://test.crisis.app/webform_rest/{webform_id}/submission/{sid}

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
