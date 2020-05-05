import requests
import pandas as pd
import json


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
        for key, value in self.session.cookies.items():
            if key.startswith('SSESS'):
                return
        raise UserWarning(
            f'error logging in. '
            f'\nresponse: {resp}\n{resp.headers}\n{resp.text}'
            f'\ncookies: {self.session.cookies.items()}'
        )

    def get_form_entry_meta(self, form_id):
        # TODO: test and implement this

        # Step 1 (optional): Get a list of webforms from f'{self.base_url}jsonapi/webform/webform'

        # Step 2 (optional): validate that form_id exists in the forms from the previous step

        # Step 3: return the `data` attribute of the `json.loads` output for the form from
        # f'{self.base_url}jsonapi/webform_submission/{form_id}'

        # e.g.:

        with open('tests/data/dummy-form-meta.json') as stream:
            return json.load(stream)['data']

    def get_form_entries_df(self, form_id, sids):
        # TODO: test and implement this

        # Step 1: Download the `data` from each sid using
        # f'{self.base_url}webform_rest/{webform_id}/submission/{sid}'

        # Step 2: The output needs to be a dataframe, e.g. :

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

    def get_taxonomy_terms(self, taxonomy_vocab):
        resp_json = self.session.get(
            f'{self.base_url}jsonapi/taxonomy_term/{taxonomy_vocab}'
        ).json()

        uuid_tids = {
            term['id']: term['attributes']['drupal_internal__tid']
            for term in resp_json['data']
        }

        return [
            {
                'id': term['attributes']['drupal_internal__tid'],
                'name': term['attributes']['name'],
                'parents': [
                    uuid_tids[parent['id']]
                    for parent in term['relationships']['parent']['data']
                    if parent['id'] in uuid_tids
                ]
            } for term in resp_json['data']
        ]
