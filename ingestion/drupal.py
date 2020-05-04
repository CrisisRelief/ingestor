import pandas as pd
import requests


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
        for key, _ in self.session.cookies.items():
            if key.startswith('SSESS'):
                return
        raise UserWarning(
            f'error logging in. '
            f'\nresponse: {resp}\n{resp.headers}\n{resp.text}'
            f'\ncookies: {self.session.cookies.items()}'
        )

    def get_form_entry_meta(self, form_id):
        """
        return the `data` attribute of the `json.loads` output for the form from
        """

        resp = self.session.get(f'{self.base_url}jsonapi/webform_submission/{form_id}')
        return resp.json()["data"]

    def get_form_entries_df(self, form_id, sids):
        """
        return the `data` attribute from each sid in `sids` for the form `form_id`.
        """

        rows = []
        for sid in sids:
            resp = self.session.get(f'{self.base_url}webform_rest/{form_id}/submission/{sid}')
            form_data = resp.json()['data']

            rows.append({
                key: form_data[key]
                for key in [
                    "contact_email",
                    "contact_name",
                    "contact_phone",
                    "link",
                    "public_contact",
                    "resource_category",
                    "resource_description",
                    "resource_title",
                ]
            })
        return pd.DataFrame(rows)

    def get_taxonomy_terms(self, taxonomy_vocab):
        resp = self.session.get(f'{self.base_url}jsonapi/taxonomy_term/{taxonomy_vocab}')
        resp_json_data = resp.json()['data']

        uuid_tids = {
            term['id']: term['attributes']['drupal_internal__tid']
            for term in resp_json_data
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
            } for term in resp_json_data
        ]
