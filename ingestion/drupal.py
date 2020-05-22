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

    def _get_jsonapi_data(self, endpoint):
        """
        return the `data` attribute of all pages for `endpoint`
        """
        data = []
        next_uri = f'{self.base_url}jsonapi/{endpoint}'
        while next_uri:
            resp = self.session.get(next_uri)
            resp_json = resp.json()
            data.extend(resp_json['data'])
            next_uri = resp_json.get('links', {}).get('next', {}).get('href')
        return data

    def get_form_entry_meta(self, form_id):
        """
        return the `data` attribute of the `json.loads` output for the form from
        """

        return self._get_jsonapi_data(f'webform_submission/{form_id}')

    def get_form_entries_df(self, form_id, sids):
        """
        return the `data` attribute from each sid in `sids` for the form `form_id`.
        """

        rows = [
            self.session.get(
                f'{self.base_url}webform_rest/{form_id}/submission/{sid}').json()['data']
            for sid in sids
        ]
        return pd.DataFrame(rows)

    def get_taxonomy_terms(self, taxonomy_vocab):
        resp_json_data = self._get_jsonapi_data(f'taxonomy_term/{taxonomy_vocab}')

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
