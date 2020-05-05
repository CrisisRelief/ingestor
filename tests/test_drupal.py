import sys
import responses
from os.path import join as path_join
import json
import pandas as pd

import pytest

from . import REPO_ROOT, TEST_DATA

try:
    PATH = sys.path
    sys.path.append(REPO_ROOT)

    from ingestion.drupal import Drupal
finally:
    sys.path = PATH


class TestDrupal:
    def setup_method(self):
        self.cookie_headers = {
            'Set-Cookie': (
                'SSESSaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
                '=bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb-ccccccc;'
                ' expires=Thu, 28-May-2020 11:22:12 GMT; Max-Age=2000000; path=/;'
                ' domain=.test.com; secure; HttpOnly')
        }

    @responses.activate
    def test_init_login_asserts_cookies(self):
        # Given
        responses.add(responses.POST, 'http://test.com/user/login')

        # Then
        with pytest.raises(UserWarning):
            Drupal(
                base_url='http://test.com/',
                username='username',
                password='password'
            )

    @responses.activate
    def test_init_login_succeeds(self):
        # Given
        responses.add(
            'POST',
            'http://test.com/user/login',
            status=200,
            headers=self.cookie_headers
        )

        # When
        drupal = Drupal(
            base_url='http://test.com/',
            username='username',
            password='password'
        )

        # Then
        assert drupal.session.cookies

    @responses.activate
    def test_get_form_entry_meta_format(self):
        # Given
        responses.add(
            'POST',
            'http://test.com/user/login',
            status=200,
            headers=self.cookie_headers
        )
        dummy_meta_path = path_join(TEST_DATA, 'dummy-form-meta.json')
        with open(dummy_meta_path) as stream:
            responses.add(
                'POST',
                'http://test.com/jsonapi/webform_submission/resource_intake',
                status=200,
                json=json.load(stream)
            )
        drupal = Drupal(
            base_url='http://test.com/',
            username='username',
            password='password'
        )

        # When
        meta = drupal.get_form_entry_meta('resource_intake')

        # Then
        assert meta
        assert meta[0]['attributes']['drupal_internal__sid']
        assert meta[0]['attributes']['changed']

    @responses.activate
    def test_get_form_entries_df_format(self):
        # Given
        responses.add(
            'POST',
            'http://test.com/user/login',
            status=200,
            headers=self.cookie_headers
        )
        dummy_data_path = path_join(TEST_DATA, 'dummy-form-data-187.json')
        with open(dummy_data_path) as stream:
            responses.add(
                'POST',
                'http://test.com/webform_rest/resource_intake/submission/187',
                status=200,
                json=json.load(stream)
            )
        drupal = Drupal(
            base_url='http://test.com/',
            username='username',
            password='password'
        )

        # When
        entries = drupal.get_form_entries_df('resource_intake', [187])

        # Then
        assert not entries.empty
        assert isinstance(entries, pd.DataFrame)
